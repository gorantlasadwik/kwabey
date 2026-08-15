"""
Kwabey Recovery Scraper - Web Application & Dashboard with Keep-Alive Service
Auto-starts the full 6→9 series scan on boot and stores results in Supabase.
"""

import json
import logging
import os
import queue
import threading
import time
from datetime import datetime
from typing import Optional

from flask import Flask, Response, jsonify, render_template, request, send_file
import requests

import config
import scraper
import supabase_client

app = Flask(__name__)

# ==========================================
# APPLICATION STATE
# ==========================================

class ScraperState:
    def __init__(self):
        self.is_running = False
        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()

        # Metrics
        self.total_processed = 0
        self.registered_count = 0
        self.unregistered_count = 0
        self.unknown_count = 0
        self.error_count = 0
        self.last_phone = ""
        self.start_time: Optional[float] = None

        # Log queue for SSE streaming
        self.log_subscribers = []
        self.recent_logs = []
        self.max_recent_logs = 150

        # Keep-Alive Anti Spin-down
        self.render_url = os.environ.get("RENDER_EXTERNAL_URL", "")
        self.keepalive_interval = int(os.environ.get("AUTO_PING_INTERVAL_MINUTES", "10")) * 60
        self.keepalive_enabled = True
        self.last_ping_time = ""
        self.last_ping_status = "Not started"
        self.ping_count = 0


state = ScraperState()


def broadcast_log(level: str, message: str, phone: str = "", status: str = ""):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = {
        "timestamp": timestamp,
        "level": level,
        "message": message,
        "phone": phone,
        "status": status,
    }
    with state.lock:
        state.recent_logs.append(entry)
        if len(state.recent_logs) > state.max_recent_logs:
            state.recent_logs.pop(0)

        dead_subscribers = []
        for q in state.log_subscribers:
            try:
                q.put_nowait(entry)
            except Exception:
                dead_subscribers.append(q)
        for q in dead_subscribers:
            state.log_subscribers.remove(q)


# ==========================================
# BACKGROUND SCAN WORKER
# ==========================================

def scan_worker_task(job_config: dict):
    state.is_running = True
    state.stop_event.clear()
    state.start_time = time.time()

    mode = job_config.get("mode", "range")
    delay = float(job_config.get("delay", config.REQUEST_DELAY))
    base_url = job_config.get("url", config.DEFAULT_BASE_URL)
    resume = job_config.get("resume", True)

    start_param = job_config.get("start", config.SCAN_START)
    end_param = job_config.get("end", config.SCAN_END)
    phone_param = job_config.get("phone")
    numbers_list = job_config.get("numbers", [])

    scraper.init_csv_files()

    checkpoint = scraper.load_checkpoint() if (resume and mode != "single") else None
    if checkpoint:
        broadcast_log("INFO", f"Resuming from checkpoint: {checkpoint}")
    else:
        broadcast_log("INFO", f"Starting fresh from {start_param}")

    # Build generator
    if mode == "single" and phone_param:
        gen = scraper.generate_input(phone=str(phone_param).strip())
    elif mode == "list" and numbers_list:
        skipping = bool(checkpoint) if (resume and checkpoint in numbers_list) else False
        def list_gen():
            nonlocal skipping
            for p in numbers_list:
                p = str(p).strip()
                if not p:
                    continue
                if skipping:
                    if p == checkpoint:
                        skipping = False
                    continue
                yield p
        gen = list_gen()
    elif mode == "range" and start_param is not None and end_param is not None:
        gen = scraper.generate_input(
            start=int(start_param),
            end=int(end_param),
            checkpoint=checkpoint if resume else None,
        )
    else:
        broadcast_log("ERROR", "Invalid job parameters.")
        state.is_running = False
        return

    session = requests.Session()
    session.headers.update({
        "User-Agent": config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    broadcast_log("INFO", f"Scan started — Mode: {mode.upper()} | Range: {start_param} → {end_param}")

    try:
        for phone in gen:
            if state.stop_event.is_set():
                broadcast_log("WARNING", f"Scan stopped at {phone}. Checkpoint saved.", phone=phone)
                break

            timestamp = datetime.now().isoformat()
            url = scraper.build_url(phone, base_url)
            http_status, html_content, error = scraper.fetch_html(session, url)

            with state.lock:
                state.total_processed += 1
                state.last_phone = phone

            if error is not None:
                with state.lock:
                    state.error_count += 1
                broadcast_log("ERROR", f"HTTP Error: {error}", phone=phone, status="ERROR")
            else:
                status, snippet = scraper.parse_response(html_content)

                if status == "REGISTERED":
                    with state.lock:
                        state.registered_count += 1
                    broadcast_log("SUCCESS", f"REGISTERED (HTTP {http_status})", phone=phone, status="REGISTERED")
                    # Save only registered numbers to Supabase
                    supabase_client.save_registered_number(phone, http_status, timestamp)
                elif status == "UNREGISTERED":
                    with state.lock:
                        state.unregistered_count += 1
                    broadcast_log("UNREGISTERED", "Unregistered", phone=phone, status="UNREGISTERED")
                else:
                    with state.lock:
                        state.unknown_count += 1
                    broadcast_log("WARNING", f"UNKNOWN ({snippet[:35]}...)", phone=phone, status="UNKNOWN")

            # Always save checkpoint so we can resume after any restart
            scraper.save_checkpoint(phone)

            if delay > 0:
                sleep_chunks = int(delay / 0.05)
                for _ in range(max(1, sleep_chunks)):
                    if state.stop_event.is_set():
                        break
                    time.sleep(0.05)

    except Exception as e:
        broadcast_log("ERROR", f"Worker fatal exception: {str(e)}")
    finally:
        session.close()
        state.is_running = False
        broadcast_log("INFO", "Scan job finished.")


# ==========================================
# AUTO-START ON RENDER BOOT
# ==========================================

def auto_start_scan():
    """
    Automatically starts the full 6→9 series scan on app boot.
    Resumes from the Supabase cloud checkpoint if available.
    """
    time.sleep(3)  # brief delay so Flask is fully up
    broadcast_log("INFO", "Auto-start: launching full 6→9 series scan...")
    job = {
        "mode": "range",
        "start": config.SCAN_START,
        "end": config.SCAN_END,
        "delay": config.REQUEST_DELAY,
        "url": config.DEFAULT_BASE_URL,
        "resume": True,
    }
    with state.lock:
        state.total_processed = 0
        state.registered_count = 0
        state.unregistered_count = 0
        state.unknown_count = 0
        state.error_count = 0

    state.worker_thread = threading.Thread(target=scan_worker_task, args=(job,), daemon=True)
    state.worker_thread.start()


# Launch auto-start thread on boot
_auto_start_thread = threading.Thread(target=auto_start_scan, daemon=True)
_auto_start_thread.start()


# ==========================================
# KEEP-ALIVE BACKGROUND SERVICE
# ==========================================

def keepalive_worker():
    """Periodically pings itself to prevent Render free-tier spin-down."""
    time.sleep(5)
    while True:
        try:
            if state.keepalive_enabled:
                target_url = state.render_url.strip() if state.render_url else "http://127.0.0.1:5000/ping"
                if not target_url.endswith("/ping"):
                    target_url = f"{target_url.rstrip('/')}/ping"

                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                resp = requests.get(target_url, timeout=10)

                with state.lock:
                    state.last_ping_time = ts
                    state.last_ping_status = f"HTTP {resp.status_code} OK"
                    state.ping_count += 1

                broadcast_log("KEEPALIVE", f"Keep-alive ping → {target_url} [{resp.status_code}]")
        except Exception as e:
            with state.lock:
                state.last_ping_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                state.last_ping_status = f"Failed: {str(e)[:40]}"
            broadcast_log("KEEPALIVE_WARN", f"Keep-alive error: {str(e)}")

        time.sleep(max(60, state.keepalive_interval))


keepalive_thread = threading.Thread(target=keepalive_worker, daemon=True)
keepalive_thread.start()


# ==========================================
# HTTP ROUTES & API
# ==========================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/ping")
def ping():
    """Health check and keep-alive endpoint."""
    return jsonify({
        "status": "alive",
        "service": "Kwabey Recovery Scraper",
        "timestamp": datetime.now().isoformat(),
        "is_scanning": state.is_running,
    })


@app.route("/api/status")
def get_status():
    speed = 0.0
    elapsed = 0.0
    if state.start_time and state.is_running:
        elapsed = time.time() - state.start_time
        if elapsed > 0 and state.total_processed > 0:
            speed = round(state.total_processed / elapsed, 2)

    with state.lock:
        return jsonify({
            "is_running": state.is_running,
            "total_processed": state.total_processed,
            "registered_count": state.registered_count,
            "unregistered_count": state.unregistered_count,
            "unknown_count": state.unknown_count,
            "error_count": state.error_count,
            "last_phone": state.last_phone,
            "speed": speed,
            "keepalive": {
                "enabled": state.keepalive_enabled,
                "url": state.render_url,
                "last_time": state.last_ping_time,
                "last_status": state.last_ping_status,
                "ping_count": state.ping_count,
                "interval_min": round(state.keepalive_interval / 60, 1),
            },
        })


@app.route("/api/scan/start", methods=["POST"])
def start_scan():
    """Manual scan trigger (overrides auto-scan or starts a custom range)."""
    if state.is_running:
        return jsonify({"error": "A scan is already running."}), 400

    data = request.get_json() or {}

    if not state.render_url:
        host = request.host_url
        if "localhost" not in host and "127.0.0.1" not in host:
            state.render_url = host

    if not data.get("resume", True) or data.get("mode") == "single":
        with state.lock:
            state.total_processed = 0
            state.registered_count = 0
            state.unregistered_count = 0
            state.unknown_count = 0
            state.error_count = 0

    state.worker_thread = threading.Thread(target=scan_worker_task, args=(data,), daemon=True)
    state.worker_thread.start()
    return jsonify({"status": "started", "job": data})


@app.route("/api/scan/stop", methods=["POST"])
def stop_scan():
    if not state.is_running:
        return jsonify({"status": "not_running", "message": "No scan is currently running."})
    state.stop_event.set()
    broadcast_log("WARNING", "Stop signal received. Gracefully halting scan...")
    return jsonify({"status": "stopping", "message": "Stop signal sent to worker."})


@app.route("/api/registered")
def get_registered_numbers():
    """
    Returns registered phone numbers fetched directly from Supabase.
    Falls back to local CSV if Supabase is not configured.
    """
    try:
        client = supabase_client._get_client()
        if client:
            result = (
                client.table("registered_numbers")
                .select("phone_number, http_status, discovered_at")
                .order("discovered_at", desc=True)
                .execute()
            )
            records = [
                {
                    "phone_number": row["phone_number"],
                    "status": "REGISTERED",
                    "http_status": row.get("http_status", 200),
                    "timestamp": row.get("discovered_at", ""),
                }
                for row in (result.data or [])
            ]
            return jsonify({"count": len(records), "data": records, "source": "supabase"})
    except Exception as e:
        logging.getLogger("kwabey_scraper").error(f"/api/registered Supabase error: {e}")

    # Fallback: empty (no local CSV on Render)
    return jsonify({"count": 0, "data": [], "source": "fallback"})


@app.route("/api/logs/stream")
def stream_logs():
    def event_stream():
        client_queue = queue.Queue(maxsize=100)
        with state.lock:
            state.log_subscribers.append(client_queue)
            for old_log in state.recent_logs[-30:]:
                yield f"data: {json.dumps(old_log)}\n\n"
        try:
            while True:
                try:
                    log_item = client_queue.get(timeout=20.0)
                    yield f"data: {json.dumps(log_item)}\n\n"
                except queue.Empty:
                    yield ": ping\n\n"
        finally:
            with state.lock:
                if client_queue in state.log_subscribers:
                    state.log_subscribers.remove(client_queue)

    return Response(event_stream(), mimetype="text/event-stream")


@app.route("/api/keepalive/config", methods=["POST"])
def configure_keepalive():
    data = request.get_json() or {}
    if "url" in data:
        state.render_url = str(data["url"]).strip()
    if "enabled" in data:
        state.keepalive_enabled = bool(data["enabled"])
    if "interval_minutes" in data:
        try:
            state.keepalive_interval = int(float(data["interval_minutes"]) * 60)
        except ValueError:
            pass
    return jsonify({
        "status": "updated",
        "url": state.render_url,
        "enabled": state.keepalive_enabled,
        "interval_minutes": state.keepalive_interval / 60,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

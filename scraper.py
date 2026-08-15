"""
Kwabey Registration Data Recovery Scraper
Retrieves HTML responses from Kwabey's endpoint, classifies registration status,
and stores audit & recovery records.
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import Generator, Optional, Tuple

import requests
from bs4 import BeautifulSoup

import config
import supabase_client

# Configure error logger — writes to both local file AND stdout (Render log viewer)
logger = logging.getLogger("kwabey_scraper")
logger.setLevel(logging.INFO)

# File handler (local backup)
file_handler = logging.FileHandler(config.ERROR_LOG_FILE, mode="a", encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# Stdout handler so Render captures warnings/errors
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.WARNING)  # only WARN+ to avoid duplicate INFO spam
stdout_handler.setFormatter(logging.Formatter("%(asctime)s [SCRAPER] %(levelname)s - %(message)s"))
logger.addHandler(stdout_handler)


def build_url(phone_number: str, base_url: str = config.DEFAULT_BASE_URL) -> str:
    """
    Constructs the request URL for the target endpoint.
    """
    prepared_request = requests.models.PreparedRequest()
    prepared_request.prepare_url(base_url, {"phone_number": str(phone_number).strip()})
    return prepared_request.url


def fetch_html(
    session: requests.Session,
    url: str,
    timeout: int = config.TIMEOUT
) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Executes an HTTP GET request using the provided session.
    Returns (status_code, response_text, error_message).
    """
    try:
        response = session.get(url, timeout=timeout)
        return response.status_code, response.text, None
    except requests.Timeout:
        err_msg = "Request timed out"
        logger.warning(f"Timeout connecting to {url}")
        return None, None, err_msg
    except requests.ConnectionError as e:
        err_msg = f"Connection error: {e}"
        logger.error(f"Connection failure for {url}: {e}")
        return None, None, err_msg
    except requests.RequestException as e:
        err_msg = f"HTTP request exception: {e}"
        logger.error(f"Request exception for {url}: {e}")
        return None, None, err_msg


def parse_response(html_content: str) -> Tuple[str, str]:
    """
    Parses HTML content using BeautifulSoup and classifies registration status.
    Returns (status, extracted_text_snippet).
    """
    if not html_content or not html_content.strip():
        return "UNKNOWN", "Empty response"

    try:
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(" ", strip=True)
    except Exception as e:
        logger.error(f"HTML parsing failure: {e}")
        text = html_content

    if config.REGISTERED_PATTERN.lower() in text.lower():
        return "REGISTERED", text
    elif config.UNREGISTERED_PATTERN.lower() in text.lower():
        return "UNREGISTERED", text
    else:
        return "UNKNOWN", text


def extract_phone(text: str, fallback_phone: str = "") -> str:
    """
    Extracts phone number from response text or falls back to provided phone.
    """
    match = re.search(r"\b\d{10}\b", text)
    if match:
        return match.group(0)
    return fallback_phone


def load_checkpoint(checkpoint_file: str = config.CHECKPOINT_FILE) -> Optional[str]:
    """
    Loads last processed phone number.
    Priority: 1) Supabase cloud checkpoint  2) local JSON file (offline fallback).
    """
    # --- 1. Try Supabase cloud checkpoint first ---
    cloud_cp = supabase_client.load_cloud_checkpoint()
    if cloud_cp:
        logger.info(f"Resuming from Supabase cloud checkpoint: {cloud_cp}")
        return cloud_cp

    # --- 2. Fall back to local file (useful for local dev without Supabase) ---
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                cp = data.get("last_processed")
                if cp:
                    logger.info(f"Resuming from local checkpoint file: {cp}")
                    return cp
        except Exception as e:
            logger.error(f"Failed to read checkpoint file: {e}")
    return None


def save_checkpoint(phone_number: str, checkpoint_file: str = config.CHECKPOINT_FILE) -> None:
    """
    Saves current phone number position to both Supabase (cloud) and local file.
    Dual-write ensures recovery even if one storage layer is unavailable.
    """
    # --- 1. Save to Supabase (primary, survives Render restarts) ---
    supabase_client.save_cloud_checkpoint(str(phone_number))

    # --- 2. Also write local file as a fast offline backup ---
    try:
        temp_file = f"{checkpoint_file}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump({"last_processed": str(phone_number), "updated_at": datetime.now().isoformat()}, f, indent=2)
        os.replace(temp_file, checkpoint_file)
    except Exception as e:
        logger.error(f"Failed to write local checkpoint for {phone_number}: {e}")


def init_csv_files(registered_file: str = config.REGISTERED_FILE):
    """
    Initialises the local registered-numbers CSV backup if it does not exist.
    (Supabase is the primary store; this file is a local safety net.)
    """
    if not os.path.exists(registered_file):
        with open(registered_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["phone_number", "status", "http_status", "timestamp"])


def save_result(
    record: dict,
    registered_file: str = config.REGISTERED_FILE
) -> None:
    """
    Appends REGISTERED numbers only to the local backup CSV.
    Unregistered / unknown / error records are NOT written anywhere —
    Supabase is the authoritative store for registered numbers,
    and the checkpoint tracks where we are.
    """
    if record.get("status") != "REGISTERED":
        return

    row = [
        record.get("phone_number", ""),
        record.get("status", "UNKNOWN"),
        record.get("http_status", ""),
        record.get("timestamp", datetime.now().isoformat())
    ]
    try:
        with open(registered_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(row)
    except Exception as e:
        logger.error(f"Failed to write local CSV backup for {record.get('phone_number')}: {e}")


# ===========================================================================
# ACTIVE INDIAN TELECOM OPERATOR SERIES (DoT / TRAI Allocated MSC Blocks)
# Covers all 22 telecom circles (Jio, Airtel, Vodafone-Idea, BSNL, MTNL)
# Format: (start_int, end_int_exclusive)
# ===========================================================================

ACTIVE_SERIES_BLOCKS = [
    # ── 6-Series (Jio, Airtel, Vi new allocations) ──
    (6000_000000, 6004_000000),   # 6000, 6001, 6002, 6003
    (6200_000000, 6210_000000),   # 6200 - 6209
    (6230_000000, 6240_000000),   # 6230 - 6239
    (6260_000000, 6270_000000),   # 6260 - 6269
    (6280_000000, 6300_000000),   # 6280 - 6299
    (6300_000000, 6310_000000),   # 6300 - 6309
    (6350_000000, 6400_000000),   # 6350 - 6399

    # ── 7-Series (Jio, Airtel, Vi, BSNL) ──
    (7000_000000, 7100_000000),   # 7000 - 7099
    (7200_000000, 7300_000000),   # 7200 - 7299
    (7300_000000, 7400_000000),   # 7300 - 7399
    (7400_000000, 7500_000000),   # 7400 - 7499
    (7500_000000, 7600_000000),   # 7500 - 7599
    (7600_000000, 7700_000000),   # 7600 - 7699
    (7700_000000, 7800_000000),   # 7700 - 7799
    (7800_000000, 7900_000000),   # 7800 - 7899
    (7900_000000, 8000_000000),   # 7900 - 7999

    # ── 8-Series (All Operators) ──
    (8000_000000, 8100_000000),   # 8000 - 8099
    (8100_000000, 8200_000000),   # 8100 - 8199
    (8200_000000, 8300_000000),   # 8200 - 8299
    (8300_000000, 8400_000000),   # 8300 - 8399
    (8400_000000, 8500_000000),   # 8400 - 8499
    (8500_000000, 8600_000000),   # 8500 - 8599
    (8600_000000, 8700_000000),   # 8600 - 8699
    (8700_000000, 8800_000000),   # 8700 - 8799
    (8800_000000, 8900_000000),   # 8800 - 8899
    (8900_000000, 9000_000000),   # 8900 - 8999

    # ── 9-Series (Airtel, Vi, BSNL, Jio - Prime Shopper Base) ──
    (9000_000000, 9100_000000),   # 9000 - 9099
    (9100_000000, 9200_000000),   # 9100 - 9199
    (9200_000000, 9300_000000),   # 9200 - 9299
    (9300_000000, 9400_000000),   # 9300 - 9399
    (9400_000000, 9500_000000),   # 9400 - 9499 (BSNL/MTNL)
    (9500_000000, 9600_000000),   # 9500 - 9599
    (9600_000000, 9700_000000),   # 9600 - 9699
    (9700_000000, 9800_000000),   # 9700 - 9799 (Airtel/Vi)
    (9800_000000, 9900_000000),   # 9800 - 9899 (Highest density e-commerce)
    (9900_000000, 10000_000000),  # 9900 - 9999
]


def generate_active_series(checkpoint: Optional[str] = None) -> Generator[str, None, None]:
    """
    Generates 10-digit mobile numbers exclusively across all active Indian telecom series.
    Seamlessly resumes from the saved checkpoint without scanning unallocated number blocks.
    """
    cp_num = None
    if checkpoint:
        try:
            cp_num = int(str(checkpoint).strip())
        except (ValueError, TypeError):
            cp_num = None

    for block_start, block_end in ACTIVE_SERIES_BLOCKS:
        # Case 1: Entire block is already completed before the checkpoint
        if cp_num is not None and cp_num >= block_end - 1:
            continue

        # Case 2: Checkpoint falls inside this block -> resume from next number
        if cp_num is not None and block_start <= cp_num < block_end:
            start_from = cp_num + 1
        else:
            start_from = block_start

        for num in range(start_from, block_end):
            yield str(num)


def generate_input(
    phone: Optional[str] = None,
    start: Optional[int] = None,
    end: Optional[int] = None,
    input_file: Optional[str] = None,
    checkpoint: Optional[str] = None,
    active_series_only: bool = False,
) -> Generator[str, None, None]:
    """
    Generates phone numbers from single phone, active series, range, or file.
    """
    if phone:
        yield str(phone).strip()
        return

    if active_series_only:
        yield from generate_active_series(checkpoint)
        return

    skipping = bool(checkpoint)

    if input_file and os.path.exists(input_file):
        with open(input_file, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip() and not l.strip().startswith("#")]
        
        if skipping and checkpoint not in lines:
            skipping = False

        for p in lines:
            if skipping:
                if p == checkpoint:
                    skipping = False
                continue
            yield p

    elif start is not None and end is not None:
        if skipping:
            try:
                cp_num = int(checkpoint)
                if cp_num < start or cp_num >= end:
                    skipping = False
            except ValueError:
                skipping = False

        for number in range(start, end):
            p = str(number)
            if skipping:
                if p == checkpoint:
                    skipping = False
                continue
            yield p


def run_scraper(
    phone: Optional[str] = None,
    start: Optional[int] = None,
    end: Optional[int] = None,
    input_file: Optional[str] = None,
    base_url: str = config.DEFAULT_BASE_URL,
    resume: bool = True,
    delay: float = config.REQUEST_DELAY
):
    """
    Main driver loop for the scraping recovery process.
    """
    init_csv_files()

    # Single number runs should not resume from an old checkpoint
    checkpoint = load_checkpoint() if (resume and not phone) else None
    if checkpoint:
        print(f"[*] Resuming from checkpoint: {checkpoint}")

    numbers_gen = generate_input(phone, start, end, input_file, checkpoint)

    session = requests.Session()
    session.headers.update({
        "User-Agent": config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    print("=" * 60)
    print("Kwabey Registration Data Recovery Scraper")
    print(f"Target Base URL: {base_url}")
    print(f"Started at     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    total_processed = 0
    registered_count = 0
    unregistered_count = 0
    unknown_count = 0
    error_count = 0

    try:
        for p in numbers_gen:
            total_processed += 1
            timestamp = datetime.now().isoformat()
            url = build_url(p, base_url)

            http_status, html_content, error = fetch_html(session, url)

            if error is not None:
                error_count += 1
                print(f"[!] {p:<12} -> HTTP ERROR: {error}")
                record = {
                    "phone_number": p,
                    "status": "ERROR",
                    "http_status": http_status or "ERR",
                    "timestamp": timestamp
                }
            else:
                status, snippet = parse_response(html_content)
                record = {
                    "phone_number": p,
                    "status": status,
                    "http_status": http_status,
                    "timestamp": timestamp
                }

                if status == "REGISTERED":
                    registered_count += 1
                    print(f"[+] {p:<12} -> REGISTERED (Status {http_status})")
                    # Save to Supabase immediately so no registered number is lost
                    supabase_client.save_registered_number(p, http_status, timestamp)
                elif status == "UNREGISTERED":
                    unregistered_count += 1
                    print(f"[-] {p:<12} -> UNREGISTERED")
                else:
                    unknown_count += 1
                    print(f"[?] {p:<12} -> UNKNOWN ({snippet[:40]}...)")

            save_result(record)
            save_checkpoint(p)

            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user. Progress saved to checkpoint.")
    finally:
        session.close()

    print("\n" + "=" * 60)
    print("SCRAPE RECOVERY SUMMARY")
    print("=" * 60)
    print(f"Total Processed  : {total_processed}")
    print(f"Registered       : {registered_count}")
    print(f"Unregistered     : {unregistered_count}")
    print(f"Unknown Responses: {unknown_count}")
    print(f"Errors Logged    : {error_count}")
    print(f"Registered Export: {config.REGISTERED_FILE}")
    print(f"Error Log        : {config.ERROR_LOG_FILE}")
    print(f"Checkpoint File  : {config.CHECKPOINT_FILE}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Kwabey Registration Data Recovery Scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--phone", "-p", type=str, help="Single phone number to check")
    parser.add_argument("--start", type=int, help="Starting phone number (integer)")
    parser.add_argument("--end", type=int, help="Ending phone number (integer, exclusive)")
    parser.add_argument("--file", type=str, help="Input text file containing phone numbers (one per line)")
    parser.add_argument("--url", type=str, default=config.DEFAULT_BASE_URL, help="Base target URL")
    parser.add_argument("--delay", type=float, default=config.REQUEST_DELAY, help="Delay in seconds between requests")
    parser.add_argument("--no-resume", action="store_true", help="Do not resume from checkpoint file")

    args = parser.parse_args()

    if not args.phone and not args.file and (args.start is None or args.end is None):
        print("[!] No phone, range, or input file specified. Running full scan: 6-series to 9-series.")
        args.start = config.SCAN_START
        args.end = config.SCAN_END

    run_scraper(
        phone=args.phone,
        start=args.start,
        end=args.end,
        input_file=args.file,
        base_url=args.url,
        resume=not args.no_resume,
        delay=args.delay
    )


if __name__ == "__main__":
    main()

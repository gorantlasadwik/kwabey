/**
 * Kwabey Recovery Hub - Client Application Logic
 */

let currentMode = "single";
let isScanning = false;
let eventSource = null;

// DOM Elements
const tabBtns = document.querySelectorAll(".tab-btn");
const modeSingle = document.getElementById("modeSingle");
const modeRange = document.getElementById("modeRange");
const modeList = document.getElementById("modeList");

const btnStartScan = document.getElementById("btnStartScan");
const btnStopScan = document.getElementById("btnStopScan");
const scannerStatusPill = document.getElementById("scannerStatusPill");
const scannerStatusDot = document.getElementById("scannerStatusDot");
const scannerStatusText = document.getElementById("scannerStatusText");

// Inputs
const inputSinglePhone = document.getElementById("inputSinglePhone");
const inputRangeStart = document.getElementById("inputRangeStart");
const inputRangeEnd = document.getElementById("inputRangeEnd");
const checkResume = document.getElementById("checkResume");
const inputNumbersList = document.getElementById("inputNumbersList");
const inputDelay = document.getElementById("inputDelay");
const delayVal = document.getElementById("delayVal");
const inputCustomUrl = document.getElementById("inputCustomUrl");
const toggleSettings = document.getElementById("toggleSettings");
const settingsContent = document.getElementById("settingsContent");

// Stats & Metrics
const statRegistered = document.getElementById("statRegistered");
const statProcessed = document.getElementById("statProcessed");
const statSpeed = document.getElementById("statSpeed");
const statUnregistered = document.getElementById("statUnregistered");
const statErrors = document.getElementById("statErrors");
const statUnknown = document.getElementById("statUnknown");

// Keep-Alive
const keepaliveStatusText = document.getElementById("keepaliveStatusText");
const keepaliveLastTime = document.getElementById("keepaliveLastTime");
const keepaliveCount = document.getElementById("keepaliveCount");
const inputRenderUrl = document.getElementById("inputRenderUrl");
const btnSaveRenderUrl = document.getElementById("btnSaveRenderUrl");

// Table & Terminal
const registeredTableBody = document.getElementById("registeredTableBody");
const tableCountBadge = document.getElementById("tableCountBadge");
const searchTableInput = document.getElementById("searchTableInput");
const terminalLogs = document.getElementById("terminalLogs");
const checkAutoscroll = document.getElementById("checkAutoscroll");
const btnClearLogs = document.getElementById("btnClearLogs");

// All registered data in memory for instant searching
let registeredRecords = [];

// ==========================================
// INITIALIZATION & TAB SWITCHING
// ==========================================

function initTabs() {
  tabBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      tabBtns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      currentMode = btn.getAttribute("data-mode");

      modeSingle.classList.toggle("hidden", currentMode !== "single");
      modeRange.classList.toggle("hidden", currentMode !== "range");
      modeList.classList.toggle("hidden", currentMode !== "list");
    });
  });

  inputDelay.addEventListener("input", (e) => {
    delayVal.textContent = `${e.target.value}s`;
  });

  toggleSettings.addEventListener("click", () => {
    settingsContent.classList.toggle("hidden");
  });
}

// ==========================================
// SSE STREAMING FOR LIVE LOGS
// ==========================================

function connectLogStream() {
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource("/api/logs/stream");

  eventSource.onmessage = (event) => {
    if (!event.data || event.data.trim() === ": ping") return;
    try {
      const data = JSON.parse(event.data);
      appendTerminalLog(data);
    } catch (err) {
      console.error("Failed to parse log event", err);
    }
  };

  eventSource.onerror = () => {
    console.warn("SSE connection interrupted. Reconnecting in 3s...");
    setTimeout(connectLogStream, 3000);
  };
}

function appendTerminalLog(entry) {
  const line = document.createElement("div");
  line.className = `log-line ${entry.level}`;

  let prefix = `[${entry.timestamp}]`;
  let content = entry.message;
  if (entry.phone) {
    content = `${entry.phone.padEnd(12, " ")} -> ${entry.message}`;
  }

  line.textContent = `${prefix} ${content}`;
  terminalLogs.appendChild(line);

  // Keep terminal buffer manageable
  if (terminalLogs.children.length > 200) {
    terminalLogs.removeChild(terminalLogs.firstChild);
  }

  if (checkAutoscroll.checked) {
    terminalLogs.scrollTop = terminalLogs.scrollHeight;
  }

  // If newly registered number is logged, refresh the table immediately
  if (entry.status === "REGISTERED") {
    fetchRegisteredNumbers();
  }
}

// ==========================================
// STATUS POLLING & METRICS
// ==========================================

async function fetchStatus() {
  try {
    const res = await fetch("/api/status");
    if (!res.ok) return;
    const data = await res.json();

    isScanning = data.is_running;
    updateScanUI(isScanning);

    statRegistered.textContent = data.registered_count.toLocaleString();
    statProcessed.textContent = data.total_processed.toLocaleString();
    statSpeed.textContent = `${data.speed} req/s`;
    statUnregistered.textContent = data.unregistered_count.toLocaleString();
    statErrors.textContent = data.error_count.toLocaleString();
    statUnknown.textContent = `${data.unknown_count} unexpected responses`;

    if (data.keepalive) {
      keepaliveLastTime.textContent = data.keepalive.last_time || "Pending";
      keepaliveCount.textContent = data.keepalive.ping_count;
      keepaliveStatusText.textContent = data.keepalive.enabled ? "Active" : "Disabled";
      if (!inputRenderUrl.value && data.keepalive.url) {
        inputRenderUrl.value = data.keepalive.url;
      }
    }
  } catch (err) {
    console.error("Status fetch failed:", err);
  }
}

function updateScanUI(running) {
  if (running) {
    btnStartScan.classList.add("hidden");
    btnStopScan.classList.remove("hidden");
    scannerStatusDot.className = "status-indicator running";
    scannerStatusText.textContent = "SCANNING";
  } else {
    btnStartScan.classList.remove("hidden");
    btnStopScan.classList.add("hidden");
    scannerStatusDot.className = "status-indicator";
    scannerStatusText.textContent = "IDLE";
  }
}

// ==========================================
// REGISTERED NUMBERS TABLE
// ==========================================

async function fetchRegisteredNumbers() {
  try {
    const res = await fetch("/api/registered");
    if (!res.ok) return;
    const json = await res.json();
    registeredRecords = json.data || [];
    renderRegisteredTable(registeredRecords);
  } catch (err) {
    console.error("Failed to load registered numbers:", err);
  }
}

function renderRegisteredTable(records) {
  const searchTerm = searchTableInput.value.trim().toLowerCase();
  const filtered = records.filter(r => (r.phone_number || "").toLowerCase().includes(searchTerm));

  tableCountBadge.textContent = `${records.length} found`;

  if (filtered.length === 0) {
    registeredTableBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="5">${searchTerm ? 'No matching phone numbers found.' : 'No registered numbers discovered yet.'}</td>
      </tr>
    `;
    return;
  }

  registeredTableBody.innerHTML = filtered.map(row => {
    const phone = row.phone_number || "";
    const rawDate = row.discovered_at || row.timestamp || "";
    const dateFormatted = rawDate ? rawDate.replace("T", " ").split(".")[0] : "-";
    return `
      <tr>
        <td><strong>${phone}</strong></td>
        <td><span class="badge-tag registered">REGISTERED</span></td>
        <td>${row.http_status || "200"}</td>
        <td style="font-size:0.75rem; color:#9ca3af;">${dateFormatted}</td>
        <td>
          <button class="copy-btn" onclick="copyToClipboard('${phone}', this)">Copy</button>
        </td>
      </tr>
    `;
  }).join("");
}

window.copyToClipboard = function(text, btn) {
  navigator.clipboard.writeText(text).then(() => {
    const originalText = btn.textContent;
    btn.textContent = "Copied!";
    setTimeout(() => { btn.textContent = originalText; }, 1500);
  });
};

searchTableInput.addEventListener("input", () => {
  renderRegisteredTable(registeredRecords);
});

// ==========================================
// SCAN CONTROL ACTIONS
// ==========================================

btnStartScan.addEventListener("click", async () => {
  const payload = {
    mode: currentMode,
    delay: parseFloat(inputDelay.value),
    url: inputCustomUrl.value.trim(),
    resume: checkResume.checked
  };

  if (currentMode === "single") {
    const phone = inputSinglePhone.value.trim();
    if (!phone) {
      alert("Please enter a phone number to test.");
      return;
    }
    payload.phone = phone;
  } else if (currentMode === "range") {
    const start = parseInt(inputRangeStart.value);
    const end = parseInt(inputRangeEnd.value);
    if (isNaN(start) || isNaN(end) || start >= end) {
      alert("Please enter a valid start and end range (e.g. 9618595000 to 9618596000).");
      return;
    }
    payload.start = start;
    payload.end = end;
  } else if (currentMode === "list") {
    const text = inputNumbersList.value.trim();
    const numbers = text.split("\n").map(n => n.trim()).filter(Boolean);
    if (numbers.length === 0) {
      alert("Please paste at least one phone number in the list.");
      return;
    }
    payload.numbers = numbers;
  }

  try {
    const res = await fetch("/api/scan/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const data = await res.json();
    if (!res.ok) {
      alert(`Error starting scan: ${data.error || "Unknown error"}`);
    } else {
      updateScanUI(true);
    }
  } catch (err) {
    alert(`Request failed: ${err.message}`);
  }
});

btnStopScan.addEventListener("click", async () => {
  try {
    const res = await fetch("/api/scan/stop", { method: "POST" });
    const data = await res.json();
    appendTerminalLog({
      timestamp: new Date().toLocaleTimeString(),
      level: "WARNING",
      message: "Stop signal sent. Scraper will pause safely at current position."
    });
  } catch (err) {
    alert(`Failed to stop scan: ${err.message}`);
  }
});

btnClearLogs.addEventListener("click", () => {
  terminalLogs.innerHTML = "";
});

// ==========================================
// RENDER KEEP-ALIVE CONFIGURATION
// ==========================================

btnSaveRenderUrl.addEventListener("click", async () => {
  const url = inputRenderUrl.value.trim();
  if (!url) {
    alert("Please enter your Render app's public URL.");
    return;
  }

  try {
    const res = await fetch("/api/keepalive/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: url, enabled: true })
    });
    if (res.ok) {
      alert("Render Keep-Alive target URL updated successfully!");
    }
  } catch (err) {
    alert(`Failed to update Keep-Alive URL: ${err.message}`);
  }
});

// ==========================================
// BOOTSTRAP
// ==========================================

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  connectLogStream();
  fetchStatus();
  fetchRegisteredNumbers();

  // Periodic polling for status & new numbers
  setInterval(fetchStatus, 1500);
  setInterval(fetchRegisteredNumbers, 4000);
});

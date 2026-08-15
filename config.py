import os

# Target endpoint
DEFAULT_BASE_URL = "https://kwabey.com/via_gt_ajax/try_to_login/"

# HTTP Settings
TIMEOUT    = 10          # seconds per request
USER_AGENT = "Kwabey-Recovery-Scraper/1.0"
REQUEST_DELAY = 0.0      # no artificial delay — concurrency handles throughput

# Concurrent scan settings
# SCAN_WORKERS: number of simultaneous HTTP threads (tune via Render env var)
# Render free tier: 20 is a safe default; paid tier can go 50-100
SCAN_WORKERS        = int(os.environ.get("SCAN_WORKERS", "20"))
CHECKPOINT_INTERVAL = int(os.environ.get("CHECKPOINT_INTERVAL", "500"))  # save checkpoint every N completions

# Full scan range — 6-series to end of 9-series Indian mobile numbers
SCAN_START = 6_000_000_000
SCAN_END   = 10_000_000_000   # exclusive, so covers up to 9999999999

# Local file paths (used as backup; primary storage is Supabase)
REGISTERED_FILE  = "registered_numbers.csv"   # local backup of registered numbers only
ERROR_LOG_FILE   = "scraper_errors.log"
CHECKPOINT_FILE  = "checkpoint.json"          # local fallback checkpoint (Supabase is primary)

# Response pattern indicators
REGISTERED_PATTERN   = "OTP is sent to mobile number"
UNREGISTERED_PATTERN = "Register Yourself"

"""
Supabase integration for Kwabey Registration Data Recovery Scraper.

Handles:
  1. Saving registered phone numbers to the `registered_numbers` table.
  2. Cloud checkpoint — reading & writing the last processed number to the
     `scraper_checkpoint` table so the scraper survives Render restarts.

Required environment variables (set in Render dashboard or .env):
  SUPABASE_URL   — your project URL, e.g. https://xxxx.supabase.co
  SUPABASE_KEY   — your service-role (or anon) key
"""

import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger("kwabey_scraper")

# ---------------------------------------------------------------------------
# Lazy initialisation — only import supabase if the env vars are present so
# the scraper can still run locally without Supabase configured.
# ---------------------------------------------------------------------------

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client

    url = (
        os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
        or os.environ.get("SUPABASE_URL", "")
    ).strip()

    # Service role key bypasses RLS — preferred for backend scrapers
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY")
        or os.environ.get("SUPABASE_KEY", "")
    ).strip()

    if not url or not key:
        logger.warning(
            "Supabase credentials not set — Supabase features disabled. "
            "Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY."
        )
        return None

    try:
        from supabase import create_client  # type: ignore
        _client = create_client(url, key)
        logger.info("Supabase client initialised (service role).")
        return _client
    except ImportError:
        logger.error("supabase-py not installed. Run: pip install supabase")
        return None
    except Exception as exc:
        logger.error(f"Failed to create Supabase client: {exc}")
        return None


# ---------------------------------------------------------------------------
# Registered numbers
# ---------------------------------------------------------------------------

def save_registered_number(phone: str, http_status: int, timestamp: str) -> bool:
    """
    Upserts a registered phone number into the `registered_numbers` table.

    Table schema (create once in Supabase SQL editor):
        create table if not exists registered_numbers (
            phone_number  text primary key,
            http_status   int,
            discovered_at timestamptz default now()
        );

    Returns True on success, False on failure.
    """
    client = _get_client()
    if client is None:
        return False

    try:
        client.table("registered_numbers").upsert(
            {
                "phone_number": phone,
                "http_status": http_status,
                "discovered_at": timestamp,
            },
            on_conflict="phone_number",
        ).execute()
        logger.info(f"Saved registered number to Supabase: {phone}")
        return True
    except Exception as exc:
        logger.error(f"Supabase: failed to save registered number {phone}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Cloud checkpoint
# ---------------------------------------------------------------------------

CHECKPOINT_ROW_ID = "kwabey_main"   # single-row sentinel key


def load_cloud_checkpoint() -> Optional[str]:
    """
    Fetches the last processed phone number from Supabase.

    Table schema (create once in Supabase SQL editor):
        create table if not exists scraper_checkpoint (
            id              text primary key,
            last_processed  text,
            updated_at      timestamptz default now()
        );

    Returns the phone number string, or None if not found / error.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        result = (
            client.table("scraper_checkpoint")
            .select("last_processed")
            .eq("id", CHECKPOINT_ROW_ID)
            .maybe_single()
            .execute()
        )
        if result.data:
            phone = result.data.get("last_processed")
            logger.info(f"Cloud checkpoint loaded: {phone}")
            return phone
    except Exception as exc:
        logger.error(f"Supabase: failed to load checkpoint: {exc}")

    return None


def save_cloud_checkpoint(phone: str) -> bool:
    """
    Upserts the checkpoint row so the next startup can resume from `phone`.
    Returns True on success, False on failure.
    """
    client = _get_client()
    if client is None:
        return False

    try:
        client.table("scraper_checkpoint").upsert(
            {
                "id": CHECKPOINT_ROW_ID,
                "last_processed": str(phone),
                "updated_at": datetime.now().isoformat(),
            },
            on_conflict="id",
        ).execute()
        return True
    except Exception as exc:
        logger.error(f"Supabase: failed to save checkpoint {phone}: {exc}")
        return False

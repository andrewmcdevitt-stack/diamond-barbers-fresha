"""
keepalive.py
------------
Visits Fresha for each account to keep sessions alive.
Run every 2-3 days to prevent session expiry.
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data"

ACCOUNTS = [
    {
        "label":     "NT (Darwin)",
        "session":   DATA_DIR / "session.json",
        "email_env": "FRESHA_EMAIL",
        "pass_env":  "FRESHA_PASSWORD",
        "timezone":  timezone(timedelta(hours=9, minutes=30)),
    },
    {
        "label":     "QLD (Cairns)",
        "session":   DATA_DIR / "session_cairns.json",
        "email_env": "CAIRNS_FRESHA_EMAIL",
        "pass_env":  "CAIRNS_FRESHA_PASSWORD",
        "timezone":  timezone(timedelta(hours=10)),
    },
]


async def keepalive(account, playwright):
    label        = account["label"]
    session_file = account["session"]

    print(f"\n{'='*50}")
    print(f"ACCOUNT: {label}")
    print(f"{'='*50}")

    if not session_file.exists():
        print(f"  No session file found — skipping.")
        return False

    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        storage_state=str(session_file),
        viewport={"width": 1280, "height": 800},
    )
    page = await context.new_page()

    try:
        await page.goto("https://partners.fresha.com/reports", wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(3000)

        if "/users/sign-in" in page.url:
            print(f"  Session expired — attempting login...")
            email    = os.environ.get(account["email_env"], "")
            password = os.environ.get(account["pass_env"], "")

            try:
                await page.get_by_role("button", name="Accept all").click(timeout=5000)
            except Exception:
                pass

            email_field = page.locator('input[placeholder="Enter your email address"]')
            await email_field.wait_for(timeout=10000)
            await email_field.fill(email)
            await page.click('[data-qa="continue"]', force=True)
            await page.wait_for_selector('input[type="password"]:not([tabindex="-1"])', timeout=15000)
            await page.locator('input[type="password"]:not([tabindex="-1"])').fill(password)

            try:
                await page.locator('button[type="submit"]').click(force=True, timeout=5000)
            except Exception:
                await page.keyboard.press("Enter")

            await page.wait_for_url(lambda url: "/users/sign-in" not in url, timeout=60000)

            if "/users/sign-in" in page.url:
                print(f"  Login failed.")
                return False

            print(f"  Logged in successfully.")

        await context.storage_state(path=str(session_file))
        print(f"  Session refreshed OK.")
        return True

    except Exception as e:
        screenshot = DATA_DIR / f"error_keepalive_{label.split()[0].lower()}.png"
        await page.screenshot(path=str(screenshot))
        print(f"  ERROR: {e}")
        return False
    finally:
        await browser.close()


async def run():
    async with async_playwright() as p:
        for account in ACCOUNTS:
            await keepalive(account, p)
    print("\nKeep-alive complete.")


if __name__ == "__main__":
    asyncio.run(run())

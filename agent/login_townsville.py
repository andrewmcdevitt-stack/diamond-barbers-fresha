"""
login_townsville.py
-------------------
One-time login for the Townsville Fresha account.
Saves session_townsville.json so the working hours script can run unattended.
Also auto-detects and prints the Townsville provider ID.

Run with:  python agent/login_townsville.py
"""

import asyncio
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

DATA_DIR     = Path(__file__).parent.parent / "data"
SESSION_FILE = DATA_DIR / "session_townsville.json"
EMAIL        = os.environ["TOWNSVILLE_FRESHA_EMAIL"]
PASSWORD     = os.environ["TOWNSVILLE_FRESHA_PASSWORD"]


async def login():
    provider_id = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1280, "height": 800},
        )

        # Intercept every request — grab __pid from the first one that has it
        async def capture_pid(request):
            nonlocal provider_id
            if provider_id is None and "__pid=" in request.url:
                params = parse_qs(urlparse(request.url).query)
                if "__pid" in params:
                    provider_id = params["__pid"][0]
                    print(f"  Detected provider ID: {provider_id}")

        context.on("request", capture_pid)

        page = await context.new_page()
        print("Opening Fresha login...")
        await page.goto("https://partners.fresha.com/users/sign-in", wait_until="networkidle")
        await page.wait_for_timeout(2000)

        # Dismiss cookie banner if present
        try:
            await page.get_by_role("button", name="Accept all").click(timeout=5000)
            await page.wait_for_timeout(1000)
        except Exception:
            pass

        # Enter email — try multiple selectors for resilience
        await page.screenshot(path=str(DATA_DIR / "login_debug.png"))
        email_field = page.locator(
            'input[placeholder="Enter your email address"], '
            'input[type="email"], '
            'input[name="email"]'
        ).first
        await email_field.wait_for(timeout=15000)
        await email_field.click()
        await email_field.type(EMAIL, delay=50)
        await page.wait_for_timeout(1000)

        # Click Continue — Fresha now sends a verification code via email (no password field)
        try:
            await page.click('[data-qa="continue"]', force=True, timeout=5000)
        except Exception:
            await page.get_by_role("button", name="Continue").click(force=True)
        await page.wait_for_timeout(2000)

        print("==============================================")
        print("CHECK THE BROWSER WINDOW.")
        print("Fresha has sent a verification code to the email.")
        print("Enter the code in the browser window.")
        print("You have 5 minutes.")
        print("==============================================")
        try:
            await page.wait_for_url(
                lambda url: "/users/sign-in" not in url,
                timeout=300000,
            )
        except Exception:
            pass

        if "/users/sign-in" in page.url:
            print("ERROR: Login failed or timed out.")
            await browser.close()
            return

        # Navigate to reports to trigger API calls so we can capture __pid
        print("Navigating to reports page to detect provider ID...")
        await page.goto("https://partners.fresha.com/reports", wait_until="networkidle")
        await page.wait_for_timeout(4000)

        if not provider_id:
            await page.goto("https://partners.fresha.com/calendar", wait_until="networkidle")
            await page.wait_for_timeout(4000)

        # Save session
        DATA_DIR.mkdir(exist_ok=True)
        await context.storage_state(path=str(SESSION_FILE))
        print(f"\nSession saved to {SESSION_FILE}")

        if provider_id:
            print(f"\nTownsville Provider ID: {provider_id}")
            print("\nAdd this to your .env file:")
            print(f"  TOWNSVILLE_PROVIDER_ID={provider_id}")
        else:
            print("\nCould not auto-detect provider ID.")
            print("Check the browser network tab manually and look for __pid= in any request URL.")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(login())

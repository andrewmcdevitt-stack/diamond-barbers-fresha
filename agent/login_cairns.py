"""
login_cairns.py
---------------
One-time login for the Cairns Fresha account.
Saves session_cairns.json so the working hours script can run unattended.
Also auto-detects and prints the Cairns provider ID.

Run with:  python login_cairns.py
"""

import asyncio
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

DATA_DIR     = Path(__file__).parent.parent / "data"
SESSION_FILE = DATA_DIR / "session_cairns.json"
EMAIL        = os.environ["CAIRNS_FRESHA_EMAIL"]
PASSWORD     = os.environ["CAIRNS_FRESHA_PASSWORD"]


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

        # Enter email
        email_field = page.locator('input[placeholder="Enter your email address"]')
        await email_field.wait_for(timeout=10000)
        await email_field.click()
        await email_field.type(EMAIL, delay=50)
        await page.wait_for_timeout(1000)

        # Click Continue
        await page.click('[data-qa="continue"]', force=True)
        await page.wait_for_selector('input[type="password"]:not([tabindex="-1"])', timeout=15000)
        await page.wait_for_timeout(1000)

        # Enter password
        pwd_field = page.locator('input[type="password"]:not([tabindex="-1"])')
        await pwd_field.fill(PASSWORD)
        await page.wait_for_timeout(1000)

        # Submit
        try:
            await page.locator('button[type="submit"]').click(force=True, timeout=5000)
        except Exception:
            try:
                await page.get_by_role("button", name="Log in").click(force=True, timeout=5000)
            except Exception:
                await page.keyboard.press("Enter")

        print("==============================================")
        print("CHECK THE BROWSER WINDOW.")
        print("Enter the 2FA code if prompted.")
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
            print(f"\nCairns Provider ID: {provider_id}")
            print("\nAdd this line to your .env file:")
            print(f"  CAIRNS_PROVIDER_ID={provider_id}")
        else:
            print("\nCould not auto-detect provider ID.")
            print("Check the browser network tab manually and look for __pid= in any request URL.")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(login())

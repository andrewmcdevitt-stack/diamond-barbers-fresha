"""
inspect_new_booking.py
-----------------------
Opens the live Fresha calendar with a VISIBLE browser and records every
background API call (any domain, any verb) while you manually click through
opening a "New booking" panel.

IMPORTANT: This only watches network traffic. It does NOT click anything for
you, and it does NOT submit/save a booking. Click around freely to open the
new-booking panel and explore the slot/service/client pickers, but do NOT
click the final "Save"/"Confirm" button if you don't want a real appointment
created.

Run with:  python agent/inspect_new_booking.py
Requires:  data/session.json (created by the main agent on first login)
"""

import asyncio
import json
from pathlib import Path

from playwright.async_api import async_playwright

DATA_DIR     = Path(__file__).parent.parent / "data"
SESSION_FILE = DATA_DIR / "session.json"
CALENDAR_URL = "https://partners.fresha.com/calendar"
OUT_PATH     = DATA_DIR / "new_booking_api_calls.json"

# Skip noisy/irrelevant infra so the console stays readable.
IGNORE_SUBSTRINGS = (
    "google", "gstatic", "doubleclick", "facebook", "hotjar", "intercom",
    "segment", "amplitude", "datadog", "sentry", ".css", ".woff", ".png",
    ".jpg", ".svg", ".ico", "favicon",
)

captured = []


def _is_relevant(url: str) -> bool:
    if not url.startswith("http"):
        return False
    return not any(s in url.lower() for s in IGNORE_SUBSTRINGS)


async def inspect():
    if not SESSION_FILE.exists():
        print("ERROR: No data/session.json found. Run agent/weekly_sync.py once first to log in.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            storage_state=str(SESSION_FILE),
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        async def on_request(request):
            url = request.url
            if request.method in ("POST", "PUT", "PATCH") and _is_relevant(url):
                payload = request.post_data
                captured.append({"type": "request", "method": request.method, "url": url, "payload": payload})
                print(f"\n[REQUEST {request.method}] {url}")
                if payload:
                    print(f"  payload: {payload[:300]}")

        async def on_response(response):
            url = response.url
            req_method = response.request.method
            if req_method in ("POST", "PUT", "PATCH") and _is_relevant(url):
                try:
                    body = await response.json()
                except Exception:
                    body = None
                captured.append({"type": "response", "method": req_method, "url": url, "status": response.status, "body": body})
                print(f"[RESPONSE {response.status}] {url}")

        # Listen at the context level (not just `page`) so we still catch
        # everything if the booking panel opens in a new tab/popup.
        context.on("request", on_request)
        context.on("response", on_response)
        context.on("page", lambda new_page: print(f"\n[NEW TAB OPENED] {new_page.url}"))

        print("Opening calendar...")
        await page.goto(CALENDAR_URL, wait_until="networkidle")

        if "/users/sign-in" in page.url:
            print("ERROR: Session expired. Delete data/session.json and re-run agent/weekly_sync.py to log in again.")
            await browser.close()
            return

        print("\n" + "=" * 70)
        print("Calendar is open. In the browser window:")
        print("  1. Click an empty calendar slot (or the 'Add'/'+' button) to")
        print("     open the New Booking panel.")
        print("  2. Pick a service, staff member, and time as you normally would.")
        print("  3. DO NOT click the final Save/Confirm button.")
        print("  4. Come back here and press Enter once you've explored enough.")
        print("=" * 70)
        print("\nWatching for POST/PUT/PATCH calls (the ones that matter for booking creation)...\n")

        input()

        OUT_PATH.write_text(json.dumps(captured, indent=2, default=str), encoding="utf-8")
        print(f"\nCaptured {len(captured)} write call(s). Saved to {OUT_PATH}")
        for item in captured:
            print(f"  [{item['type']:8s}] {item.get('method','')} {item['url']}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(inspect())

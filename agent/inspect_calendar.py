"""
inspect_calendar.py
-------------------
Opens the Fresha calendar and intercepts the background API calls to
staff-working-hours-api.fresha.com and partners-calendar-api.fresha.com
so we can see the exact JSON structure Fresha uses for roster hours.

Run with:  python inspect_calendar.py
"""

import asyncio
import json
from pathlib import Path

from playwright.async_api import async_playwright

DATA_DIR     = Path(__file__).parent.parent / "data"
SESSION_FILE = DATA_DIR / "session.json"
CALENDAR_URL = "https://partners.fresha.com/calendar"

# APIs we want to intercept
WATCH_URLS = [
    "staff-working-hours-api.fresha.com",
    "partners-calendar-api.fresha.com",
    "timesheets-api.fresha.com",
    "partners-api.fresha.com",
    "partners-app.fresha.com",
]

captured = []


async def inspect():
    if not SESSION_FILE.exists():
        print("ERROR: No session.json found. Run the main agent first to log in.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            storage_state=str(SESSION_FILE),
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        # ── Intercept requests AND responses from the target APIs ──
        async def on_request(request):
            url = request.url
            if any(w in url for w in WATCH_URLS):
                try:
                    payload = request.post_data
                    captured.append({"type": "request", "url": url, "payload": payload})
                    print(f"\n  [REQUEST] {url}")
                    if payload:
                        print(f"  PAYLOAD: {payload[:500]}")
                except Exception:
                    pass

        async def on_response(response):
            url = response.url
            if any(w in url for w in WATCH_URLS):
                try:
                    body = await response.json()
                    captured.append({"type": "response", "url": url, "body": body})
                    print(f"\n  [RESPONSE] {url}")
                except Exception:
                    try:
                        text = await response.text()
                        captured.append({"type": "response", "url": url, "body": text})
                        print(f"\n  [RESPONSE text] {url}")
                    except Exception:
                        pass

        page.on("request", on_request)
        page.on("response", on_response)

        print("Opening calendar...")
        await page.goto(CALENDAR_URL, wait_until="networkidle")
        await page.wait_for_timeout(5000)

        if "/users/sign-in" in page.url:
            print("ERROR: Session expired. Delete data/session.json and re-run the main agent.")
            await browser.close()
            return

        print(f"Current URL: {page.url}")
        print(f"\nCaptured {len(captured)} API response(s) so far.")

        # ── Save everything captured to a JSON file ──
        out_path = DATA_DIR / "calendar_api_responses.json"
        out_path.write_text(json.dumps(captured, indent=2, default=str), encoding="utf-8")
        print(f"Saved: {out_path}")

        if captured:
            print("\n=== First captured response (truncated) ===")
            first = captured[0]
            print(f"URL: {first['url']}")
            text = json.dumps(first['body'], indent=2, default=str)
            print(text[:3000])
            print("... [truncated]" if len(text) > 3000 else "")
        else:
            print("\nNo matching API calls captured yet.")
            print("Try scrolling the calendar in the browser window, then press Enter.")

        print("\nPress Enter to close the browser...")
        input()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(inspect())


if __name__ == "__main__":
    asyncio.run(inspect())

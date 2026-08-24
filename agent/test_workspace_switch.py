"""
test_workspace_switch.py
------------------------
Confirms that the Fresha workspace switcher works correctly.
Switches Darwin -> Cairns -> Townsville -> Darwin and verifies each step.

Run with:  python agent/test_workspace_switch.py
"""

import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

DATA_DIR     = Path(__file__).parent.parent / "data"
SESSION_FILE = DATA_DIR / "session_admin.json"
KNOWN_WORKSPACES = ["Darwin", "Townsville", "Cairns"]


async def _click_any(page, name, timeout=5000):
    for loc in [
        page.get_by_role("button",   name=name, exact=True),
        page.get_by_role("option",   name=name, exact=True),
        page.get_by_role("menuitem", name=name, exact=True),
        page.get_by_role("link",     name=name, exact=True),
        page.locator(f'[aria-label="{name}"]'),
        page.get_by_text(name, exact=True).first,
        page.locator("li").filter(has_text=name).first,
        page.locator("div").filter(has_text=name).last,
    ]:
        try:
            if await loc.count() > 0:
                await loc.click(timeout=timeout, force=True)
                return True
        except Exception:
            continue
    return False


async def detect_current_workspace(page):
    for name in KNOWN_WORKSPACES:
        for loc in [
            page.get_by_role("button", name=name, exact=True),
            page.locator(f'text="{name}"').first,
        ]:
            try:
                if await loc.count() > 0:
                    return name
            except Exception:
                continue
    return None


async def switch_workspace(page, target):
    current = await detect_current_workspace(page)
    if current == target:
        return True, "already on target"

    # Open the switcher
    if current:
        opened = await _click_any(page, current, timeout=5000)
    else:
        opened = False

    if not opened:
        return False, "switcher not found"

    await page.wait_for_timeout(1500)

    # Click the target in the panel
    clicked = await _click_any(page, target, timeout=5000)
    if not clicked:
        return False, f"'{target}' not found in panel"

    await page.wait_for_timeout(3000)

    # Confirm the switch
    new_current = await detect_current_workspace(page)
    if new_current == target:
        return True, "confirmed"
    return False, f"still showing '{new_current}' after switch"


async def run():
    if not SESSION_FILE.exists():
        print(f"ERROR: Session file not found: {SESSION_FILE}")
        return

    print(f"Loading session from {SESSION_FILE.name}...\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            storage_state=str(SESSION_FILE),
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        print("Navigating to Fresha...")
        await page.goto(
            "https://partners.fresha.com/calendar",
            wait_until="networkidle",
            timeout=60000,
        )
        await page.wait_for_timeout(3000)

        if "/users/sign-in" in page.url:
            print("ERROR: Session expired — run agent/login_townsville.py to refresh.")
            await browser.close()
            return

        initial = await detect_current_workspace(page)
        print(f"Starting workspace: {initial or '(unknown)'}\n")

        # Test sequence: go through each workspace then back to Darwin
        sequence = ["Cairns", "Townsville", "Darwin"]
        all_passed = True

        for target in sequence:
            print(f"Switching to '{target}'...", end=" ", flush=True)
            ok, reason = await switch_workspace(page, target)
            status = "PASS" if ok else "FAIL"
            print(f"{status}  ({reason})")
            if not ok:
                all_passed = False
                # Take a screenshot to help debug
                shot = DATA_DIR / f"workspace_switch_fail_{target.lower()}.png"
                await page.screenshot(path=str(shot))
                print(f"  Screenshot saved: {shot.name}")

        print()
        print("=" * 40)
        print(f"Result: {'ALL PASSED' if all_passed else 'SOME FAILED'}")
        print("=" * 40)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(run())

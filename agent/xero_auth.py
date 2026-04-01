"""
xero_auth.py
------------
One-time Xero OAuth 2.0 login. Run this once to authorise the app.
It saves a token file so the payroll script can use Xero without logging in again.

Run with:  python agent/xero_auth.py
"""

import base64
import json
import os
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.environ["XERO_CLIENT_ID"]
CLIENT_SECRET = os.environ["XERO_CLIENT_SECRET"]
REDIRECT_URI  = "http://localhost:8080/callback"
TOKEN_FILE    = Path(__file__).parent.parent / "data" / "xero_token.json"

SCOPES = " ".join([
    "openid",
    "profile",
    "email",
    "offline_access",
    "accounting.settings",
    "payroll.timesheets",
    "payroll.employees",
    "payroll.payruns",
    "payroll.payslip",
    "payroll.settings",
])


def exchange_code_for_token(code):
    credentials = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    data = urllib.parse.urlencode({
        "grant_type":   "authorization_code",
        "code":         code,
        "redirect_uri": REDIRECT_URI,
    }).encode()
    req = urllib.request.Request(
        "https://identity.xero.com/connect/token",
        data=data,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get_tenants(access_token):
    """Return all connected Xero organisations."""
    req = urllib.request.Request(
        "https://api.xero.com/connections",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    with urllib.request.urlopen(req) as resp:
        connections = json.loads(resp.read())
    if not connections:
        raise Exception("No Xero organisations connected.")
    tenants = []
    for conn in connections:
        print(f"  Found org: {conn['tenantName']} (id={conn['tenantId']})")
        tenants.append({"name": conn["tenantName"], "id": conn["tenantId"]})
    return tenants


def main():
    # Build the authorisation URL
    params = urllib.parse.urlencode({
        "response_type": "code",
        "client_id":     CLIENT_ID,
        "redirect_uri":  REDIRECT_URI,
        "scope":         SCOPES,
        "state":         "diamond_barbers",
    })
    auth_url = f"https://login.xero.com/identity/connect/authorize?{params}"

    print("Opening Xero login in your browser...")
    print(f"If it doesn't open, go to:\n{auth_url}\n")
    webbrowser.open(auth_url)

    print("=" * 60)
    print("1. Select an organisation from the dropdown in Xero")
    print("2. Click 'Allow Access'")
    print("3. Your browser will show an error page — that's fine!")
    print("4. Copy the FULL URL from your browser's address bar")
    print("   (it starts with http://localhost:8080/callback?code=...)")
    print("=" * 60)

    callback_url = input("\nPaste the full URL here and press Enter:\n> ").strip()

    parsed = urllib.parse.urlparse(callback_url)
    qs     = urllib.parse.parse_qs(parsed.query)

    if "code" not in qs:
        print("\nERROR: No code found in that URL. Make sure you copied the full address bar URL.")
        return

    auth_code = qs["code"][0]
    print("\nCode received. Exchanging for token...")
    token = exchange_code_for_token(auth_code)

    print("Fetching connected Xero organisations...")
    tenants = get_tenants(token["access_token"])
    token["tenants"] = tenants

    TOKEN_FILE.parent.mkdir(exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(token, indent=2))
    print(f"\nToken saved to {TOKEN_FILE}")
    print(f"Connected organisations: {len(tenants)}")
    for t in tenants:
        print(f"  - {t['name']}")
    print("\nSetup complete. You can now run python agent/xero_payroll.py")
    print("Upload data/xero_token.json to your GitHub repo so GitHub Actions can use it.")


if __name__ == "__main__":
    main()

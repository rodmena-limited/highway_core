# --- tools/fetch.py ---
# Implements 'tools.fetch.*' functions.

import requests


def get(url: str, headers: dict = None) -> dict:
    """Makes an HTTP GET request and returns a standardized dict."""
    print(f"  [Tool.Fetch.Get] Fetching {url}...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        return {
            "status": response.status_code,
            "data": response.json(),
            "headers": dict(response.headers),
        }
    except requests.exceptions.RequestException as e:
        print(f"  [Tool.Fetch.Get] FAILED: {e}")
        return {
            "status": getattr(e.response, "status_code", 500),
            "data": str(e),
            "headers": {},
        }


def post(url: str, data: dict = None, headers: dict = None) -> dict:
    """Makes an HTTP POST request and returns a standardized dict."""
    print(f"  [Tool.Fetch.Post] Posting to {url}...")
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        return {
            "status": response.status_code,
            "data": response.json(),
            "headers": dict(response.headers),
        }
    except requests.exceptions.RequestException as e:
        print(f"  [Tool.Fetch.Post] FAILED: {e}")
        return {
            "status": getattr(e.response, "status_code", 500),
            "data": str(e),
            "headers": {},
        }

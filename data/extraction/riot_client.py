"""Riot API client with rate limiting and session management."""
import time
import requests
from config import RIOT_API_KEY

# Global session with API key header
SESSION = requests.Session()
SESSION.headers.update({
    "X-Riot-Token": RIOT_API_KEY,
    "Accept": "application/json",
})

# Rate limit tracking
_last_request_time = 0
_request_interval = 0.05  # 20 requests/second = 50ms between requests


def api_request(url: str, session: requests.Session = None) -> dict | None:
    """Make a rate-limited request to the Riot API.
    
    Args:
        url: Full API URL to request
        session: Optional session to use (defaults to global SESSION)
        
    Returns:
        JSON response as dict, or None on error
    """
    global _last_request_time
    
    if session is None:
        session = SESSION
    
    # Simple rate limiting
    elapsed = time.time() - _last_request_time
    if elapsed < _request_interval:
        time.sleep(_request_interval - elapsed)
    
    _last_request_time = time.time()
    
    try:
        response = session.get(url, timeout=30)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            return api_request(url, session)
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

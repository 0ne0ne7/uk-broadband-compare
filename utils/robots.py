import httpx
import urllib.robotparser as robotparser
from urllib.parse import urlparse
from typing import Optional, Dict

_parsers: Dict[str, Optional[robotparser.RobotFileParser]] = {}

async def get_robots_parser(host: str) -> Optional[robotparser.RobotFileParser]:
    url = f"https://{host}/robots.txt"
    try:
        async with httpx.AsyncClient(timeout=6.0, follow_redirects=True) as client:
            r = await client.get(url)
        if r.status_code >= 400 or not r.text:
            return None
        rp = robotparser.RobotFileParser()
        rp.parse(r.text.splitlines())
        return rp
    except Exception:
        return None

async def robots_allowed(full_url: str, user_agent: str = "Mozilla/5.0") -> bool:
    parsed = urlparse(full_url)
    host = parsed.hostname or ""
    if not host:
        return True
    rp = _parsers.get(host)
    if rp is None:
        rp = await get_robots_parser(host)
        _parsers[host] = rp
    if rp is None:
        return True
    return rp.can_fetch(user_agent, full_url)

# scrapers/scrapers.py

import asyncio
import re
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import (
    async_playwright,
    TimeoutError as PWTimeout,
    Page,
    Browser,
    Error as PWError,  # for safe wrappers
)

from utils.robots import robots_allowed
from utils.cache import provider_of


# -------- Speed & pricing regex helpers --------
SPEED_GB_RE = re.compile(
    r'(\d+(?:\.\d+)?)\s*(?:g(?:ig)?(?:a)?(?:b(?:it)?(?:/s|ps)?)?|gigabit(?:/s|ps)?)\b',
    re.IGNORECASE
)
SPEED_MB_RE = re.compile(
    r'(\d+(?:\.\d+)?)\s*m(?:eg)?b(?:it)?(?:/s|ps)?\b',
    re.IGNORECASE
)

PRICE_RE   = re.compile(r'£\s*([0-9]+(?:\.[0-9]{2})?)\s*(?:/(?:m|month)|per\s*month|a\s*month|pm)?', re.IGNORECASE)
UPFRONT_RE = re.compile(r'(?:upfront|activation|setup|set[-\s]*up)[^£]*£\s*([0-9]+(?:\.[0-9]{2})?)', re.IGNORECASE)
TERM_RE    = re.compile(r'(\d{2})\s*month', re.IGNORECASE)
PLAN_HINTS = re.compile(
    r'(Gigafast|Gigabit|Gig1|Full Fibre|Fibre|Essential|Advanced|Pro|Halo|Complete|Unlimited|M125|M250|Gig1|Superfast|Ultrafast|G\.?fast|FTTP|FTTC|Fast|Faster|Fastest)',
    re.IGNORECASE
)

# Playwright regex selector for any speed text (Mb or Gb), case-insensitive
SPEED_TEXT_SELECTOR = ":text-matches('\\b\\d+(?:\\.\\d+)?\\s*(?:g(?:ig)?b|m(?:eg)?b)(?:ps|/s)?\\b', 'i')"


def _parse_speed_mbps(text: str):
    candidates = []
    for m in SPEED_GB_RE.finditer(text):
        try:
            val = float(m.group(1)) * 1000.0
            candidates.append((val, m.start(), m.end()))
        except Exception:
            continue
    for m in SPEED_MB_RE.finditer(text):
        try:
            val = float(m.group(1))
            candidates.append((val, m.start(), m.end()))
        except Exception:
            continue
    if not candidates:
        return None, None, None
    candidates.sort(key=lambda t: (-t[0], t[1]))
    best_val, start, end = candidates[0]
    return int(round(best_val)), start, end


# -----------------------------
# HTML -> offers extraction
# -----------------------------
def extract_offers_from_html(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    card_candidates = soup.select("""
        [data-component*='product' i],
        [data-component*='card' i],
        [class*='card' i],
        [class*='Tile' i],
        [class*='Product' i],
        section, article, li
    """)

    offers = []
    seen = set()

    for node in card_candidates:
        text = " ".join(node.get_text(separator=" ").split())
        if not text or ("£" not in text) or (("mb" not in text.lower()) and ("gb" not in text.lower())):
            continue

        sp = _parse_speed_mbps(text)
        if not sp or sp[0] is None:
            continue
        speed, sp_start, _ = sp

        pr = PRICE_RE.search(text); uf = UPFRONT_RE.search(text); tm = TERM_RE.search(text)
        if not (speed and pr):
            continue

        price = float(pr.group(1)) if pr else None
        upfront = float(uf.group(1)) if uf else None
        term = int(tm.group(1)) if tm else None

        name = None
        m = PLAN_HINTS.search(text)
        if m:
            start = max(0, m.start() - 25); end = min(len(text), m.end() + 50)
            name = " ".join(text[start:end].split())[:80]
        elif sp_start is not None:
            pre = text[:sp_start].strip().split()
            name = " ".join(pre[-6:]) if pre else None

        key = (speed, price, upfront, term, name)
        if key in seen:
            continue
        seen.add(key)

        offers.append({
            "plan_name": name,
            "speed_mbps": speed,                 # normalized to Mb/s
            "monthly_price_gbp": price,
            "upfront_fee_gbp": upfront,
            "contract_months": term,
            "card_text_sample": text[:240] + ("…" if len(text) > 240 else "")
        })

    uniq = {}
    for o in offers:
        k = (o["speed_mbps"], o["monthly_price_gbp"])
        if k not in uniq:
            uniq[k] = o
    return list(uniq.values())


# -----------------------------
# Site hints (selectors/paths)
# -----------------------------
SITE_HINTS = {
    "bt.com": {
        "fallback_paths": ["/broadband/deals"],
        "cookie_selectors": ["button:has-text('Accept all')", "[aria-label='Accept all']"],
        "postcode_input": [
            "input[placeholder*='postcode' i]",
            "input[name*='postcode' i]",
            "input[id*='postcode' i]",
            "input:near(:text-matches('post\\s*code', 'i'))"
        ],
        "submit_buttons": [
            "button:has-text('Check')", "button:has-text('Find deals')",
            "button:has-text('See deals')", "button:has-text('Go')"
        ],
        "result_selectors": [
            "[data-component*='product' i]",
            "[class*='card' i]",
            SPEED_TEXT_SELECTOR,
            "div:has-text('See deals')",
            "button:has-text('See deals')",
        ]
    },
    "virginmedia.com": {
        "fallback_paths": ["/broadband", "/broadband/deals"],
        "cookie_selectors": ["button:has-text('Accept all')", "button:has-text('Accept All')", "button:has-text('Accept')"],
        "postcode_input": [
            "input[placeholder*='postcode' i]", "[name*='postcode' i]", "[id*='postcode' i]",
            "input:near(:text-matches('post\\s*code', 'i'))"
        ],
        "submit_buttons": [
            "button:has-text('Check availability')", "button:has-text('Check')",
            "button:has-text('Go')", "button:has-text('See deals')"
        ],
        "result_selectors": [
            "[class*='card' i]",
            SPEED_TEXT_SELECTOR,
            "div:has-text('See deals')",
            "button:has-text('See deals')",
        ]
    },
    "sky.com": {
        "fallback_paths": ["/broadband", "/broadband/deals", "/broadband/buy"],
        "cookie_selectors": [
            "button:has-text('Accept all')",
            "button:has-text('Accept All')",
            "label:has-text('Accept all')"
        ],
        "pre_cta_selectors": [
            "[data-test-id='ineligible-button']",
            "a:has-text('See broadband deals')",
            "button:has-text('See broadband deals')",
            "a[href*='/broadband/buy']",
            "a:has-text('See deals')",
            "button:has-text('See deals')"
        ],
        "session_error_text": [
            "session timed out", "session timeout", "something went wrong",
            "sorry, there seems to be a problem", "please try again later",
            "intent error", "we can't process your request right now", "access denied"
        ],
        "postcode_input": [
            "[data-test-id='postcode-lookup-field']",
            "input[placeholder*='postcode' i]",
            "[name*='postcode' i]",
            "[id*='postcode' i]",
            "input:near(:text-matches('post\\s*code', 'i'))"
        ],
        "submit_buttons": [
            "[data-test-id='postcode-lookup-submit']",
            "button:has-text('Check')",
            "button:has-text('Search')",
            "button:has-text('Go')"
        ],
        "result_selectors": [
            "[data-component*='product' i]",
            "[role='listbox'] [role='option']",
            "select option",
            "div:has-text('Are you moving')",
            "div:has-text('moving to this address')",
            "div:has-text('Select your address')",
            "div:has-text('Choose your address')",
            "div:has-text('Confirm address')",
            "div:has-text('See deals')",
            SPEED_TEXT_SELECTOR,
        ]
    },
    "talktalk.co.uk": {
        "fallback_paths": ["/", "/broadband"],
        "cookie_selectors": ["button:has-text('Accept all')", "button:has-text('I accept')"],
        "postcode_input": [
            "input[placeholder*='postcode' i]", "[name*='postcode' i]", "[id*='postcode' i]",
            "input:near(:text-matches('post\\s*code', 'i'))"
        ],
        "submit_buttons": ["button:has-text('Check')", "button:has-text('Go')", "button:has-text('See deals')"],
        "result_selectors": [
            "[class*='card' i]",
            SPEED_TEXT_SELECTOR,
            "div:has-text('See deals')",
            "button:has-text('See deals')",
        ]
    },
    "vodafone.co.uk": {
        "fallback_paths": ["/broadband"],
        "cookie_selectors": ["button:has-text('Accept all')", "button:has-text('Accept All')"],
        "postcode_input": [
            "input[placeholder*='postcode' i]", "[name*='postcode' i]",
            "input:near(:text-matches('post\\s*code', 'i'))"
        ],
        "submit_buttons": ["button:has-text('Check')", "button:has-text('See deals')", "button:has-text('Go')"],
        "result_selectors": [
            "[class*='card' i]",
            SPEED_TEXT_SELECTOR,
            "div:has-text('See deals')",
            "button:has-text('See deals')",
        ]
    },
    "ee.co.uk": {
        "fallback_paths": ["/broadband"],
        "cookie_selectors": ["button:has-text('Accept all')", "button:has-text('Accept All')"],
        "postcode_input": [
            "input[placeholder*='postcode' i]", "[name*='postcode' i]",
            "input:near(:text-matches('post\\s*code', 'i'))"
        ],
        "submit_buttons": ["button:has-text('Check')", "button:has-text('Go')"],
        "result_selectors": [
            "[class*='card' i]",
            SPEED_TEXT_SELECTOR,
        ]
    },
    "plus.net": {
        "fallback_paths": ["/broadband/"],
        "cookie_selectors": ["button:has-text('Accept all')", "button:has-text('Accept All')"],
        "postcode_input": [
            "input[placeholder*='postcode' i]", "[name*='postcode' i]",
            "input:near(:text-matches('post\\s*code', 'i'))"
        ],
        "submit_buttons": ["button:has-text('Check')", "button:has-text('Go')", "button:has-text('See deals')"],
        "result_selectors": [
            "[class*='card' i]",
            SPEED_TEXT_SELECTOR,
            "div:has-text('See deals')",
            "button:has-text('See deals')",
        ]
    },
    "nowtv.com": {
        "fallback_paths": ["/broadband"],
        "cookie_selectors": ["button:has-text('Accept all')", "button:has-text('Accept All')"],
        "postcode_input": [
            "input[placeholder*='postcode' i]", "[name*='postcode' i]", "[id*='postcode' i]",
            "input:near(:text-matches('post\\s*code', 'i'))"
        ],
        "submit_buttons": ["button:has-text('Check')", "button:has-text('Go')"],
        "result_selectors": [
            "[class*='card' i]",
            SPEED_TEXT_SELECTOR,
        ]
    },
}

GENERIC_INPUTS = [
    "input[placeholder*='postcode' i]",
    "input[name*='postcode' i]",
    "input[id*='postcode' i]",
    "input[type='text']",
    "input[aria-label*='postcode' i]",
    "input:near(:text-matches('post\\s*code', 'i'))",
]
GENERIC_SUBMITS = [
    "button:has-text('Check')",
    "button:has-text('Find deals')",
    "button:has-text('See deals')",
    "button:has-text('Check availability')",
    "button:has-text('Search')",
    "button:has-text('Go')",
]
GENERIC_RESULTS = [
    "[data-component*='product' i]",
    "[class*='card' i]",
    SPEED_TEXT_SELECTOR,
    "div:has-text('See deals')",
    "button:has-text('See deals')",
]


# -----------------------------
# Utility
# -----------------------------
def domain_key(url: str) -> str:
    host = urlparse(url).hostname or ""
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


# -------- Safe wrappers to avoid closed-target errors --------
async def safe_page_content(page: Optional[Page]) -> str:
    try:
        if page and not page.is_closed():
            return await page.content()
    except PWError as e:
        if "Target page, context or browser has been closed" not in str(e):
            raise
    return "<html></html>"

async def safe_page_close(page: Optional[Page]):
    try:
        if page and not page.is_closed():
            await page.close()
    except Exception:
        pass


# -----------------------------
# Low-level nav helpers
# -----------------------------
async def accept_cookies(page: Page, selectors: List[str]) -> bool:
    # Try on main page first
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=2000)
                await page.wait_for_timeout(200)
                return True
        except Exception:
            pass

    # Try inside consent iframes (e.g., SP Consent Message)
    try:
        for frame in page.frames:
            title = f"{getattr(frame, 'name', '')} {getattr(frame, 'url', '')}"
            if re.search(r'consent|privacy|sp\s*consent|message', title, re.I) or True:
                for sel in [
                    "label:has-text('Accept all')",
                    "button:has-text('Accept all')",
                    "button:has-text('Accept All')",
                    "button[mode='primary']:has-text('Accept')",
                    *selectors,
                ]:
                    try:
                        loc = frame.locator(sel)
                        if await loc.count() > 0 and await loc.first.is_visible():
                            await loc.first.click(timeout=2000)
                            await page.wait_for_timeout(200)
                            return True
                    except Exception:
                        continue
    except Exception:
        pass

    return False


async def type_postcode_and_submit(page: Page, postcode: str, inputs: List[str], submits: List[str]) -> bool:
    for sel in inputs:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                field = loc.first
                await field.fill("")
                await field.type(postcode, delay=20)
                for bsel in submits:
                    btn = page.locator(bsel)
                    if await btn.count() > 0:
                        try:
                            await btn.first.click(timeout=3000)
                            await page.wait_for_timeout(400)
                            return True
                        except Exception:
                            continue
                try:
                    await field.press("Enter")
                    await page.wait_for_timeout(400)
                    return True
                except Exception:
                    pass
        except Exception:
            continue
    return False


async def wait_for_results(page: Page, selectors: List[str]):
    for sel in selectors:
        try:
            await page.locator(sel).first.wait_for(timeout=8000)
            return
        except PWTimeout:
            continue
    await page.wait_for_timeout(4000)


CONTINUE_BUTTONS = [
    "button:has-text('Continue')","button:has-text('Next')","button:has-text('Confirm')",
    "button:has-text('Proceed')","button:has-text('See deals')","button:has-text('View deals')",
    "button:has-text('Go')","a:has-text('Continue')","a:has-text('Next')",
]

async def click_continue_like(page: Page) -> bool:
    for sel in CONTINUE_BUTTONS:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await page.wait_for_timeout(400)
                return True
        except Exception:
            pass
    return False


# ---------- Sky CTA + session watchdog ----------
async def run_site_pre_actions(page: Page, key: str, hints: Dict, counters: Dict[str, int]) -> None:
    try:
        if key == "sky.com":
            pre_list = hints.get("pre_cta_selectors", [])
            if pre_list:
                current_url = page.url
                path = urlparse(current_url).path or ""
                if "/broadband" in path and "/broadband/buy" not in path:
                    for sel in pre_list:
                        try:
                            loc = page.locator(sel)
                            if await loc.count() > 0 and await loc.first.is_visible():
                                await loc.first.click(timeout=2000)
                                await page.wait_for_load_state("domcontentloaded")
                                counters["goto_count"] += 1
                                try:
                                    await page.mouse.wheel(0, 400)
                                    await page.wait_for_timeout(150)
                                except Exception:
                                    pass
                                break
                        except Exception:
                            # ignore flaky CTA lookups
                            pass
    except Exception:
        # outer guard: never let pre-actions crash the flow
        pass

async def is_sky_session_broken(page: Page, hints: Dict) -> bool:
    try:
        url = page.url.lower()
        if any(k in url for k in ["timeout", "timedout", "error"]):
            return True

        text_signals = hints.get("session_error_text", [])
        scopes = [page.locator("main"), page.locator("body"), page.locator("[role='main']")]
        for phrase in text_signals:
            pattern = f":text-matches('{re.escape(phrase)}', 'i')"
            for scope in scopes:
                if await scope.locator(pattern).count() > 0:
                    return True

        generic = page.locator(":text-matches('went wrong|try again|error|blocked', 'i')")
        if await generic.count() > 0:
            return True
    except Exception:
        pass
    return False

async def sky_go_direct_buy(page: Page, respect_robots: bool) -> bool:
    try:
        target = "https://www.sky.com/broadband/buy"
        if not respect_robots or await robots_allowed(target):
            await page.goto(target, wait_until="domcontentloaded")
            await page.wait_for_timeout(400)
            return True
    except Exception:
        pass
    return False


async def try_handle_address_picker(page: Page, *, address_hint: Optional[str], address_index: int) -> bool:
    picked = False
    try:
        selects = page.locator("select")
        cnt = await selects.count()
        for i in range(cnt):
            sel = selects.nth(i)
            options = await sel.locator("option").all_text_contents()
            if len(options) >= 2 and any(re.search(r'\d+|road|street|flat|house|avenue|close|drive|lane|rd|st', o, re.I) for o in options):
                chosen_label = None
                if address_hint:
                    for o in options:
                        if address_hint.lower() in o.lower():
                            chosen_label = o
                            break
                if not chosen_label:
                    idx = max(0, min(len(options)-1, address_index-1))
                    chosen_label = options[idx]
                try:
                    await sel.select_option(label=chosen_label)
                    await page.wait_for_timeout(300)
                    picked = True
                    break
                except Exception:
                    opt_nodes = sel.locator("option")
                    for j in range(await opt_nodes.count()):
                        txt = (await opt_nodes.nth(j).text_content() or "").strip()
                        if txt == chosen_label.strip():
                            await opt_nodes.nth(j).click()
                            await page.wait_for_timeout(300)
                            picked = True
                            break
            if picked:
                break
    except Exception:
        pass

    if not picked:
        try:
            list_opts = page.locator("[role='listbox'] [role='option']")
            if await list_opts.count() == 0:
                list_opts = page.locator("ul li, ol li").filter(
                    has_text=re.compile(r'\d+|road|street|flat|house|avenue|close|drive|lane|rd|st', re.I)
                )
            count = await list_opts.count()
            if count >= 2:
                idx = 0
                if address_hint:
                    best = -1
                    for i in range(count):
                        txt = (await list_opts.nth(i).inner_text() or "").strip()
                        if address_hint.lower() in txt.lower():
                            best = i
                            break
                    if best >= 0:
                        idx = best
                else:
                    idx = max(0, min(count-1, address_index-1))
                await list_opts.nth(idx).click()
                await page.wait_for_timeout(300)
                picked = True
        except Exception:
            pass

    if picked:
        await click_continue_like(page)

    return picked


LIVE_HERE_LABELS = [
    "I live here and am or have consent from the bill payer",
    "I live here",
    "I currently live at this address",
    "I’m staying at this address",
    "I am staying at this address",
]
MOVING_LABELS = [
    "I am moving to this address or have moved here in the last 14 days",
    "I am moving to this address",
    "I'm moving to this address",
    "moving to this address",
    "I am moving home",
    "I'm moving home",
    "I have moved here in the last 14 days",
]

async def _click_label_with_text(page: Page, phrases: List[str]) -> bool:
    for p in phrases:
        lab = page.locator(f"label:has-text('{p}')")
        if await lab.count() > 0:
            await lab.first.click()
            await page.wait_for_timeout(200)
            return True
    for p in phrases:
        span = page.locator(f":text('{p}')")
        if await span.count() > 0:
            candidate = span.first.locator("xpath=ancestor::label[1]")
            if await candidate.count() > 0:
                await candidate.first.click()
                await page.wait_for_timeout(200)
                return True
    for p in phrases:
        radio_like = page.locator(f"input[type='radio'][aria-label*='{p}']")
        if await radio_like.count() > 0:
            await radio_like.first.check()
            await page.wait_for_timeout(200)
            return True
    return False

async def try_answer_moving_question(page: Page, *, moving: Optional[bool]) -> bool:
    if moving is None:
        return False
    scope = page
    blk = page.locator(":text-matches('moving|live here', 'i')")
    if await blk.count() > 0:
        scope = blk.first
    picked = False
    if moving:
        picked = await _click_label_with_text(scope, MOVING_LABELS) or await _click_label_with_text(scope, ["moving"])
    else:
        picked = await _click_label_with_text(scope, LIVE_HERE_LABELS) or await _click_label_with_text(scope, ["live here"])
    if picked:
        await click_continue_like(page)
        return True
    return False

COMMON_REQUIRED_LABELS = re.compile(r'(house|flat|unit|apartment|building|number|street|address line)', re.I)

async def try_fill_additional_fields(page: Page, extra_fields: Optional[Dict[str, str]]) -> bool:
    changed = False
    if extra_fields:
        for label_txt, val in extra_fields.items():
            try:
                label = page.locator(f"label:has-text('{label_txt}')")
                if await label.count() > 0:
                    control = (await label.first.get_attribute("for")) or ""
                    inp = page.locator(f"#{control}") if control else label.first.locator("xpath=following::input[1]")
                    if await inp.count() > 0:
                        await inp.first.fill(str(val))
                        await page.wait_for_timeout(100)
                        changed = True
                        continue
                inp = page.locator(
                    f"input[placeholder*='{label_txt}' i], input[name*='{label_txt}' i], input[id*='{label_txt}' i]"
                )
                if await inp.count() > 0:
                    await inp.first.fill(str(val))
                    await page.wait_for_timeout(100)
                    changed = True
            except Exception:
                pass

    try:
        text_inputs = page.locator("input[type='text'], input:not([type])")
        cnt = await text_inputs.count()
        for i in range(cnt):
            inp = text_inputs.nth(i)
            try:
                val = await inp.input_value()
                visible = await inp.is_visible()
                if not visible or (val and val.strip()):
                    continue
                lab = None
                lbl_prev = inp.locator("xpath=preceding::label[1]")
                if await lbl_prev.count() > 0:
                    lab = (await lbl_prev.first.text_content() or "").strip()
                else:
                    parent_label = inp.locator("xpath=ancestor::label[1]")
                    if await parent_label.count() > 0:
                        lab = (await parent_label.first.text_content() or "").strip()
                if lab and not COMMON_REQUIRED_LABELS.search(lab):
                    continue
                await inp.fill("1")
                await page.wait_for_timeout(100)
                changed = True
            except Exception:
                continue
    except Exception:
        pass

    if changed:
        await click_continue_like(page)
    return changed


async def drive_flow_until_results(
    page: Page, *, result_selectors: List[str], max_steps: int,
    address_hint: Optional[str], address_index: int,
    moving: Optional[bool], extra_fields: Optional[Dict[str, str]],
    counters: Dict[str, int]
):
    for _ in range(max_steps):
        for sel in result_selectors:
            try:
                if await page.locator(sel).first.count() > 0:
                    return
            except Exception:
                continue

        progressed = False
        if await try_handle_address_picker(page, address_hint=address_hint, address_index=address_index):
            counters["wizard_steps"] += 1
            progressed = True
        if await try_answer_moving_question(page, moving=moving):
            counters["wizard_steps"] += 1
            progressed = True
        if await try_fill_additional_fields(page, extra_fields=extra_fields):
            counters["wizard_steps"] += 1
            progressed = True
        if await click_continue_like(page):
            counters["wizard_steps"] += 1
            progressed = True

        await page.wait_for_timeout(500)
        if not progressed:
            await page.wait_for_timeout(600)

    await wait_for_results(page, result_selectors)


# -----------------------------
# Scraper entrypoints
# -----------------------------
async def scrape_one(
    context, url: str, postcode: str,
    address_hint: Optional[str], address_index: int,
    moving: Optional[bool], extra_fields: Optional[Dict[str, str]],
    max_steps: int = 6, respect_robots: bool = True
) -> Tuple[List[Dict], List[Dict]]:
    """
    Returns (offers, status_rows)
    Includes Sky-specific retry logic to recover from session timeout/intent pages.
    """
    status: List[Dict] = []
    counters = {"goto_count": 0, "wizard_steps": 0}

    key = domain_key(url)
    hints = SITE_HINTS.get(key, {})
    attempts = 3 if key == "sky.com" else 1

    offers: List[Dict] = []

    for attempt in range(1, attempts + 1):
        page: Page = await context.new_page()
        page.set_default_timeout(15000)
        page.set_default_navigation_timeout(25000)

        try:
            await page.mouse.wheel(0, 250)
            await page.wait_for_timeout(150)
        except Exception:
            pass

        if getattr(context, "_pause_on_start", False):
            try:
                await page.pause()
            except Exception:
                pass

        cookie_sel = hints.get("cookie_selectors", ["button:has-text('Accept all')"])
        input_sel = hints.get("postcode_input", GENERIC_INPUTS)
        submit_sel = hints.get("submit_buttons", GENERIC_SUBMITS)
        result_sel = hints.get("result_selectors", GENERIC_RESULTS)
        fallback_paths = hints.get("fallback_paths", [])

        try:
            # 1) initial nav
            try:
                if respect_robots and not await robots_allowed(url):
                    status.append({"provider": provider_of(url), "url": url, "step": f"robots_blocked_initial_a{attempt}", "detail": "", "allowed": False, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})
                    await safe_page_close(page)
                    break
                await page.goto(url, wait_until="domcontentloaded")
                counters["goto_count"] += 1
                status.append({"provider": provider_of(url), "url": url, "step": f"navigated_a{attempt}", "detail": "", "allowed": True, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})
            except Exception:
                base = f"https://{urlparse(url).hostname or ''}/"
                if respect_robots and not await robots_allowed(base):
                    status.append({"provider": provider_of(url), "url": base, "step": f"robots_blocked_base_a{attempt}", "detail": "", "allowed": False, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})
                    await safe_page_close(page)
                    break
                await page.goto(base, wait_until="domcontentloaded")
                counters["goto_count"] += 1
                status.append({"provider": provider_of(url), "url": base, "step": f"navigated_base_a{attempt}", "detail": "", "allowed": True, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})

            # 2) cookies + pre-CTA (Sky)
            await accept_cookies(page, cookie_sel)
            await run_site_pre_actions(page, key, hints, counters)

            # 3) postcode entry (or fallback to /buy)
            ok = await type_postcode_and_submit(page, postcode, input_sel, submit_sel)
            if not ok and key == "sky.com":
                if await sky_go_direct_buy(page, respect_robots):
                    await accept_cookies(page, cookie_sel)
                    ok = await type_postcode_and_submit(page, postcode, input_sel, submit_sel)

            # 4) fallbacks
            if not ok:
                base = f"https://{urlparse(url).hostname or ''}"
                for path in fallback_paths:
                    try:
                        target = base + path
                        if respect_robots and not await robots_allowed(target):
                            status.append({"provider": provider_of(url), "url": target, "step": f"robots_blocked_fallback_a{attempt}", "detail": path, "allowed": False, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})
                            continue
                        await page.goto(target, wait_until="domcontentloaded")
                        counters["goto_count"] += 1
                        status.append({"provider": provider_of(url), "url": target, "step": f"navigated_fallback_a{attempt}", "detail": path, "allowed": True, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})
                        await accept_cookies(page, cookie_sel)
                        await run_site_pre_actions(page, key, hints, counters)
                        ok = await type_postcode_and_submit(page, postcode, input_sel, submit_sel)
                        if ok:
                            break
                    except Exception:
                        continue

            # SKY: detect timeout/intent and recover or retry
            if key == "sky.com" and await is_sky_session_broken(page, hints):
                # soft reload
                try:
                    await page.reload(wait_until="domcontentloaded")
                    await page.wait_for_timeout(600)
                    if not await is_sky_session_broken(page, hints):
                        await run_site_pre_actions(page, key, hints, counters)
                        ok = await type_postcode_and_submit(page, postcode, input_sel, submit_sel)
                except Exception:
                    pass

                # direct /buy on fresh cookies if still broken
                if await is_sky_session_broken(page, hints):
                    try:
                        await page.context.clear_cookies()
                        if await sky_go_direct_buy(page, respect_robots):
                            await accept_cookies(page, cookie_sel)
                            await run_site_pre_actions(page, key, hints, counters)
                            ok = await type_postcode_and_submit(page, postcode, input_sel, submit_sel)
                    except Exception:
                        pass

                # Still broken? retry attempt
                if await is_sky_session_broken(page, hints):
                    status.append({"provider": provider_of(url), "url": page.url,
                                   "step": f"sky_intent_error_a{attempt}", "detail": "intent/timeout page",
                                   "allowed": True, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})
                    await safe_page_close(page)
                    if attempt < attempts:
                        await asyncio.sleep(0.9 * attempt)
                        continue
                    else:
                        break

            # 5) Drive wizard to results
            try:
                await drive_flow_until_results(
                    page, result_selectors=result_sel, max_steps=max_steps,
                    address_hint=address_hint, address_index=address_index, moving=moving,
                    extra_fields=extra_fields, counters=counters
                )
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(1200)
            except Exception:
                pass

            # 6) parse offers
            html = await safe_page_content(page)
            offers = extract_offers_from_html(html)
            status.append({"provider": provider_of(url), "url": url, "step": f"offers_found_a{attempt}", "detail": f"{len(offers)}", "allowed": True, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})
            if not offers:
                status.append({"provider": provider_of(url), "url": url, "step": f"no_offers_a{attempt}", "detail": "", "allowed": True, "goto": counters["goto_count"], "steps": counters["wizard_steps"]})
            await safe_page_close(page)
            break  # success or no offers – stop retrying

        except Exception as e:
            detail = str(e)
            status.append({
                "provider": provider_of(url), "url": url,
                "step": f"exception_a{attempt}", "detail": detail,
                "allowed": None, "goto": counters["goto_count"], "steps": counters["wizard_steps"]
            })
            await safe_page_close(page)
            # Retry on closed-target style failures
            if "Target page, context or browser has been closed" in detail and attempt < attempts:
                await asyncio.sleep(0.6 * attempt)
                continue
            if attempt < attempts:
                await asyncio.sleep(0.6 * attempt)
                continue
            else:
                break

    return offers, status


async def scrape_many(
    postcode: str, urls: List[str],
    address_hint: Optional[str], address_index: int,
    moving: Optional[bool], extra_fields: Optional[Dict[str, str]],
    max_steps: int, respect_robots: bool,
    headed: bool = False,
    slow_mo_ms: int = 0,
    devtools: bool = False,
    record_video_dir: Optional[str] = None,
    record_har_path: Optional[str] = None,
    trace_path: Optional[str] = None,
    pause_on_start: bool = False,
    console_log_path: Optional[str] = None,
) -> Tuple[pd.DataFrame, List[Dict]]:
    # Default debug outputs under logs/
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    if console_log_path is None:
        console_log_path = str(logs_dir / f"console-{ts}.log")
    if record_video_dir is None:
        record_video_dir = str(logs_dir / "videos" / ts)
    if record_har_path is None:
        record_har_path = str(logs_dir / f"network-{ts}.har")
    if trace_path is None:
        trace_path = str(logs_dir / f"trace-{ts}.zip")

    Path(record_video_dir).mkdir(parents=True, exist_ok=True)
    console_fh = open(console_log_path, "a", encoding="utf-8")

    async with async_playwright() as p:
        # Local default: Chromium only (simpler, fewer OS deps)
        engines = [("chromium", p.chromium)]
        last_exception = None

        for engine_name, engine in engines:
            browser: Optional[Browser] = None
            try:
                browser = await engine.launch(
                    headless=not headed,
                    slow_mo=slow_mo_ms or 0,
                    devtools=devtools
                )

                context_kwargs = {
                    "viewport": {"width": 1366, "height": 900},
                    "user_agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                                   "Chrome/120.0.0.0 Safari/537.36"),
                    "locale": "en-GB",
                    "record_video_dir": record_video_dir,
                    "record_har_path": record_har_path,
                }
                context = await browser.new_context(**context_kwargs)

                # Minor stealth & UK environment
                await context.add_init_script("""
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-GB','en']});
""")
                try:
                    await context.set_default_navigation_timeout(25000)
                except Exception:
                    pass

                # ---- Console logging (stdout + file) ----
                def _on_new_page(page: Page):
                    def _log(msg):
                        try:
                            line = (
                                f"{datetime.utcnow().isoformat(timespec='seconds')}Z "
                                f"[{engine_name}:{msg.type}] {msg.text}"
                            )
                            print("[console]", msg.type, msg.text)
                            console_fh.write(line + "\n")
                            console_fh.flush()
                        except Exception:
                            # never let console logging crash the run
                            pass
                    page.on("console", _log)
                context.on("page", _on_new_page)

                # Tracing
                if trace_path:
                    await context.tracing.start(screenshots=True, snapshots=True, sources=True)

                # Allow page.pause() at start if requested
                context._pause_on_start = bool(pause_on_start)

                rows: List[Dict] = []
                status_rows: List[Dict] = []

                tasks = [scrape_one(context, u, postcode, address_hint, address_index, moving, extra_fields,
                                    max_steps=max_steps, respect_robots=respect_robots) for u in urls]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                ts_now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                for u, res in zip(urls, results):
                    prov = provider_of(u)
                    if isinstance(res, Exception):
                        status_rows.append({"provider": prov, "url": u, "step": f"exception({engine_name})", "detail": str(res), "allowed": None, "goto": None, "steps": None})
                        continue
                    offers, strows = res
                    status_rows.extend(strows)
                    for o in offers:
                        rows.append({
                            "provider": prov, "url": u, "postcode": postcode,
                            "plan_name": o.get("plan_name"), "speed_mbps": o.get("speed_mbps"),
                            "monthly_price_gbp": o.get("monthly_price_gbp"),
                            "upfront_fee_gbp": o.get("upfront_fee_gbp"),
                            "contract_months": o.get("contract_months"),
                            "scraped_at": ts_now, "card_text_sample": o.get("card_text_sample"),
                        })

                if trace_path:
                    await context.tracing.stop(path=trace_path)

                await context.close()
                await browser.close()

                console_fh.close()

                df = pd.DataFrame(rows)
                if not df.empty:
                    def mk_id(row):
                        raw = f"{row['provider']}|{row.get('plan_name')}|{row['speed_mbps']}|{row['monthly_price_gbp']}|{row['url']}"
                        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
                    df["row_id"] = df.apply(mk_id, axis=1)

                return df, status_rows

            except Exception as e:
                last_exception = e
                try:
                    if browser:
                        await browser.close()
                except Exception:
                    pass
                # try next engine (only one by default)
                continue

    console_fh.close()
    raise last_exception if last_exception else RuntimeError("All engines failed during scrape_many")

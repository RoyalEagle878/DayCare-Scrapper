import argparse
import base64
import csv
import json
import logging
import os
import random
import re
import shutil
import socket
import socketserver
import select
import threading
import time
import tempfile
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html import unescape
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.common.exceptions import SessionNotCreatedException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib3.util.retry import Retry

import clean_daycare_names as name_cleaner
from adapters.registry import ADAPTER_REGISTRY
from adapters.google import GoogleSearchAdapter
from adapters.winnie import WinnieFallbackAdapter
from apis.registry import API_REGISTRY
from runtime_env import (
    ADAPTER_ONLY_TEST_STATES,
    ACCEPT_LANGUAGE_POOL,
    API_TEST_MAX_WORKERS,
    BASE_DIR,
    BING_SEARCH_URL,
    CALIFORNIA_PROVIDER_SEARCH_API_URL,
    CHECKPOINT_FILE,
    CHECKPOINT_SCHEMA_VERSION,
    CHROME_BINARY_PATH,
    CHROME_PROFILE_DIR,
    CLEANED_INPUT_CSV,
    CSV_CLEANING_ONLY_MODE,
    DEFAULT_MAX_WORKERS,
    ENABLE_GOOGLE_FALLBACK_FOR_API_MISSES,
    ENABLE_TRUSTED_PUBLIC_SEARCH,
    ENRICHMENT_VALUE_FIELDS,
    FETCH_RETRIES,
    FORCE_HEADED,
    CONNECTICUT_PROVIDER_SEARCH_API_URL,
    CONTACT_PAGE_LIMIT,
    GENERIC_OPEN_DATA_API_STATES,
    GOOGLE_API_MISS_SAMPLE_LIMIT,
    GOOGLE_CHECKPOINT_FILE,
    GOOGLE_BAD_PROXY_FILE,
    GOOGLE_MISS_FILE,
    GOOGLE_FALLBACK_MAX_CONCURRENT,
    GOOGLE_HOME_URL,
    GOOGLE_SEARCH_MIN_DELAY_SECONDS,
    GOOGLE_SEARCH_RETRIES,
    GOOGLE_SEARCH_TOTAL_TIMEOUT_SECONDS,
    GOOGLE_SEARCH_URL,
    GOOGLE_USE_HEADLESS,
    GOOGLE_USE_PERSISTENT_PROFILE,
    HEADER_ACCEPT_POOL,
    HTTP_MIN_DELAY_SECONDS,
    INPUT_CSV,
    LEGACY_CHECKPOINT_FILES,
    LEGACY_STAGING_FILE,
    LOG_DIR,
    LOG_FILE,
    LOG_BACKUP_COUNT,
    LOG_MAX_BYTES,
    OUTPUT_CSV,
    OUTPUT_DIR,
    OUTPUT_HEADERS,
    PORTAL_VALIDATION_ALL_ROWS_STATES,
    PORTAL_VALIDATION_SAMPLE_ONLY,
    PORTAL_VALIDATION_SAMPLE_STATES,
    RATE_LIMIT_COOLDOWN_SECONDS,
    REQUEST_TIMEOUT,
    ROTATING_BROWSER_PROXY_ENABLED,
    ROTATING_BROWSER_PROXIES,
    ROTATING_BROWSER_BAD_PROXY_HOSTS,
    ROTATING_BROWSER_PROXY_SCHEME,
    RETRY_BACKOFF_SECONDS,
    RUN_API_STATE_TEST_MODE,
    RUN_GOOGLE_ONLY_SAMPLE_MODE,
    RUN_VALIDATION_SAMPLE,
    SAMPLE_OUTPUT_CSV,
    SEARCH_ENGINE_URL,
    SEARCH_MIN_DELAY_SECONDS,
    SEARCH_RESULTS_LIMIT,
    SEARCH_RETRIES,
    SELENIUM_PAGELOAD_TIMEOUT,
    SELENIUM_WAIT_TIMEOUT,
    SINGLE_PID_FILTER,
    STATE_BATCH_MAX_WORKERS,
    STATE_PORTAL_URLS,
    STATE_SCRAPER_MODELS_FILE,
    STAGING_DIR,
    STAGING_OUTPUT_CSV,
    TEXAS_PROVIDER_DETAIL_URL_TEMPLATE,
    TEXAS_PROVIDER_SEARCH_API_URL,
    USE_STATE_PORTAL_ADAPTERS_ONLY,
    USER_AGENT,
    USER_AGENT_POOL,
    VALIDATION_RANDOM_SEED,
    VALIDATION_SAMPLE_SIZE,
    VALIDATION_STATE_FILTER,
    YAHOO_SEARCH_URL,
    CONTACT_PAGE_LIMIT,
    CONNECTICUT_PROVIDER_SEARCH_API_URL,
)

BLACKLISTED_OFFICIAL_DOMAINS = {
    "facebook.com",
    "m.facebook.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
    "yelp.com",
    "mapquest.com",
    "greatschools.org",
    "privateschoolreview.com",
    "trueschools.com",
    "care.com",
    "nextdoor.com",
    "yellowpages.com",
    "bbb.org",
    "niche.com",
    "zoominfo.com",
    "opencorporates.com",
    "bizapedia.com",
    "childcarereview.com",
    "countyoffice.org",
    "opengovus.com",
    "tootris.com",
}

LISTING_DOMAINS = {
    "care.com",
    "childcarecenter.us",
    "childcarecenters.org",
    "daycare.com",
    "winnie.com",
    "mybrightwheel.com",
    "nationalhealthratings.com",
    "freepreschools.org",
}

TRUSTED_PUBLIC_DOMAINS = {
    "care.com",
    "ocfs.ny.gov",
    "myflfamilies.com",
    "txchildcaresearch.org",
    "childcaresearch.ohio.gov",
    "hs.ocfs.ny.gov",
    "dhs.state.mn.us",
    "secure.in.gov",
    "dcf.wisconsin.gov",
    "licensingregulations.acf.hhs.gov",
}

CONTACT_KEYWORDS = ("contact", "about", "location", "locations", "visit", "find-us")

PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[-.\s]*)?(?:\(?\d{3}\)?[-.\s]*)\d{3}[-.\s]*\d{4}(?!\d)"
)
ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")
CAPACITY_RE = re.compile(
    r"\b(?:maximum\s+)?capacity(?:\s+of)?[:\s]+(\d{1,4})\b", re.IGNORECASE
)
AGE_RANGE_RE = re.compile(
    r"\bages?\s+(?:of\s*:?\s*)?([A-Za-z0-9 ,/&+\-]+?)(?:[.;]|$)", re.IGNORECASE
)
AGE_RANGE_ALT_RE = re.compile(
    r"\b(?:serves|serving|for)\s+children\s+([A-Za-z0-9 ,/&+\-]+?)(?:[.;]|$)",
    re.IGNORECASE,
)
AGE_KEYWORDS = (
    "infant",
    "toddler",
    "preschool",
    "pre-kindergarten",
    "pre kindergarten",
    "pre-k",
    "prek",
    "kindergarten",
    "school age",
    "school-aged",
    "months",
    "month",
    "years",
    "year",
    "grade",
)

AGE_GROUP_TO_RANGE = {
    "Infant": (0, 15),
    "Toddler": (15, 33),
    "Preschool": (33, 72),
    "Pre-Kindergarten": (48, 72),
    "Pre Kindergarten": (48, 72),
    "Pre-K": (48, 72),
    "Kindergarten": (60, 72),
    "School Age": (60, 168),
}

NAME_ABBREVIATION_EXPANSIONS = {
    "CTR": ["CENTER", "CENTRE"],
    "CDC": ["CHILD DEVELOPMENT CENTER"],
    "DCC": ["DAY CARE CENTER", "CHILD CARE CENTER"],
    "ECC": ["EARLY CHILDHOOD CENTER", "EARLY CHILDHOOD EDUCATION CENTER"],
    "CH": ["CHURCH"],
    "CHLD": ["CHILD"],
    "KDGN": ["KINDERGARTEN"],
    "PRE": ["PRESCHOOL", "PRE SCHOOL"],
    "PRESCH": ["PRESCHOOL", "PRE SCHOOL"],
    "PREK": ["PRE KINDERGARTEN"],
    "PRE-K": ["PRE KINDERGARTEN"],
    "SCH": ["SCHOOL"],
    "DEV": ["DEVELOPMENT"],
    "EDUC": ["EDUCATION"],
    "MONSGNR": ["MONSIGNOR"],
    "MNTSSRI": ["MONTESSORI"],
    "ACDMY": ["ACADEMY"],
    "ST": ["SAINT", "ST."],
    "MT": ["MOUNT", "MT."],
}

DECORATIVE_NAME_TOKENS = {
    "THE",
    "INC",
    "LLC",
    "LTD",
    "CORP",
    "CORPORATION",
    "COMPANY",
    "CO",
}

GENERIC_NAME_TOKENS = {
    "DAYCARE",
    "DAY",
    "CARE",
    "CHILDCARE",
    "CHILD",
    "CENTER",
    "CENTRE",
    "CTR",
    "SCHOOL",
    "SCH",
    "PRESCHOOL",
    "PRESCH",
    "PROGRAM",
    "PROGRAMS",
    "ACADEMY",
    "EARLY",
    "LEARNING",
}

RELIGIOUS_COMMUNITY_TOKENS = {
    "ST",
    "SAINT",
    "MT",
    "MOUNT",
    "CHURCH",
    "TEMPLE",
    "BAPTIST",
    "LUTHERAN",
    "CATHOLIC",
    "METHODIST",
    "YMCA",
    "JCC",
    "JEWISH",
    "COMMUNITY",
}

MAX_NAME_VARIANTS = 12

STATE_NAMES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}


def configure_logging() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CHROME_PROFILE_DIR, exist_ok=True)
    logger = logging.getLogger("daycare_enricher")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    logger.debug("Logger configured", extra={})
    return logger


LOGGER = configure_logging()
RUN_GOOGLE_QUERY_MODE = False
ROTATING_BROWSER_PROXY_LOCK = threading.Lock()


class AuthenticatedProxyBridgeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class AuthenticatedProxyBridgeHandler(socketserver.BaseRequestHandler):
    def _recv_until_headers(self) -> bytes:
        data = b""
        self.request.settimeout(20)
        while b"\r\n\r\n" not in data and len(data) < 65536:
            chunk = self.request.recv(8192)
            if not chunk:
                break
            data += chunk
        return data

    def _open_upstream(self) -> socket.socket:
        upstream = socket.create_connection((self.server.upstream_host, self.server.upstream_port), timeout=20)
        upstream.settimeout(20)
        return upstream

    def _proxy_authorization_header(self) -> str:
        token = base64.b64encode(f"{self.server.username}:{self.server.password}".encode("utf-8")).decode("ascii")
        return f"Proxy-Authorization: Basic {token}\r\n"

    def _tunnel(self, left: socket.socket, right: socket.socket) -> None:
        sockets = [left, right]
        while True:
            readable, _, exceptional = select.select(sockets, [], sockets, 30)
            if exceptional:
                break
            if not readable:
                break
            for current in readable:
                try:
                    payload = current.recv(8192)
                except Exception:
                    return
                if not payload:
                    return
                target = right if current is left else left
                try:
                    target.sendall(payload)
                except Exception:
                    return

    def handle(self) -> None:
        upstream = None
        try:
            initial = self._recv_until_headers()
            if not initial:
                return
            header_blob, _, remainder = initial.partition(b"\r\n\r\n")
            header_text = header_blob.decode("iso-8859-1", errors="replace")
            lines = header_text.split("\r\n")
            if not lines:
                return
            request_line = lines[0]
            parts = request_line.split(" ", 2)
            if len(parts) < 3:
                return
            method, target, version = parts
            upstream = self._open_upstream()
            auth_header = self._proxy_authorization_header()
            if method.upper() == "CONNECT":
                connect_request = f"CONNECT {target} {version}\r\nHost: {target}\r\n{auth_header}\r\n".encode("iso-8859-1")
                upstream.sendall(connect_request)
                response = b""
                while b"\r\n\r\n" not in response and len(response) < 65536:
                    chunk = upstream.recv(8192)
                    if not chunk:
                        break
                    response += chunk
                status_line = response.split(b"\r\n", 1)[0]
                if b" 200" not in status_line:
                    self.request.sendall(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                    return
                self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                if remainder:
                    upstream.sendall(remainder)
                self._tunnel(self.request, upstream)
                return

            forward_lines = [request_line]
            saw_proxy_auth = False
            for line in lines[1:]:
                if not line:
                    continue
                lower = line.lower()
                if lower.startswith("proxy-authorization:"):
                    saw_proxy_auth = True
                forward_lines.append(line)
            if not saw_proxy_auth:
                forward_lines.append(auth_header.strip())
            forward_request = ("\r\n".join(forward_lines) + "\r\n\r\n").encode("iso-8859-1") + remainder
            upstream.sendall(forward_request)
            self._tunnel(self.request, upstream)
        except Exception:
            return
        finally:
            try:
                if upstream:
                    upstream.close()
            except Exception:
                pass


class AuthenticatedProxyBridge:
    def __init__(self, upstream_host: str, upstream_port: int, username: str, password: str) -> None:
        self.server = AuthenticatedProxyBridgeServer(("127.0.0.1", 0), AuthenticatedProxyBridgeHandler)
        self.server.upstream_host = upstream_host
        self.server.upstream_port = int(upstream_port)
        self.server.username = username
        self.server.password = password
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def close(self) -> None:
        try:
            self.server.shutdown()
        except Exception:
            pass
        try:
            self.server.server_close()
        except Exception:
            pass


def load_checkpoint_file(path: str) -> Dict[str, Dict[str, object]]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if (
            isinstance(data, dict)
            and data.get("_meta", {}).get("schema_version") == CHECKPOINT_SCHEMA_VERSION
            and isinstance(data.get("rows"), dict)
        ):
            return {
                pid: payload
                for pid, payload in data["rows"].items()
                if isinstance(payload, dict) and isinstance(payload.get("row"), dict)
            }
    except Exception:
        LOGGER.exception("Failed to load checkpoint file from %s", path)
    return {}


def normalize_space(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def clean_text(value: Optional[str]) -> str:
    cleaned = normalize_space(unescape(value or ""))
    if cleaned in {",,", ",", "N/A", "n/a", "NA", "na", "None", "null"}:
        return ""
    return cleaned


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def normalize_phone(value: Optional[str]) -> str:
    if not value:
        return ""
    digits = re.sub(r"\D", "", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}){digits[3:6]}-{digits[6:]}"
    return clean_text(value)


def normalize_zip(value: Optional[str]) -> str:
    if not value:
        return ""
    match = ZIP_RE.search(value)
    return match.group(0) if match else clean_text(value)


def normalize_url(value: Optional[str]) -> str:
    value = clean_text(value)
    if not value:
        return ""
    if value.startswith("//"):
        value = f"https:{value}"
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    return value.rstrip("/")


@dataclass
class NameSearchProfile:
    original_name: str
    normalized_name: str
    search_name_primary: str
    search_name_variants: List[str]


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        normalized = clean_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def normalize_name_text(name: str) -> str:
    name = clean_text(unicodedata.normalize("NFKD", name or ""))
    if not name:
        return ""
    name = name.replace("&", " AND ")
    name = name.replace("/", " ")
    name = name.replace("\\", " ")
    name = name.replace("-", " ")
    name = name.replace("’", "'").replace("`", "'")
    name = re.sub(r"\bPRE[\s\-]*K\b", " PREK ", name, flags=re.IGNORECASE)
    name = re.sub(r"\bPRE[\s\-]*SCHOOL\b", " PRESCHOOL ", name, flags=re.IGNORECASE)
    name = re.sub(r"\b([A-Za-z]+)'S\b", r"\1S", name)
    name = re.sub(r"[^A-Za-z0-9\s']", " ", name)
    name = name.replace("'", "")
    name = re.sub(r"\s+", " ", name).strip().upper()
    return name


def tokenize_provider_name(name: str) -> List[str]:
    normalized = normalize_name_text(name)
    if not normalized:
        return []
    return [token for token in normalized.split() if token]


def remove_tokens(tokens: List[str], blocked: set) -> List[str]:
    return [token for token in tokens if token not in blocked]


def simplify_name(name: str) -> str:
    tokens = tokenize_provider_name(name)
    tokens = remove_tokens(tokens, DECORATIVE_NAME_TOKENS | GENERIC_NAME_TOKENS | {"AND", "OF", "FOR", "AT", "IN"})
    return " ".join(tokens)


def normalize_age_range_value(value: str) -> str:
    value = clean_text(value)
    value = re.sub(r"\s+", " ", value)
    value = value.replace("School", "School-Age") if value == "Infant, Toddler, Pre-Kindergarten, School" else value
    value = re.sub(r"\bin [A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*,\s*[A-Z][a-z]+\b$", "", value).strip(" ,:-")
    if not value:
        return ""
    lowered = value.lower()
    if any(keyword in lowered for keyword in AGE_KEYWORDS) or re.search(r"\b\d+\s*(?:to|-)\s*\d+\b", lowered):
        return value
    return ""


def token_overlap_score(a: str, b: str) -> int:
    a_tokens = set(simplify_name(a).split())
    b_tokens = set(simplify_name(b).split())
    if not a_tokens or not b_tokens:
        return 0
    return len(a_tokens & b_tokens)


def token_overlap_metrics(a: str, b: str) -> Tuple[int, float, float]:
    a_tokens = set(simplify_name(a).split())
    b_tokens = set(simplify_name(b).split())
    if not a_tokens or not b_tokens:
        return 0, 0.0, 0.0
    shared = len(a_tokens & b_tokens)
    return shared, shared / len(a_tokens), shared / len(b_tokens)


def build_core_identity_name(tokens: List[str]) -> str:
    core_tokens = []
    for token in tokens:
        if token in DECORATIVE_NAME_TOKENS:
            continue
        if token in GENERIC_NAME_TOKENS and token not in RELIGIOUS_COMMUNITY_TOKENS:
            continue
        core_tokens.append(token)
    return " ".join(core_tokens)


def build_short_search_name(tokens: List[str]) -> str:
    distinctive = [token for token in tokens if token not in DECORATIVE_NAME_TOKENS and token not in GENERIC_NAME_TOKENS]
    if not distinctive:
        distinctive = [token for token in tokens if token not in DECORATIVE_NAME_TOKENS]
    return " ".join(distinctive[:5])


def build_religious_community_name(tokens: List[str]) -> str:
    prioritized = []
    for token in tokens:
        if token in DECORATIVE_NAME_TOKENS:
            continue
        if token in RELIGIOUS_COMMUNITY_TOKENS or token not in GENERIC_NAME_TOKENS:
            prioritized.append(token)
    return " ".join(prioritized)


def build_name_search_profile(name: str) -> NameSearchProfile:
    original_name = clean_text(name)
    tokens = tokenize_provider_name(name)
    normalized_tokens = remove_tokens(tokens, DECORATIVE_NAME_TOKENS)
    normalized_name = " ".join(normalized_tokens)
    variants: List[str] = []
    if normalized_name:
        variants.append(normalized_name)

    for index, token in enumerate(normalized_tokens):
        for expansion in NAME_ABBREVIATION_EXPANSIONS.get(token, []):
            expanded_tokens = list(normalized_tokens)
            expanded_tokens[index] = normalize_name_text(expansion)
            variants.append(" ".join(expanded_tokens))

    core_identity = build_core_identity_name(normalized_tokens)
    short_search = build_short_search_name(normalized_tokens)
    religious_preserving = build_religious_community_name(normalized_tokens)
    variants.extend([core_identity, short_search, religious_preserving])

    if normalized_name:
        variants.append(re.sub(r"\b(ST|SAINT)\b", "SAINT", normalized_name))
        variants.append(re.sub(r"\b(MT|MOUNT)\b", "MOUNT", normalized_name))

    ranked_variants = dedupe_preserve_order(variants)[:MAX_NAME_VARIANTS]
    primary = ranked_variants[0] if ranked_variants else original_name
    return NameSearchProfile(
        original_name=original_name,
        normalized_name=normalized_name,
        search_name_primary=primary,
        search_name_variants=ranked_variants,
    )


def normalize_provider_name_variants(name: str) -> List[str]:
    return build_name_search_profile(name).search_name_variants


def build_city_search_variants(city: str) -> List[str]:
    normalized_city = normalize_name_text(city)
    if not normalized_city:
        return []
    tokens = normalized_city.split()
    variants = [normalized_city]
    if len(tokens) >= 2:
        variants.append("".join(tokens))
    return dedupe_preserve_order(variants)


def get_record_name_profile(record: Dict[str, str]) -> NameSearchProfile:
    original_name = clean_text(record.get("Original_Name") or record.get("Daycare_Name"))
    normalized_name = clean_text(record.get("Normalized_Name"))
    search_name_primary = clean_text(record.get("Search_Name_Primary"))
    variants_blob = clean_text(record.get("Search_Name_Variants"))
    if normalized_name and search_name_primary:
        variants = [item.strip() for item in variants_blob.split(" || ") if clean_text(item)] if variants_blob else [search_name_primary]
        return NameSearchProfile(
            original_name=original_name,
            normalized_name=normalized_name,
            search_name_primary=search_name_primary,
            search_name_variants=dedupe_preserve_order(variants),
        )
    return build_name_search_profile(original_name)


def apply_name_profile_to_row(row: Dict[str, str], profile: NameSearchProfile) -> None:
    row["Original_Name"] = profile.original_name
    row["Normalized_Name"] = profile.normalized_name
    row["Search_Name_Primary"] = profile.search_name_primary
    row["Search_Name_Variants"] = " || ".join(profile.search_name_variants)


def classify_match_status(
    record: Dict[str, str],
    candidate_name: str,
    candidate_city: str = "",
    candidate_address: str = "",
    candidate_phone: str = "",
    candidate_url: str = "",
    closed_hint: str = "",
    prior_name_hint: bool = False,
) -> Tuple[str, str, str]:
    original_name = clean_text(record.get("Daycare_Name"))
    normalized_name = clean_text(record.get("Normalized_Name")) or build_name_search_profile(original_name).normalized_name
    candidate_name = clean_text(candidate_name)
    candidate_city = clean_text(candidate_city)
    candidate_address = clean_text(candidate_address)
    candidate_phone = normalize_phone(candidate_phone)
    candidate_url = normalize_url(candidate_url)
    shared, recall, precision = token_overlap_metrics(normalized_name or original_name, candidate_name)
    record_city = clean_text(record.get("Mailing_City")).lower()
    city_match = record_city == candidate_city.lower() if candidate_city else False
    if not city_match and record_city and candidate_address:
        city_match = record_city in candidate_address.lower()
    support_count = sum(
        1
        for flag in [
            bool(candidate_phone),
            bool(candidate_url),
            bool(candidate_address),
            prior_name_hint,
        ]
        if flag
    )
    closed_text = clean_text(closed_hint).lower()
    confidence = min(99, int(35 + shared * 15 + recall * 25 + support_count * 6 + (8 if city_match else 0)))
    if closed_text and any(marker in closed_text for marker in ("close", "inactive", "revoked", "suspend")):
        return "closed_likely", str(max(confidence, 75)), "Official source indicates the facility is closed or inactive."
    if city_match and (normalize_name_text(candidate_name) == normalized_name or (shared >= 2 and recall >= 0.95 and precision >= 0.75)):
        return "exact_match", str(max(confidence, 92)), "Candidate name closely matches the cleaned daycare name in the same city."
    if city_match and any(normalize_name_text(candidate_name) == variant for variant in get_record_name_profile(record).search_name_variants):
        return "expanded_match", str(max(confidence, 84)), "Candidate matched one of the ranked expanded search-name variants."
    if city_match and shared >= 2 and (recall >= 0.55 or support_count >= 2):
        status = "renamed_likely" if prior_name_hint or recall < 0.8 else "partial_match"
        reason = (
            "City matched and the official/source record appears renamed but still has strong overlapping identity tokens."
            if status == "renamed_likely"
            else "City matched and the provider name partially overlaps with supporting address/contact evidence."
        )
        return status, str(max(confidence, 68)), reason
    return "not_found", str(min(confidence, 45)), "Candidate did not meet the balanced city-plus-name acceptance threshold."


def looks_like_pdf(url: str) -> bool:
    return url.lower().endswith(".pdf")


def is_blacklisted_official(url: str) -> bool:
    return domain_of(url) in BLACKLISTED_OFFICIAL_DOMAINS


def is_listing_domain(url: str) -> bool:
    return domain_of(url) in LISTING_DOMAINS


def likely_official_domain(url: str) -> bool:
    if looks_like_pdf(url):
        return False
    if is_blacklisted_official(url):
        return False
    if is_listing_domain(url):
        return False
    domain = domain_of(url)
    if not domain:
        return False
    return True


def looks_like_street_address(value: str) -> bool:
    value = clean_text(value)
    if not value:
        return False
    if re.fullmatch(r"\d(?:-\d)+", value):
        return False
    if len(value) < 8:
        return False
    return bool(
        re.search(
            r"\b\d{1,6}\s+[A-Za-z0-9#.'\-]+\s+(?:[A-Za-z0-9#.'\-]+\s+){0,5}"
            r"(?:ST|STREET|RD|ROAD|AVE|AVENUE|BLVD|BOULEVARD|DR|DRIVE|LN|LANE|CT|COURT|HWY|HIGHWAY|PKWY|PARKWAY|PL|PLACE|CIR|CIRCLE|WAY)\b",
            value,
            re.IGNORECASE,
        )
    )


def is_trusted_public_source(url: str) -> bool:
    domain = domain_of(url)
    if not domain:
        return False
    if domain.endswith(".gov") or domain.endswith(".us"):
        return True
    return domain in TRUSTED_PUBLIC_DOMAINS


def has_meaningful_enrichment(row: Dict[str, str]) -> bool:
    return any(clean_text(row.get(field)) for field in ENRICHMENT_VALUE_FIELDS)


def has_fetched_enrichment(
    row: Dict[str, str], sources: Optional[Dict[str, Dict[str, str]]] = None
) -> bool:
    if not has_meaningful_enrichment(row):
        return False
    if not sources:
        return True
    for field in ENRICHMENT_VALUE_FIELDS:
        value = clean_text(row.get(field))
        source_type = clean_text((sources.get(field) or {}).get("source_type"))
        if value and source_type and source_type != "input_file":
            return True
    return False


def build_source_entry(
    value: str,
    source_url: str = "",
    source_type: str = "",
    notes: str = "",
) -> Dict[str, str]:
    source_url = normalize_url(source_url)
    return {
        "value": clean_text(value),
        "source_url": source_url,
        "source_domain": domain_of(source_url),
        "source_type": clean_text(source_type),
        "notes": clean_text(notes),
    }


def format_age_groups(age_groups: List[str]) -> str:
    ordered = []
    seen = set()
    for value in age_groups:
        normalized = clean_text(value)
        if normalized and normalized not in seen:
            ordered.append(normalized)
            seen.add(normalized)
    return ", ".join(ordered)


def months_to_range_label(months: int) -> str:
    if months % 12 == 0 and months >= 12:
        years = months // 12
        return f"{years} year" if years == 1 else f"{years} years"
    if months == 0:
        return "0 months"
    return f"{months} month" if months == 1 else f"{months} months"


def age_groups_to_numeric_range(age_groups: List[str]) -> str:
    ranges = [AGE_GROUP_TO_RANGE[group] for group in age_groups if group in AGE_GROUP_TO_RANGE]
    if not ranges:
        return ""
    min_months = min(start for start, _ in ranges)
    max_months = max(end for _, end in ranges)
    return f"{months_to_range_label(min_months)} - {months_to_range_label(max_months)}"


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    ordered = []
    for value in values:
        normalized = clean_text(value)
        if normalized and normalized not in seen:
            ordered.append(normalized)
            seen.add(normalized)
    return ordered


def normalize_age_groups_text_to_numeric_range(value: str) -> str:
    normalized = clean_text(value)
    if not normalized:
        return ""
    numeric_style = normalized.lower()
    if re.search(r"\b\d+\s*(?:month|months|year|years|yr|yrs)\b", numeric_style):
        normalized = re.sub(r"\byrs\b", "years", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\byr\b", "year", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\s+", " ", normalized)
        normalized = normalized.replace(" to ", " - ")
        return normalized
    normalized = normalized.replace("Prekindergarten", "Pre-Kindergarten")
    normalized = normalized.replace("Pre Kindergarten", "Pre-Kindergarten")
    normalized = normalized.replace("Pre K", "Pre-K")
    parts = re.split(r"\s*,\s*|\s*/\s*|\s+and\s+", normalized)
    matched_groups = []
    for part in parts:
        token = clean_text(part)
        if not token:
            continue
        for known_group in AGE_GROUP_TO_RANGE:
            if token.lower() == known_group.lower():
                matched_groups.append(known_group)
                break
    return age_groups_to_numeric_range(dedupe_preserve_order(matched_groups))


def format_numeric_age_range(min_value: str, max_value: str, unit: str = "years") -> str:
    minimum = clean_text(min_value)
    maximum = clean_text(max_value)
    if not minimum or not maximum:
        return ""
    if not re.fullmatch(r"\d+(?:\.\d+)?", minimum) or not re.fullmatch(r"\d+(?:\.\d+)?", maximum):
        return normalize_age_groups_text_to_numeric_range(f"{minimum} - {maximum}")
    min_number = float(minimum)
    max_number = float(maximum)
    if unit == "months":
        min_label = months_to_range_label(int(round(min_number)))
        max_label = months_to_range_label(int(round(max_number)))
        return f"{min_label} - {max_label}"
    min_label = f"{int(min_number)} year" if min_number == 1 else f"{int(min_number)} years"
    max_label = f"{int(max_number)} year" if max_number == 1 else f"{int(max_number)} years"
    return f"{min_label} - {max_label}"


def first_non_empty(candidate: Dict[str, object], keys: List[str]) -> str:
    for key in keys:
        value = clean_text(str(candidate.get(key, "")))
        if value:
            return value
    return ""


def extract_google_target_url(href: str) -> str:
    href = clean_text(href)
    if not href:
        return ""
    parsed = urlparse(href)
    if "google." in parsed.netloc and parsed.path == "/url":
        query = parse_qs(parsed.query or "")
        target = (query.get("q") or [""])[0]
        if target:
            try:
                return normalize_url(unquote(target))
            except Exception:
                return ""
    return normalize_url(href)


def extract_bing_target_url(href: str) -> str:
    href = clean_text(href)
    if not href:
        return ""
    parsed = urlparse(href)
    if "bing.com" in parsed.netloc and parsed.path.startswith("/ck/"):
        query = parsed.query or ""
        match = re.search(r"(?:^|&)u=([^&]+)", query)
        if match:
            try:
                encoded = unquote(match.group(1))
                if encoded.startswith("a1"):
                    encoded = encoded[2:]
                import base64

                padding = "=" * (-len(encoded) % 4)
                decoded = base64.b64decode(encoded + padding).decode("utf-8", "ignore")
                return normalize_url(decoded)
            except Exception:
                return href
    return normalize_url(href)


def is_internal_search_engine_url(url: str) -> bool:
    parsed = urlparse(clean_text(url))
    domain = parsed.netloc.lower().replace("www.", "")
    path = parsed.path or ""
    if not domain:
        return True
    if domain.endswith("google.com"):
        if path in {"/search", "/url", "/sorry", "/httpservice/retry/enablejs"}:
            return True
        if domain == "support.google.com":
            return True
    if domain.endswith("bing.com") and path.startswith(("/search", "/ck/", "/copilotsearch")):
        return True
    if domain.endswith("yahoo.com") and path.startswith(("/search", "/s")):
        return True
    return False


def looks_like_junk_search_result(url: str, title: str, snippet: str) -> bool:
    haystack = " ".join([clean_text(url).lower(), clean_text(title).lower(), clean_text(snippet).lower()])
    if not haystack:
        return True
    junk_markers = (
        "enable javascript",
        "unusual traffic",
        "support.google.com",
        "httpservice/retry/enablejs",
        "captcha",
        "access denied",
        "retry",
        "feedback",
        "all regions",
        "privacy policy",
        "terms of service",
    )
    return any(marker in haystack for marker in junk_markers)


def is_usable_search_result(url: str, title: str, snippet: str = "") -> bool:
    url = normalize_url(url)
    title = clean_text(title)
    snippet = clean_text(snippet)
    if not url or not title:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    if is_internal_search_engine_url(url):
        return False
    if looks_like_junk_search_result(url, title, snippet):
        return False
    return True


@dataclass
class SearchResult:
    rank: int
    title: str
    url: str
    snippet: str
    provider: str = ""


@dataclass
class PortalSearchResult:
    title: str
    detail_url: str
    address: str
    program_type: str


def build_random_request_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENT_POOL),
        "Accept": random.choice(HEADER_ACCEPT_POOL),
        "Accept-Language": random.choice(ACCEPT_LANGUAGE_POOL),
        "Cache-Control": random.choice(["max-age=0", "no-cache"]),
        "DNT": random.choice(["1", "0"]),
        "Upgrade-Insecure-Requests": "1",
    }


def build_random_browser_profile() -> Dict[str, str]:
    return {
        "user_agent": random.choice(USER_AGENT_POOL),
        "accept_language": random.choice(ACCEPT_LANGUAGE_POOL),
    }


def flatten_dict_rows(payload: object) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if isinstance(payload, dict):
        rows.append(payload)
        return rows
    if isinstance(payload, list):
        for item in payload:
            rows.extend(flatten_dict_rows(item))
    return rows


def pick_best_name_token(name: str) -> str:
    tokens = [clean_text(token) for token in simplify_name(name).split() if clean_text(token)]
    filtered = [
        token
        for token in tokens
        if token not in {"DAY", "CARE", "CHILD", "CHILDREN", "CENTER", "SCHOOL", "PRESCHOOL", "NURSERY", "ACADEMY"}
    ]
    ranked = sorted(filtered or tokens, key=lambda token: (-len(token), token))
    return ranked[0] if ranked else ""


GENERIC_PROVIDER_NAME_FIELDS = [
    "provider_name",
    "providername",
    "facility_name",
    "operation_name",
    "resource_name",
    "name",
    "site_name",
    "program_name",
    "legal_name",
]

GENERIC_CITY_FIELDS = [
    "city",
    "site_city",
    "physicalcity",
    "provider_town",
    "facility_city",
    "mailing_city",
]

GENERIC_ADDRESS_FIELDS = [
    "street_address",
    "site_street_address",
    "physicalstreetaddress",
    "facility_address",
    "address_line",
    "address1",
    "address_1",
    "address",
]

GENERIC_STATE_FIELDS = [
    "state",
    "site_state",
    "physicalstate",
    "facility_state",
    "statecode",
]

GENERIC_ZIP_FIELDS = [
    "zip",
    "zipcode",
    "physicalzip",
    "site_zip_code",
    "facility_zip",
    "zip_code",
]

GENERIC_PHONE_FIELDS = [
    "phone",
    "phone_number",
    "primarycontactphonenumber",
    "facility_telephone_number",
    "telephone",
]

GENERIC_URL_FIELDS = [
    "url",
    "website",
    "website_address",
    "facility_website",
]

GENERIC_CAPACITY_FIELDS = [
    "capacity",
    "licensed_capacity",
    "licensecapacity",
    "facility_capacity",
    "total_capacity",
    "total_licensed_capacity",
    "maximumcapacity",
]

GENERIC_AGE_FIELDS = [
    "age_range",
    "ages_served",
    "licensed_to_serve_ages",
    "minimumage",
    "maximumage",
    "startingage",
    "endingage",
]


class RateLimitedSession:
    def __init__(self, min_delay_seconds: float = 1.0) -> None:
        self.session = requests.Session()
        self.session.headers.update(build_random_request_headers())
        retry_total = 0 if RUN_GOOGLE_ONLY_SAMPLE_MODE else 2
        retry = Retry(
            total=retry_total,
            connect=retry_total,
            read=retry_total,
            status=retry_total,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD", "POST"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.min_delay_seconds = min_delay_seconds
        self.lock = threading.Lock()
        self.last_request_time = 0.0
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay_seconds:
                time.sleep(self.min_delay_seconds - elapsed + random.uniform(0.05, 0.25))
            merged_headers = dict(build_random_request_headers())
            if isinstance(kwargs.get("headers"), dict):
                merged_headers.update(kwargs["headers"])
            kwargs["headers"] = merged_headers
            response = self.session.request(method=method.upper(), url=url, timeout=REQUEST_TIMEOUT, **kwargs)
            self.last_request_time = time.time()
        response.raise_for_status()
        return response

    def get(self, url: str, **kwargs) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        return self.request("POST", url, **kwargs)


class DaycareEnricher:
    def __init__(self, checkpoint_file_override: Optional[str] = None) -> None:
        self.session = RateLimitedSession(min_delay_seconds=HTTP_MIN_DELAY_SECONDS)
        self.checkpoint_file = checkpoint_file_override or CHECKPOINT_FILE
        self.checkpoint_lock = threading.Lock()
        self.search_lock = threading.Lock()
        self.last_search_time = 0.0
        self.google_fallback_lock = threading.Lock()
        self.google_fallback_attempts = 0
        self.google_fallback_semaphore = threading.Semaphore(GOOGLE_FALLBACK_MAX_CONCURRENT)
        self.state_scraper_models = load_state_scraper_models()
        self.driver_local = threading.local()
        self.driver_lock = threading.Lock()
        self.driver_registry: List[webdriver.Chrome] = []
        self.state_portal_drivers: Dict[str, webdriver.Chrome] = {}
        self.state_portal_base_handles: Dict[str, str] = {}
        self.state_portal_query_handles: Dict[str, str] = {}
        self.state_portal_session_flags: Dict[str, Dict[str, object]] = {}
        self.state_portal_run_locks: Dict[str, threading.Lock] = {}
        self.api_retry_lock = threading.Lock()
        self.pending_api_city_only_retries: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}
        self.api_city_only_retry_active_states: set[str] = set()
        self.adapter_timeout_retry_lock = threading.Lock()
        self.pending_adapter_timeout_retries: Dict[str, Dict[str, Dict[str, str]]] = {}
        self.winnie_retry_lock = threading.Lock()
        self.winnie_run_lock = threading.Lock()
        self.pending_winnie_retries: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}
        self.winnie_backoff_lock = threading.Lock()
        self.winnie_backoff_seconds: Dict[str, float] = {}
        self.google_backoff_lock = threading.Lock()
        self.google_backoff_seconds: Dict[str, float] = {}
        self.google_retry_lock = threading.Lock()
        self.pending_google_antibot_retries: Dict[str, Dict[str, str]] = {}
        self.google_miss_lock = threading.Lock()
        self.google_miss_pids = self.load_google_miss_registry()
        self.browser_proxy_queue_lock = threading.Lock()
        self.good_browser_proxy_queue: List[str] = []
        self.bad_browser_proxy_queue: List[str] = []
        self.temp_profile_dirs: List[str] = []
        self.proxy_bridges: List[AuthenticatedProxyBridge] = []
        self.staging_lock = threading.Lock()
        self.checkpoint = self.load_checkpoint()
        self.staging = self.checkpoint
        self.initialize_browser_proxy_queues()
        LOGGER.info(
            "DaycareEnricher initialized with %s checkpoint rows, %s staging rows, %s state scraper models",
            len(self.checkpoint),
            len(self.staging),
            len(self.state_scraper_models),
        )

    def initialize_browser_proxy_queues(self) -> None:
        proxies = [clean_text(item) for item in ROTATING_BROWSER_PROXIES if clean_text(item)]
        bad_hosts = [clean_text(item) for item in ROTATING_BROWSER_BAD_PROXY_HOSTS if clean_text(item)]
        if os.path.exists(GOOGLE_BAD_PROXY_FILE):
            try:
                with open(GOOGLE_BAD_PROXY_FILE, "r", encoding="utf-8") as handle:
                    raw_value = handle.read().strip()
                persisted_hosts = json.loads(raw_value) if raw_value else []
                if isinstance(persisted_hosts, list):
                    bad_hosts.extend(clean_text(item) for item in persisted_hosts if clean_text(item))
            except Exception:
                LOGGER.warning("Ignoring invalid Google bad proxy file at %s", GOOGLE_BAD_PROXY_FILE)
        bad_queue: List[str] = []
        used_bad_hosts: List[str] = []
        for host in bad_hosts:
            matched = next((entry for entry in proxies if clean_text(entry.split(":")[0]) == host), "")
            if matched:
                bad_queue.append(matched)
                used_bad_hosts.append(host)
        bad_host_set = set(used_bad_hosts)
        good_queue = [entry for entry in proxies if clean_text(entry.split(":")[0]) not in bad_host_set]
        with self.browser_proxy_queue_lock:
            self.good_browser_proxy_queue = good_queue
            self.bad_browser_proxy_queue = bad_queue

    def save_bad_browser_proxy_hosts(self) -> None:
        with self.browser_proxy_queue_lock:
            hosts = []
            for entry in self.bad_browser_proxy_queue:
                parts = clean_text(entry).split(":")
                if len(parts) == 4 and clean_text(parts[0]):
                    hosts.append(clean_text(parts[0]))
        deduped_hosts = []
        seen = set()
        for host in hosts:
            if host in seen:
                continue
            seen.add(host)
            deduped_hosts.append(host)
        try:
            with open(GOOGLE_BAD_PROXY_FILE, "w", encoding="utf-8") as handle:
                json.dump(deduped_hosts, handle, indent=2)
        except Exception:
            LOGGER.exception("Failed saving Google bad proxy file to %s", GOOGLE_BAD_PROXY_FILE)

    def load_google_miss_registry(self) -> set:
        if not os.path.exists(GOOGLE_MISS_FILE):
            return set()
        try:
            with open(GOOGLE_MISS_FILE, "r", encoding="utf-8") as handle:
                raw_value = handle.read().strip()
            if not raw_value:
                return set()
            payload = json.loads(raw_value)
            if isinstance(payload, list):
                return {clean_text(item) for item in payload if clean_text(item)}
        except Exception:
            LOGGER.warning("Ignoring invalid Google miss file at %s", GOOGLE_MISS_FILE)
        return set()

    def save_google_miss_registry(self) -> None:
        with self.google_miss_lock:
            payload = sorted(pid for pid in self.google_miss_pids if clean_text(pid))
        try:
            with open(GOOGLE_MISS_FILE, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        except Exception:
            LOGGER.exception("Failed saving Google miss file to %s", GOOGLE_MISS_FILE)

    def is_google_miss(self, pid: str) -> bool:
        pid = clean_text(pid)
        if not pid:
            return False
        with self.google_miss_lock:
            return pid in self.google_miss_pids

    def mark_google_miss(self, pid: str) -> None:
        pid = clean_text(pid)
        if not pid:
            return
        with self.google_miss_lock:
            self.google_miss_pids.add(pid)
        self.save_google_miss_registry()

    def clear_google_miss(self, pid: str) -> None:
        pid = clean_text(pid)
        if not pid:
            return
        removed = False
        with self.google_miss_lock:
            if pid in self.google_miss_pids:
                self.google_miss_pids.remove(pid)
                removed = True
        if removed:
            self.save_google_miss_registry()

    def clear_bad_browser_proxy_hosts(self, hosts_to_clear: List[str]) -> None:
        targets = {clean_text(host) for host in hosts_to_clear if clean_text(host)}
        if not targets:
            return
        with self.browser_proxy_queue_lock:
            self.bad_browser_proxy_queue = [
                entry
                for entry in self.bad_browser_proxy_queue
                if clean_text(entry).split(":")[0] not in targets
            ]
        self.save_bad_browser_proxy_hosts()

    def pop_next_browser_proxy_entry(self) -> str:
        with self.browser_proxy_queue_lock:
            if not self.good_browser_proxy_queue and self.bad_browser_proxy_queue:
                self.good_browser_proxy_queue = list(self.bad_browser_proxy_queue)
                self.bad_browser_proxy_queue = []
                LOGGER.info("Browser proxy queue recycled; promoted bad queue into good queue")
            if not self.good_browser_proxy_queue:
                return ""
            return self.good_browser_proxy_queue.pop(0)

    def queue_bad_browser_proxy_entry(self, proxy_entry: str) -> None:
        proxy_entry = clean_text(proxy_entry)
        if not proxy_entry:
            return
        with self.browser_proxy_queue_lock:
            self.bad_browser_proxy_queue.append(proxy_entry)
        self.save_bad_browser_proxy_hosts()

    def queue_active_google_proxies_as_bad(self) -> None:
        current_entry = clean_text(getattr(self.driver_local, "current_browser_proxy_entry", ""))
        previous_entry = clean_text(getattr(self.driver_local, "last_browser_proxy_entry", ""))
        if current_entry:
            self.queue_bad_browser_proxy_entry(current_entry)
        if previous_entry:
            self.queue_bad_browser_proxy_entry(previous_entry)

    def queue_google_antibot_retry(self, record: Dict[str, str]) -> None:
        pid = clean_text(record.get("PID"))
        if not pid:
            return
        with self.google_retry_lock:
            self.pending_google_antibot_retries[pid] = dict(record)

    def pop_google_antibot_retries(self) -> Dict[str, Dict[str, str]]:
        with self.google_retry_lock:
            retries = dict(self.pending_google_antibot_retries)
            self.pending_google_antibot_retries.clear()
            return retries

    def get_browser_profile(self) -> Dict[str, str]:
        profile = getattr(self.driver_local, "browser_profile", None)
        if profile:
            return profile
        profile = build_random_browser_profile()
        self.driver_local.browser_profile = profile
        return profile

    def get_rotating_browser_proxy(self) -> Optional[Dict[str, object]]:
        if not ROTATING_BROWSER_PROXY_ENABLED:
            return None
        with ROTATING_BROWSER_PROXY_LOCK:
            proxy_entry = self.pop_next_browser_proxy_entry()
        if not proxy_entry:
            return None
        parts = proxy_entry.split(":")
        if len(parts) != 4:
            LOGGER.warning("Skipping invalid rotating browser proxy entry=%s", proxy_entry)
            return None
        host, port, username, password = parts
        previous_entry = clean_text(getattr(self.driver_local, "current_browser_proxy_entry", ""))
        if previous_entry:
            self.driver_local.last_browser_proxy_entry = previous_entry
        self.driver_local.current_browser_proxy_entry = proxy_entry
        return {
            "scheme": clean_text(ROTATING_BROWSER_PROXY_SCHEME) or "http",
            "host": clean_text(host),
            "port": int(clean_text(port) or "0"),
            "username": clean_text(username),
            "password": clean_text(password),
        }

    def create_proxy_auth_extension_dir(self, proxy_config: Dict[str, object], purpose: str) -> str:
        extension_dir = tempfile.mkdtemp(prefix=f"{purpose}_proxy_", dir=os.path.join(BASE_DIR, "chrome_profiles"))
        self.temp_profile_dirs.append(extension_dir)
        manifest = {
            "version": "1.0.0",
            "manifest_version": 3,
            "name": f"{purpose.title()} Proxy Auth",
            "permissions": [
                "proxy",
                "storage",
                "webRequest",
                "webRequestAuthProvider",
            ],
            "host_permissions": ["<all_urls>"],
            "background": {"service_worker": "background.js"},
        }
        background_js = f"""
const config = {{
  mode: "fixed_servers",
  rules: {{
    singleProxy: {{
      scheme: "{proxy_config['scheme']}",
      host: "{proxy_config['host']}",
      port: {int(proxy_config['port'])}
    }},
    bypassList: ["localhost", "127.0.0.1"]
  }}
}};

chrome.runtime.onInstalled.addListener(() => {{
  chrome.proxy.settings.set({{ value: config, scope: "regular" }});
}});

chrome.runtime.onStartup.addListener(() => {{
  chrome.proxy.settings.set({{ value: config, scope: "regular" }});
}});

chrome.webRequest.onAuthRequired.addListener(
  () => {{
    return {{
      authCredentials: {{
        username: "{proxy_config['username']}",
        password: "{proxy_config['password']}"
      }}
    }};
  }},
  {{ urls: ["<all_urls>"] }},
  ["blocking"]
);
""".strip()
        with open(os.path.join(extension_dir, "manifest.json"), "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2)
        with open(os.path.join(extension_dir, "background.js"), "w", encoding="utf-8") as handle:
            handle.write(background_js)
        return extension_dir

    def create_proxy_bridge(self, proxy_config: Dict[str, object]) -> AuthenticatedProxyBridge:
        bridge = AuthenticatedProxyBridge(
            upstream_host=str(proxy_config["host"]),
            upstream_port=int(proxy_config["port"]),
            username=str(proxy_config["username"]),
            password=str(proxy_config["password"]),
        )
        self.proxy_bridges.append(bridge)
        return bridge

    def apply_browser_proxy_options(self, options: ChromeOptions, purpose: str, state: str = "") -> bool:
        proxy_config = self.get_rotating_browser_proxy()
        if not proxy_config:
            return False
        purpose_key = clean_text(purpose).lower()
        state_key = clean_text(state).upper()
        if purpose_key == "portal":
            return False
        bridge = self.create_proxy_bridge(proxy_config)
        options.add_argument(f"--proxy-server=http://{bridge.host}:{bridge.port}")
        LOGGER.info(
            "Applied rotating browser proxy for purpose=%s state=%s upstream=%s:%s local=%s:%s",
            purpose,
            state,
            proxy_config["host"],
            proxy_config["port"],
            bridge.host,
            bridge.port,
        )
        return True

    def log_browser_ip(self, driver: webdriver.Chrome, purpose: str, state: str = "") -> None:
        try:
            driver.get("https://api.ipify.org?format=json")
            WebDriverWait(driver, 10).until(
                lambda d: "ip" in clean_text(d.find_element(By.TAG_NAME, "body").text)
            )
            body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text)
            match = re.search(r'"ip"\s*:\s*"([^"]+)"', body_text)
            ip_value = clean_text(match.group(1)) if match else body_text
            LOGGER.info("Browser window IP purpose=%s state=%s ip=%s", purpose, state, ip_value)
            driver.get("about:blank")
        except Exception:
            LOGGER.exception("Failed to log browser IP for purpose=%s state=%s", purpose, state)

    def load_checkpoint(self) -> Dict[str, Dict[str, str]]:
        checkpoint_path = self.checkpoint_file
        if not os.path.exists(checkpoint_path):
            if checkpoint_path == CHECKPOINT_FILE:
                for legacy_path in LEGACY_CHECKPOINT_FILES:
                    if os.path.exists(legacy_path):
                        checkpoint_path = legacy_path
                        LOGGER.info("Primary checkpoint missing; loading legacy checkpoint from %s", legacy_path)
                        break
                else:
                    LOGGER.info("No checkpoint file found at startup")
                    return {}
            else:
                LOGGER.info("No checkpoint file found at startup")
                return {}
        LOGGER.debug("Loading checkpoint from %s", checkpoint_path)
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
                if (
                    isinstance(data, dict)
                    and data.get("_meta", {}).get("schema_version") == CHECKPOINT_SCHEMA_VERSION
                    and isinstance(data.get("rows"), dict)
                ):
                    rows = {
                        pid: payload
                        for pid, payload in data["rows"].items()
                        if isinstance(payload, dict)
                        and isinstance(payload.get("row"), dict)
                        and has_fetched_enrichment(payload.get("row", {}), payload.get("sources", {}))
                    }
                    LOGGER.info("Loaded checkpoint with %s rows", len(rows))
                    return rows
                if isinstance(data, dict) and all(isinstance(v, dict) for v in data.values()):
                    LOGGER.warning(
                        "Ignoring legacy checkpoint format so trusted-source run starts clean; delete %s if you want to remove it",
                        checkpoint_path,
                    )
                    return {}
                LOGGER.warning("Checkpoint file did not contain a dictionary; ignoring it")
                return {}
        except Exception:
            LOGGER.exception("Failed to load checkpoint; starting with empty state")
            return {}

    def load_staging(self) -> Dict[str, Dict[str, str]]:
        LOGGER.info("Staging cache is unified with checkpoint file at %s", self.checkpoint_file)
        return self.load_checkpoint()

    def save_checkpoint(self) -> None:
        with self.checkpoint_lock:
            snapshot = {
                pid: dict(payload)
                for pid, payload in self.checkpoint.items()
                if isinstance(payload, dict)
                and isinstance(payload.get("row"), dict)
                and has_fetched_enrichment(payload.get("row", {}), payload.get("sources", {}))
            }
            checkpoint_path = self.checkpoint_file
            temp_path = f"{checkpoint_path}.tmp"
            LOGGER.debug("Saving checkpoint with %s rows to %s", len(snapshot), temp_path)
            with open(temp_path, "w", encoding="utf-8") as handle:
                json.dump(
                    {"_meta": {"schema_version": CHECKPOINT_SCHEMA_VERSION}, "rows": snapshot},
                    handle,
                    ensure_ascii=False,
                    indent=2,
                )
            os.replace(temp_path, checkpoint_path)
            LOGGER.info("Checkpoint saved with %s rows", len(snapshot))

    def save_staging(self) -> None:
        self.save_checkpoint()
        LOGGER.info("Unified cache saved to %s with %s rows", self.checkpoint_file, len(self.checkpoint))

    def get_checkpoint_row(self, pid: str) -> Optional[Dict[str, str]]:
        with self.checkpoint_lock:
            value = self.checkpoint.get(pid)
            if value:
                LOGGER.debug("Checkpoint hit for PID=%s", pid)
            return dict(value) if value else None

    def get_staging_row(self, pid: str) -> Optional[Dict[str, str]]:
        return self.get_checkpoint_row(pid)

    def queue_api_city_only_retry(self, state: str, record: Dict[str, str]) -> None:
        pid = clean_text(record.get("PID"))
        state = clean_text(state).upper()
        city = clean_text(record.get("Mailing_City")).upper()
        if not pid or not state or not city:
            return
        with self.api_retry_lock:
            self.pending_api_city_only_retries.setdefault(state, {}).setdefault(city, {})[pid] = dict(record)

    def pop_api_city_only_retries(self, state: str) -> Dict[str, Dict[str, Dict[str, str]]]:
        state = clean_text(state).upper()
        with self.api_retry_lock:
            return dict(self.pending_api_city_only_retries.pop(state, {}))

    def queue_adapter_timeout_retry(self, record: Dict[str, str]) -> None:
        pid = clean_text(record.get("PID"))
        state = clean_text(record.get("Mailing_State")).upper()
        if not pid or not state:
            return
        with self.adapter_timeout_retry_lock:
            self.pending_adapter_timeout_retries.setdefault(state, {})[pid] = dict(record)

    def pop_adapter_timeout_retries(self, state: str) -> Dict[str, Dict[str, str]]:
        state = clean_text(state).upper()
        with self.adapter_timeout_retry_lock:
            return dict(self.pending_adapter_timeout_retries.pop(state, {}))

    def queue_winnie_retry(self, record: Dict[str, str]) -> None:
        pid = clean_text(record.get("PID"))
        state = clean_text(record.get("Mailing_State")).upper()
        city = clean_text(record.get("Mailing_City")).upper()
        if not pid or not state or not city:
            return
        with self.winnie_retry_lock:
            self.pending_winnie_retries.setdefault(state, {}).setdefault(city, {})[pid] = dict(record)

    def pop_winnie_retries(self, state: str) -> Dict[str, Dict[str, Dict[str, str]]]:
        state = clean_text(state).upper()
        with self.winnie_retry_lock:
            return dict(self.pending_winnie_retries.pop(state, {}))

    def get_next_winnie_backoff_seconds(self, state: str) -> float:
        state = clean_text(state).upper() or "DEFAULT"
        with self.winnie_backoff_lock:
            current = float(self.winnie_backoff_seconds.get(state, 60.0))
            self.winnie_backoff_seconds[state] = min(current * 2.0, 900.0)
            return current

    def reset_winnie_backoff_seconds(self, state: str) -> None:
        state = clean_text(state).upper()
        if not state:
            return
        with self.winnie_backoff_lock:
            self.winnie_backoff_seconds.pop(state, None)

    def get_next_google_backoff_seconds(self, state: str) -> float:
        state = clean_text(state).upper() or "DEFAULT"
        with self.google_backoff_lock:
            current = float(self.google_backoff_seconds.get(state, 60.0))
            self.google_backoff_seconds[state] = min(current * 2.0, 900.0)
            return current

    def reset_google_backoff_seconds(self, state: str) -> None:
        state = clean_text(state).upper()
        if not state:
            return
        with self.google_backoff_lock:
            self.google_backoff_seconds.pop(state, None)

    def set_checkpoint_row(self, pid: str, row: Dict[str, str], sources: Optional[Dict[str, Dict[str, str]]] = None) -> None:
        with self.checkpoint_lock:
            if not has_fetched_enrichment(row, sources):
                if pid in self.checkpoint:
                    LOGGER.debug("Checkpoint row preserved for PID=%s because an existing fetched row is already cached", pid)
                else:
                    LOGGER.debug("Checkpoint row skipped for PID=%s because no fetched data was found", pid)
                return
            self.checkpoint[pid] = {
                "row": dict(row),
                "sources": {key: dict(value) for key, value in (sources or {}).items()},
            }
            LOGGER.debug("Checkpoint updated for PID=%s", pid)

    def set_staging_row(self, pid: str, row: Dict[str, str], sources: Optional[Dict[str, Dict[str, str]]] = None) -> None:
        return

    def checkpoint_size(self) -> int:
        with self.checkpoint_lock:
            return len(self.checkpoint)

    def extract_checkpoint_payload(
        self, payload: Optional[Dict[str, Dict[str, str]]]
    ) -> Tuple[Optional[Dict[str, str]], Dict[str, Dict[str, str]]]:
        if not payload:
            return None, {}
        if "row" in payload and isinstance(payload.get("row"), dict):
            row = dict(payload["row"])
            sources = payload.get("sources", {})
            if not isinstance(sources, dict):
                sources = {}
            return row, {key: dict(value) for key, value in sources.items() if isinstance(value, dict)}
        if all(isinstance(value, str) for value in payload.values()):
            return dict(payload), {}
        return None, {}

    def build_search_chrome_options(self, profile_dir: str) -> ChromeOptions:
        browser_profile = self.get_browser_profile()
        options = ChromeOptions()
        options.binary_location = CHROME_BINARY_PATH
        proxy_applied = self.apply_browser_proxy_options(options, "google")
        effective_headless = GOOGLE_USE_HEADLESS and not clean_text(SINGLE_PID_FILTER) and not FORCE_HEADED
        if effective_headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1366,1200")
        options.add_argument("--lang=en-US")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--remote-debugging-pipe")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        if not proxy_applied:
            options.add_argument("--disable-extensions")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-features=Translate,AcceptCHFrame,MediaRouter,OptimizationHints")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--metrics-recording-only")
        options.add_argument("--password-store=basic")
        options.add_argument("--use-mock-keychain")
        options.add_argument("--start-maximized")
        options.add_argument(f"--user-agent={browser_profile['user_agent']}")
        options.add_argument(f"--user-data-dir={profile_dir}")
        options.add_argument(f"--accept-lang={browser_profile['accept_language']}")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.page_load_strategy = "eager"
        return options

    def get_search_driver(self) -> webdriver.Chrome:
        driver = getattr(self.driver_local, "driver", None)
        if driver:
            return driver

        LOGGER.info("Starting Chrome for search discovery")
        effective_headless = GOOGLE_USE_HEADLESS and not clean_text(SINGLE_PID_FILTER) and not FORCE_HEADED
        profile_attempts: List[Tuple[str, bool]] = []
        if GOOGLE_USE_PERSISTENT_PROFILE:
            profile_attempts.append((CHROME_PROFILE_DIR, True))
        for _ in range(3):
            temp_profile_dir = tempfile.mkdtemp(prefix="google_search_", dir=os.path.join(BASE_DIR, "chrome_profiles"))
            self.temp_profile_dirs.append(temp_profile_dir)
            profile_attempts.append((temp_profile_dir, False))

        last_error: Optional[Exception] = None
        for profile_dir, is_persistent in profile_attempts:
            options = self.build_search_chrome_options(profile_dir)

            LOGGER.info(
                "Chrome search driver config persistent_profile=%s headless=%s profile_dir=%s",
                is_persistent,
                effective_headless,
                profile_dir,
            )
            try:
                driver = webdriver.Chrome(options=options)
                driver.set_page_load_timeout(min(SELENIUM_PAGELOAD_TIMEOUT, GOOGLE_SEARCH_TOTAL_TIMEOUT_SECONDS))
                try:
                    browser_profile = self.get_browser_profile()
                    driver.execute_cdp_cmd("Network.enable", {})
                    driver.execute_cdp_cmd(
                        "Network.setExtraHTTPHeaders",
                        {
                            "headers": {
                                "Accept-Language": browser_profile["accept_language"],
                                "DNT": random.choice(["1", "0"]),
                                "Upgrade-Insecure-Requests": "1",
                            }
                        },
                    )
                    driver.execute_cdp_cmd(
                        "Page.addScriptToEvaluateOnNewDocument",
                        {
                            "source": """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
                            """
                        },
                    )
                except Exception:
                    LOGGER.debug("Failed to apply anti-detection Chrome script", exc_info=True)
                self.log_browser_ip(driver, "google")
                try:
                    if FORCE_HEADED:
                        driver.get(GOOGLE_HOME_URL)
                        consent_handled = GoogleSearchAdapter().handle_google_consent(driver)
                        if consent_handled:
                            LOGGER.info("Google consent popup detected and accepted on startup")
                except Exception:
                    LOGGER.exception("Failed during headed Google startup pause")
                self.driver_local.driver = driver
                with self.driver_lock:
                    self.driver_registry.append(driver)
                return driver
            except SessionNotCreatedException as exc:
                last_error = exc
                LOGGER.warning(
                    "Chrome session creation failed for profile_dir=%s persistent=%s; will %s",
                    profile_dir,
                    is_persistent,
                    "retry with another fresh profile" if is_persistent or profile_dir != profile_attempts[-1][0] else "raise the error",
                )
            except WebDriverException as exc:
                last_error = exc
                LOGGER.warning(
                    "Chrome WebDriver startup failed for profile_dir=%s persistent=%s; will %s",
                    profile_dir,
                    is_persistent,
                    "retry with another fresh profile" if is_persistent or profile_dir != profile_attempts[-1][0] else "raise the error",
                )
        if last_error:
            raise last_error
        raise RuntimeError("Failed to start Chrome search driver")

    def close(self) -> None:
        with self.driver_lock:
            state_drivers = list(self.state_portal_drivers.values())
            self.state_portal_drivers.clear()
            self.state_portal_base_handles.clear()
            self.state_portal_query_handles.clear()
            self.state_portal_session_flags.clear()
        with self.driver_lock:
            drivers = list(self.driver_registry)
            self.driver_registry.clear()
        for driver in state_drivers + drivers:
            try:
                service = getattr(driver, "service", None)
                if service:
                    try:
                        service.stop()
                    except Exception:
                        pass
                driver.quit()
            except Exception:
                LOGGER.exception("Failed while closing Selenium driver")
        if hasattr(self.driver_local, "driver"):
            self.driver_local.driver = None
        for profile_dir in list(self.temp_profile_dirs):
            try:
                shutil.rmtree(profile_dir, ignore_errors=True)
            except Exception:
                LOGGER.exception("Failed to clean temporary Chrome profile at %s", profile_dir)
        self.temp_profile_dirs.clear()
        for bridge in list(self.proxy_bridges):
            try:
                bridge.close()
            except Exception:
                LOGGER.exception("Failed to close proxy bridge")
        self.proxy_bridges.clear()

    def reset_search_driver(self) -> None:
        driver = getattr(self.driver_local, "driver", None)
        if not driver:
            return
        try:
            with self.driver_lock:
                if driver in self.driver_registry:
                    self.driver_registry.remove(driver)
            try:
                service = getattr(driver, "service", None)
                if service:
                    try:
                        service.stop()
                    except Exception:
                        pass
                driver.quit()
            except Exception:
                LOGGER.exception("Failed while resetting Google search driver")
        finally:
            self.driver_local.driver = None
            for bridge in list(self.proxy_bridges):
                try:
                    bridge.close()
                except Exception:
                    LOGGER.exception("Failed to close proxy bridge during search driver reset")
            self.proxy_bridges.clear()

    def enrich_from_state_portal(self, record: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        state = clean_text(record.get("Mailing_State"))
        if not state:
            return {}, {}
        with self.driver_lock:
            state_lock = self.state_portal_run_locks.setdefault(state, threading.Lock())
        with state_lock:
            if state in ADAPTER_REGISTRY:
                return ADAPTER_REGISTRY[state].run(self, record)
            if state in API_REGISTRY:
                return API_REGISTRY[state].run(self, record)
            self.queue_winnie_retry(record)
            if state in STATE_PORTAL_URLS:
                LOGGER.warning(
                    "State portal adapter not yet fully implemented or reachable for state=%s portal=%s",
                    state,
                    STATE_PORTAL_URLS[state],
                )
            else:
                LOGGER.info("No official state portal adapter registered for state=%s", state)
            return {}, {}

    def get_state_scraper_model(self, state: str) -> Dict[str, object]:
        return self.state_scraper_models.get(clean_text(state), {})

    def resolve_model_filter_fields(self, model: Dict[str, object], kind: str) -> List[str]:
        raw_filters = [clean_text(str(value)).replace("`", "") for value in (model.get("filters") or [])]
        if kind == "provider":
            raw_value = raw_filters[0] if raw_filters else ""
            if raw_value and raw_value.lower() not in {"provider name", "provider", "name"}:
                return [raw_value]
            return GENERIC_PROVIDER_NAME_FIELDS
        if kind == "city":
            raw_value = raw_filters[1] if len(raw_filters) > 1 else ""
            if raw_value and raw_value.lower() not in {"city", "mailing city"}:
                return [raw_value]
            return GENERIC_CITY_FIELDS
        return []

    def build_open_data_query(self, state: str, model: Dict[str, object], name_variant: str, city: str, city_only: bool = False) -> Tuple[str, Dict[str, str]]:
        query_template = clean_text(str(model.get("query_template", "")))
        api_type = clean_text(str(model.get("api_type", "")))
        if not query_template:
            return "", {}
        if city_only:
            if api_type == "socrata_soql":
                query_template = re.sub(
                    r"WHERE\s+caseless_contains\([^)]+\)\s+AND\s+",
                    "WHERE ",
                    query_template,
                    flags=re.IGNORECASE,
                )
                query_template = re.sub(
                    r"\s+AND\s+caseless_contains\([^)]+\)",
                    "",
                    query_template,
                    flags=re.IGNORECASE,
                )
            elif api_type == "ckan_sql":
                query_template = re.sub(
                    r"WHERE\s+[^W]*?\{name_variant\}[^A]*?\s+AND\s+",
                    "WHERE ",
                    query_template,
                    flags=re.IGNORECASE,
                )
                query_template = re.sub(
                    r"\s+AND\s+[^A]*?\{name_variant\}[^L]*(?=(?:\s+LIMIT|\s*$))",
                    "",
                    query_template,
                    flags=re.IGNORECASE,
                )
        if api_type == "socrata_soql":
            query = (
                query_template.replace("{name_variant}", name_variant.replace("'", "''").replace('"', '""'))
                .replace("{city}", city.replace("'", "''").replace('"', '""'))
            )
            return "soql", {"$query": query}
        if api_type == "ckan_sql":
            sql = (
                query_template.replace("{name_variant}", name_variant.replace("'", "''"))
                .replace("{city}", city.replace("'", "''"))
            )
            return "ckan_sql", {"sql": sql}
        if api_type == "socrata_json":
            return "json_list", {}
        return "", {}

    def fetch_generic_open_data_rows(
        self,
        state: str,
        model: Dict[str, object],
        request_kind: str,
        params: Dict[str, str],
        action_label: str,
    ) -> List[Dict[str, object]]:
        endpoint = clean_text(str(model.get("endpoint", "")))
        if not endpoint:
            return []
        response = self.request_with_retries(
            url=endpoint,
            retries=2,
            method="GET",
            action_label=action_label,
            params=params or None,
            headers={"Accept": "application/json"},
        )
        data = response.json()
        if request_kind == "ckan_sql":
            result = data.get("result", {}) if isinstance(data, dict) else {}
            records = result.get("records", []) if isinstance(result, dict) else []
            return records if isinstance(records, list) else []
        if isinstance(data, list):
            rows = flatten_dict_rows(data)
            if rows:
                return rows
        LOGGER.warning("Generic open data response for state=%s was not a list/dataset payload", state)
        return []

    def filter_json_list_candidates(
        self,
        record: Dict[str, str],
        rows: List[Dict[str, object]],
        provider_fields: List[str],
        city_fields: List[str],
        city_only: bool = False,
    ) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City")).lower()
        variants = [simplify_name(variant) for variant in profile.search_name_variants if simplify_name(variant)]
        filtered: List[Dict[str, object]] = []
        for item in rows:
            provider_name = first_non_empty(item, provider_fields)
            provider_simple = simplify_name(provider_name)
            item_city = ""
            for key in city_fields:
                item_city = clean_text(str(item.get(key, "")))
                if item_city:
                    break
            item_city_lower = item_city.lower()
            if city and item_city_lower and city != item_city_lower:
                continue
            if city_only or any(provider_simple and variant and (variant in provider_simple or provider_simple in variant) for variant in variants):
                filtered.append(item)
        return filtered

    def search_generic_open_data_api(self, record: Dict[str, str], city_only: bool = False) -> List[Dict[str, object]]:
        state = clean_text(record.get("Mailing_State"))
        model = self.get_state_scraper_model(state)
        if not model:
            LOGGER.warning("No state scraper model found for state=%s", state)
            return []
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        provider_fields = self.resolve_model_filter_fields(model, "provider")
        city_fields = self.resolve_model_filter_fields(model, "city")
        candidates: List[Dict[str, object]] = []
        seen = set()
        api_type = clean_text(str(model.get("api_type", "")))
        if api_type == "socrata_json":
            request_kind, params = self.build_open_data_query(state, model, profile.search_name_primary, city, city_only=city_only)
            if not request_kind:
                return []
            rows = self.fetch_generic_open_data_rows(
                state=state,
                model=model,
                request_kind=request_kind,
                params=params,
                action_label=f"{state.lower()} generic api search [{record.get('PID', '')}::json-list]",
            )
            rows = self.filter_json_list_candidates(
                record=record,
                rows=rows,
                provider_fields=provider_fields,
                city_fields=city_fields,
                city_only=city_only,
            )
            LOGGER.info("Generic API search for state=%s PID=%s produced %s candidates", state, record.get("PID", ""), len(rows))
            return rows
        variants = [""] if city_only else list(profile.search_name_variants)
        for variant in variants:
            request_kind, params = self.build_open_data_query(state, model, variant, city, city_only=city_only)
            if not request_kind:
                continue
            LOGGER.info(
                "Generic API search for state=%s PID=%s variant=%s city=%s request_kind=%s",
                state,
                record.get("PID", ""),
                variant,
                city,
                request_kind,
            )
            rows = self.fetch_generic_open_data_rows(
                state=state,
                model=model,
                request_kind=request_kind,
                params=params,
                action_label=f"{state.lower()} generic api search [{record.get('PID', '')}::{variant}]",
            )
            if api_type == "socrata_json":
                rows = self.filter_json_list_candidates(
                    record=record,
                    rows=rows,
                    provider_fields=provider_fields,
                    city_fields=city_fields,
                )
            for item in rows:
                candidate_key = json.dumps(item, sort_keys=True, default=str)
                if candidate_key in seen:
                    continue
                seen.add(candidate_key)
                candidates.append(item)
        LOGGER.info("Generic API search for state=%s PID=%s produced %s candidates", state, record.get("PID", ""), len(candidates))
        return candidates

    def score_generic_open_data_candidate(self, record: Dict[str, str], candidate: Dict[str, object], model: Dict[str, object]) -> int:
        provider_name = first_non_empty(candidate, self.resolve_model_filter_fields(model, "provider"))
        provider_name = provider_name or first_non_empty(candidate, GENERIC_PROVIDER_NAME_FIELDS)
        city = first_non_empty(candidate, self.resolve_model_filter_fields(model, "city"))
        city = city or first_non_empty(candidate, GENERIC_CITY_FIELDS)
        address_parts = [
            first_non_empty(candidate, GENERIC_ADDRESS_FIELDS),
            city,
            first_non_empty(candidate, GENERIC_STATE_FIELDS),
            first_non_empty(candidate, GENERIC_ZIP_FIELDS),
        ]
        address = ", ".join([part for part in address_parts if clean_text(part)])
        score = token_overlap_score(record.get("Daycare_Name", ""), provider_name) * 4
        if clean_text(record.get("Mailing_City")).lower() == city.lower():
            score += 3
        elif clean_text(record.get("Mailing_City")).lower() in address.lower():
            score += 2
        if clean_text(record.get("Mailing_State")).lower() in address.lower():
            score += 1
        if provider_name and simplify_name(record.get("Daycare_Name", "")) in simplify_name(provider_name):
            score += 2
        return score

    def build_generic_open_data_values(
        self, state: str, candidate: Dict[str, object], model: Dict[str, object]
    ) -> Dict[str, str]:
        source_fields = model.get("source_fields", {}) if isinstance(model.get("source_fields"), dict) else {}
        address_fields = source_fields.get("Mailing_Address", []) or [*GENERIC_ADDRESS_FIELDS, *GENERIC_CITY_FIELDS, *GENERIC_STATE_FIELDS, *GENERIC_ZIP_FIELDS]
        zip_fields = source_fields.get("Mailing_Zip", []) or GENERIC_ZIP_FIELDS
        phone_fields = source_fields.get("Telephone", []) or GENERIC_PHONE_FIELDS
        url_fields = source_fields.get("URL", []) or GENERIC_URL_FIELDS
        capacity_fields = source_fields.get("Capacity (optional)", []) or GENERIC_CAPACITY_FIELDS
        age_fields = source_fields.get("Age Range (optional)", []) or GENERIC_AGE_FIELDS
        address_value = ", ".join(
            [
                clean_text(str(candidate.get(field, "")))
                for field in address_fields
                if clean_text(str(candidate.get(field, "")))
            ]
        )
        if state == "CO" and "," in address_value:
            address_value = clean_text(address_value.split(",", 1)[0])
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(first_non_empty(candidate, zip_fields)),
            "Telephone": normalize_phone(first_non_empty(candidate, phone_fields)),
            "URL": normalize_url(first_non_empty(candidate, url_fields)),
            "Capacity (optional)": first_non_empty(candidate, capacity_fields),
            "Age Range (optional)": "",
        }
        if state == "WA":
            values["Age Range (optional)"] = format_numeric_age_range(
                first_non_empty(candidate, ["startingage"]),
                first_non_empty(candidate, ["endingage"]),
                unit="years",
            )
        elif state == "UT":
            values["Age Range (optional)"] = normalize_age_groups_text_to_numeric_range(first_non_empty(candidate, age_fields))
        elif state == "DE":
            values["Age Range (optional)"] = normalize_age_groups_text_to_numeric_range(first_non_empty(candidate, age_fields))
        elif state == "PA":
            values["Age Range (optional)"] = normalize_age_groups_text_to_numeric_range(first_non_empty(candidate, age_fields))
        return values

    def enrich_from_generic_open_data_api(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        state = clean_text(record.get("Mailing_State"))
        model = self.get_state_scraper_model(state)
        if not model:
            return {}, {}
        candidates = self.search_generic_open_data_api(record)
        if not candidates:
            return {}, {}
        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = self.score_generic_open_data_candidate(record, candidate, model)
            LOGGER.info(
                "Generic API candidate scored %s for state=%s PID=%s candidate=%s",
                score,
                state,
                record.get("PID", ""),
                candidate,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
        if not best_candidate or best_score < 6:
            LOGGER.warning("Generic API adapter rejected state=%s PID=%s best_score=%s", state, record.get("PID", ""), best_score)
            return {}, {}
        values = self.build_generic_open_data_values(state, best_candidate, model)
        matched_provider_name = first_non_empty(best_candidate, self.resolve_model_filter_fields(model, "provider")) or first_non_empty(
            best_candidate, GENERIC_PROVIDER_NAME_FIELDS
        )
        candidate_city = first_non_empty(best_candidate, self.resolve_model_filter_fields(model, "city")) or first_non_empty(
            best_candidate, GENERIC_CITY_FIELDS
        )
        source_url = clean_text(str(model.get("endpoint", "")))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=candidate_city,
            candidate_address=values.get("Mailing_Address", ""),
            candidate_phone=values.get("Telephone", ""),
            candidate_url=values.get("URL", ""),
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        sources: Dict[str, Dict[str, str]] = {}
        for field, value in values.items():
            if not clean_text(value):
                continue
            notes = f"{state} official open-data API"
            if field.startswith("Match_") or field == "Matched_Provider_Name":
                notes = f"{state} official open-data API; accepted candidate metadata"
            sources[field] = build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes=notes,
            )
        return values, sources

    def fetch_texas_public_dataset(self, query: str, action_label: str) -> List[Dict[str, object]]:
        response = self.request_with_retries(
            url=TEXAS_PROVIDER_SEARCH_API_URL,
            retries=2,
            method="GET",
            action_label=action_label,
            params={"$query": query},
            headers={"Accept": "application/json"},
        )
        data = response.json()
        return data if isinstance(data, list) else []

    def search_texas_portal_api(self, record: Dict[str, str], city_only: bool = False) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        provider_variants = [""] if city_only else profile.search_name_variants
        seen_provider_ids = set()
        candidates: List[Dict[str, object]] = []

        for variant in provider_variants:
            escaped_city = city.replace('"', '""')
            if city_only:
                query = (
                    "SELECT operation_id, operation_type, operation_number, operation_name, "
                    "programs_provided, location_address, mailing_address, phone_number, county, "
                    "website_address, administrator_director_name, type_of_issuance, issuance_date, "
                    "conditions_on_permit, accepts_child_care_subsidies, hours_of_operation, "
                    "days_of_operation, other_schedule_information, total_capacity, "
                    "licensed_to_serve_ages, corrective_action, adverse_action, temporarily_closed, "
                    "email_address, care_type, operation_status, address_line, city, state, zipcode "
                    f'WHERE caseless_one_of(city, "{escaped_city}")'
                )
            else:
                escaped_variant = variant.replace('"', '""')
                query = (
                    "SELECT operation_id, operation_type, operation_number, operation_name, "
                    "programs_provided, location_address, mailing_address, phone_number, county, "
                    "website_address, administrator_director_name, type_of_issuance, issuance_date, "
                    "conditions_on_permit, accepts_child_care_subsidies, hours_of_operation, "
                    "days_of_operation, other_schedule_information, total_capacity, "
                    "licensed_to_serve_ages, corrective_action, adverse_action, temporarily_closed, "
                    "email_address, care_type, operation_status, address_line, city, state, zipcode "
                    f'WHERE caseless_contains(operation_name, "{escaped_variant}") '
                    f'AND caseless_one_of(city, "{escaped_city}")'
                )
            LOGGER.info(
                "Texas public dataset search for PID=%s provider_variant=%s city=%s",
                record.get("PID", ""),
                variant,
                city,
            )
            rows = self.fetch_texas_public_dataset(
                query=query,
                action_label=f"texas dataset search [{record.get('PID', '')}::{variant}]",
            )
            LOGGER.info(
                "Texas public dataset returned %s candidates for PID=%s provider_variant=%s",
                len(rows),
                record.get("PID", ""),
                variant,
            )
            for item in rows:
                if not isinstance(item, dict):
                    continue
                provider_id = item.get("operation_id")
                if not provider_id or provider_id in seen_provider_ids:
                    continue
                seen_provider_ids.add(provider_id)
                candidates.append(item)
            if candidates:
                break
        return candidates

    def score_texas_candidate(self, record: Dict[str, str], candidate: Dict[str, object]) -> int:
        provider_name = clean_text(str(candidate.get("operation_name", "")))
        address = clean_text(str(candidate.get("location_address", "") or candidate.get("mailing_address", "")))
        city = clean_text(str(candidate.get("city", "")))
        state = clean_text(str(candidate.get("state", "")))
        score = token_overlap_score(record.get("Daycare_Name", ""), provider_name) * 4
        if clean_text(record.get("Mailing_City")).lower() == city.lower():
            score += 3
        elif clean_text(record.get("Mailing_City")).lower() in address.lower():
            score += 2
        if clean_text(record.get("Mailing_State")).lower() == state.lower():
            score += 2
        if provider_name and simplify_name(record.get("Daycare_Name", "")) in simplify_name(provider_name):
            score += 2
        if candidate.get("operation_type") or candidate.get("care_type"):
            score += 1
        return score

    def enrich_from_texas_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_texas_portal_api(record)
        except Exception:
            LOGGER.exception("Texas portal API search failed for PID=%s", record.get("PID", ""))
            return {}, {}

        if not candidates:
            LOGGER.info("Texas portal API returned no candidates for PID=%s", record.get("PID", ""))
            return {}, {}

        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = self.score_texas_candidate(record, candidate)
            LOGGER.info(
                "Texas candidate scored %s for PID=%s operation_id=%s operation_name=%s",
                score,
                record.get("PID", ""),
                candidate.get("operation_id", ""),
                candidate.get("operation_name", ""),
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate

        if not best_candidate or best_score < 6:
            LOGGER.warning(
                "Texas adapter rejected candidates for PID=%s because best score=%s was below threshold",
                record.get("PID", ""),
                best_score,
            )
            return {}, {}

        source_url = TEXAS_PROVIDER_DETAIL_URL_TEMPLATE.format(provider_id=best_candidate.get("operation_id"))
        website_url = normalize_url(str(best_candidate.get("website_address", "")))
        address_value = clean_text(
            str(best_candidate.get("location_address", "") or best_candidate.get("mailing_address", ""))
        )
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(str(best_candidate.get("zipcode", ""))),
            "Telephone": normalize_phone(str(best_candidate.get("phone_number", ""))),
            "URL": website_url,
            "Capacity (optional)": clean_text(str(best_candidate.get("total_capacity", ""))),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(
                str(best_candidate.get("licensed_to_serve_ages", ""))
            ),
        }
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=clean_text(str(best_candidate.get("operation_name", ""))),
            candidate_city=clean_text(str(best_candidate.get("city", ""))),
            candidate_address=address_value,
            candidate_phone=values["Telephone"],
            candidate_url=website_url,
            closed_hint=" ".join(
                [
                    clean_text(str(best_candidate.get("operation_status", ""))),
                    clean_text(str(best_candidate.get("temporarily_closed", ""))),
                    clean_text(str(best_candidate.get("adverse_action", ""))),
                ]
            ),
            prior_name_hint=False,
        )
        values.update(
            {
                "Matched_Provider_Name": clean_text(str(best_candidate.get("operation_name", ""))),
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        sources: Dict[str, Dict[str, str]] = {}
        for field, value in values.items():
            if not clean_text(value):
                continue
            field_source_url = website_url if field == "URL" and website_url else source_url
            notes = "Texas official childcare public dataset"
            if field == "Age Range (optional)" and clean_text(str(best_candidate.get("licensed_to_serve_ages", ""))):
                notes = "Texas official childcare public dataset; age range normalized from licensed_to_serve_ages"
            if field.startswith("Match_") or field == "Matched_Provider_Name":
                notes = "Texas official childcare public dataset; accepted candidate metadata"
            sources[field] = build_source_entry(
                value=value,
                source_url=field_source_url,
                source_type="official_state_portal",
                notes=notes,
            )
        LOGGER.info(
            "Texas adapter selected operation_id=%s for PID=%s with score=%s",
            best_candidate.get("operation_id", ""),
            record.get("PID", ""),
            best_score,
        )
        return values, sources

    def fetch_california_public_dataset(self, sql: str, action_label: str) -> List[Dict[str, object]]:
        response = self.request_with_retries(
            url=CALIFORNIA_PROVIDER_SEARCH_API_URL,
            retries=2,
            method="GET",
            action_label=action_label,
            params={"sql": sql},
            headers={"Accept": "application/json"},
        )
        data = response.json()
        if not isinstance(data, dict):
            return []
        result = data.get("result", {})
        records = result.get("records", []) if isinstance(result, dict) else []
        return records if isinstance(records, list) else []

    def build_california_sql_query(self, name_variant: str, city_variant: str) -> str:
        escaped_name = name_variant.replace("'", "''").upper()
        escaped_city = city_variant.replace("'", "''").upper()
        return (
            'SELECT * FROM "5bac6551-4d6c-45d6-93b8-e6ded428d98e" '
            f"WHERE UPPER(facility_name) ILIKE '%{escaped_name}%' "
            f"AND UPPER(facility_city) ILIKE '%{escaped_city}%'"
        )

    def search_california_dataset(self, record: Dict[str, str], city_only: bool = False) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        provider_variants = [""] if city_only else profile.search_name_variants
        city_variants = build_city_search_variants(city)
        candidates: List[Dict[str, object]] = []
        seen_ids = set()
        for variant in provider_variants:
            for city_variant in city_variants:
                if city_only:
                    escaped_city_variant = city_variant.replace("'", "''").upper()
                    sql = (
                        'SELECT * FROM "5bac6551-4d6c-45d6-93b8-e6ded428d98e" '
                        f"WHERE UPPER(facility_city) ILIKE '%{escaped_city_variant}%'"
                    )
                else:
                    sql = self.build_california_sql_query(variant, city_variant)
                LOGGER.info(
                    "California public dataset search for PID=%s provider_variant=%s city_variant=%s",
                    record.get("PID", ""),
                    variant,
                    city_variant,
                )
                rows = self.fetch_california_public_dataset(
                    sql=sql,
                    action_label=f"california dataset search [{record.get('PID', '')}::{variant}::{city_variant}]",
                )
                LOGGER.info(
                    "California public dataset returned %s candidates for PID=%s provider_variant=%s city_variant=%s",
                    len(rows),
                    record.get("PID", ""),
                    variant,
                    city_variant,
                )
                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    candidate_id = first_non_empty(item, ["facility_number", "license_number", "_id", "facility_name"])
                    if candidate_id in seen_ids:
                        continue
                    seen_ids.add(candidate_id)
                    candidates.append(item)
                if candidates:
                    break
            if candidates:
                break
        return candidates

    def score_california_candidate(self, record: Dict[str, str], candidate: Dict[str, object]) -> int:
        provider_name = first_non_empty(candidate, ["facility_name", "licensee_name"])
        address = first_non_empty(candidate, ["facility_address", "address", "street_address"])
        city = first_non_empty(candidate, ["facility_city", "city"])
        state = first_non_empty(candidate, ["facility_state", "state"])
        score = token_overlap_score(record.get("Daycare_Name", ""), provider_name) * 4
        if clean_text(record.get("Mailing_City")).lower() == city.lower():
            score += 3
        elif clean_text(record.get("Mailing_City")).lower() in address.lower():
            score += 2
        if not state or clean_text(record.get("Mailing_State")).lower() == state.lower():
            score += 2
        if provider_name and simplify_name(record.get("Daycare_Name", "")) in simplify_name(provider_name):
            score += 2
        return score

    def enrich_from_california_dataset(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_california_dataset(record)
        except Exception:
            LOGGER.exception("California dataset search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            LOGGER.info("California dataset returned no candidates for PID=%s", record.get("PID", ""))
            return {}, {}

        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = self.score_california_candidate(record, candidate)
            LOGGER.info(
                "California candidate scored %s for PID=%s facility=%s",
                score,
                record.get("PID", ""),
                first_non_empty(candidate, ["facility_name", "licensee_name"]),
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
        if not best_candidate or best_score < 6:
            LOGGER.warning(
                "California adapter rejected candidates for PID=%s because best score=%s was below threshold",
                record.get("PID", ""),
                best_score,
            )
            return {}, {}

        website_url = normalize_url(first_non_empty(best_candidate, ["facility_website", "website", "website_address"]))
        address_value = ", ".join(
            part
            for part in [
                first_non_empty(best_candidate, ["facility_address", "address", "street_address"]),
                first_non_empty(best_candidate, ["facility_city", "city"]),
                first_non_empty(best_candidate, ["facility_state", "state"]) or "CA",
                normalize_zip(first_non_empty(best_candidate, ["facility_zip", "zip", "zipcode"])),
            ]
            if clean_text(part)
        )
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(first_non_empty(best_candidate, ["facility_zip", "zip", "zipcode"])),
            "Telephone": normalize_phone(first_non_empty(best_candidate, ["telephone", "phone", "facility_phone"])),
            "URL": website_url,
            "Capacity (optional)": first_non_empty(best_candidate, ["capacity", "licensed_capacity", "facility_capacity"]),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(
                first_non_empty(best_candidate, ["age_range", "ages_served", "facility_ages"])
            ),
        }
        matched_provider_name = first_non_empty(best_candidate, ["facility_name", "licensee_name"])
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=first_non_empty(best_candidate, ["facility_city", "city"]),
            candidate_address=address_value,
            candidate_phone=values["Telephone"],
            candidate_url=website_url,
            closed_hint=" ".join(
                [
                    first_non_empty(best_candidate, ["status", "facility_status", "license_status"]),
                    first_non_empty(best_candidate, ["closed", "temporarily_closed"]),
                ]
            ),
            prior_name_hint=False,
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = website_url or STATE_PORTAL_URLS["CA"]
        sources: Dict[str, Dict[str, str]] = {}
        for field, value in values.items():
            if not clean_text(value):
                continue
            notes = "California official childcare public dataset"
            if field.startswith("Match_") or field == "Matched_Provider_Name":
                notes = "California official childcare public dataset; accepted candidate metadata"
            sources[field] = build_source_entry(
                value=value,
                source_url=website_url if field == "URL" and website_url else source_url,
                source_type="official_state_portal",
                notes=notes,
            )
        return values, sources

    def fetch_connecticut_public_dataset(self, query: str, action_label: str) -> List[Dict[str, object]]:
        response = self.request_with_retries(
            url=CONNECTICUT_PROVIDER_SEARCH_API_URL,
            retries=2,
            method="GET",
            action_label=action_label,
            params={"$query": query},
            headers={"Accept": "application/json"},
        )
        data = response.json()
        return data if isinstance(data, list) else []

    def search_connecticut_dataset(self, record: Dict[str, str], city_only: bool = False) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        provider_variants = [""] if city_only else profile.search_name_variants
        candidates: List[Dict[str, object]] = []
        seen_ids = set()
        for variant in provider_variants:
            escaped_city = city.replace("'", "''")
            if city_only:
                query = (
                    "SELECT name, address2, address3, city, statecode, zipcode, phone, "
                    "minimumage, maximumage, maximumcapacity "
                    f"WHERE caseless_one_of(city, '{escaped_city}')"
                )
            else:
                escaped_variant = variant.replace("'", "''")
                query = (
                    "SELECT name, address2, address3, city, statecode, zipcode, phone, "
                    "minimumage, maximumage, maximumcapacity "
                    f"WHERE caseless_contains(name, '{escaped_variant}') "
                    f"AND caseless_one_of(city, '{escaped_city}')"
                )
            LOGGER.info(
                "Connecticut public dataset search for PID=%s provider_variant=%s city=%s",
                record.get("PID", ""),
                variant,
                city,
            )
            rows = self.fetch_connecticut_public_dataset(
                query=query,
                action_label=f"connecticut dataset search [{record.get('PID', '')}::{variant}]",
            )
            LOGGER.info(
                "Connecticut public dataset returned %s candidates for PID=%s provider_variant=%s",
                len(rows),
                record.get("PID", ""),
                variant,
            )
            for item in rows:
                if not isinstance(item, dict):
                    continue
                candidate_id = "|".join(
                    [
                        first_non_empty(item, ["name"]),
                        first_non_empty(item, ["address2"]),
                        first_non_empty(item, ["zipcode"]),
                    ]
                )
                if candidate_id in seen_ids:
                    continue
                seen_ids.add(candidate_id)
                candidates.append(item)
            if candidates:
                break
        return candidates

    def score_connecticut_candidate(self, record: Dict[str, str], candidate: Dict[str, object]) -> int:
        provider_name = first_non_empty(candidate, ["name"])
        address = " ".join(
            [
                first_non_empty(candidate, ["address2"]),
                first_non_empty(candidate, ["address3"]),
            ]
        )
        city = first_non_empty(candidate, ["city"])
        state = first_non_empty(candidate, ["statecode"])
        score = token_overlap_score(record.get("Daycare_Name", ""), provider_name) * 4
        if clean_text(record.get("Mailing_City")).lower() == city.lower():
            score += 3
        elif clean_text(record.get("Mailing_City")).lower() in address.lower():
            score += 2
        if clean_text(record.get("Mailing_State")).lower() == state.lower():
            score += 2
        if provider_name and simplify_name(record.get("Daycare_Name", "")) in simplify_name(provider_name):
            score += 2
        if first_non_empty(candidate, ["maximumcapacity"]):
            score += 1
        return score

    def enrich_from_connecticut_dataset(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_connecticut_dataset(record)
        except Exception:
            LOGGER.exception("Connecticut dataset search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            LOGGER.info("Connecticut dataset returned no candidates for PID=%s", record.get("PID", ""))
            return {}, {}

        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = self.score_connecticut_candidate(record, candidate)
            LOGGER.info(
                "Connecticut candidate scored %s for PID=%s provider=%s",
                score,
                record.get("PID", ""),
                first_non_empty(candidate, ["name"]),
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
        if not best_candidate or best_score < 6:
            LOGGER.warning(
                "Connecticut adapter rejected candidates for PID=%s because best score=%s was below threshold",
                record.get("PID", ""),
                best_score,
            )
            return {}, {}

        address_value = ", ".join(
            part
            for part in [
                first_non_empty(best_candidate, ["address2"]),
                first_non_empty(best_candidate, ["address3", "city"]),
                first_non_empty(best_candidate, ["statecode"]) or "CT",
                normalize_zip(first_non_empty(best_candidate, ["zipcode"])),
            ]
            if clean_text(part)
        )
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(first_non_empty(best_candidate, ["zipcode"])),
            "Telephone": normalize_phone(first_non_empty(best_candidate, ["phone"])),
            "URL": "",
            "Capacity (optional)": first_non_empty(best_candidate, ["maximumcapacity"]),
            "Age Range (optional)": format_numeric_age_range(
                first_non_empty(best_candidate, ["minimumage"]),
                first_non_empty(best_candidate, ["maximumage"]),
                unit="years",
            ),
        }
        matched_provider_name = first_non_empty(best_candidate, ["name"])
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=first_non_empty(best_candidate, ["city"]),
            candidate_address=address_value,
            candidate_phone=values["Telephone"],
            candidate_url="",
            closed_hint="",
            prior_name_hint=False,
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = f"{CONNECTICUT_PROVIDER_SEARCH_API_URL}?$query="
        sources: Dict[str, Dict[str, str]] = {}
        for field, value in values.items():
            if not clean_text(value):
                continue
            notes = "Connecticut official childcare public dataset"
            if field == "Age Range (optional)":
                notes = "Connecticut official childcare public dataset; age range inferred from minimumage and maximumage as years"
            if field.startswith("Match_") or field == "Matched_Provider_Name":
                notes = "Connecticut official childcare public dataset; accepted candidate metadata"
            sources[field] = build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes=notes,
            )
        return values, sources

    def fetch_pennsylvania_public_dataset(self, query: str, action_label: str) -> List[Dict[str, object]]:
        response = self.request_with_retries(
            url="https://data.pa.gov/resource/ajn5-kaxt.json",
            retries=1,
            method="GET",
            action_label=action_label,
            params={"$query": query},
            headers={"Accept": "application/json"},
        )
        data = response.json()
        return data if isinstance(data, list) else []

    def search_pennsylvania_dataset(self, record: Dict[str, str], city_only: bool = False) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        candidates: List[Dict[str, object]] = []
        seen_ids = set()
        variants = [""] if city_only else profile.search_name_variants[:4]
        for variant in variants:
            escaped_city = city.replace("'", "''")
            if city_only:
                token = ""
                query = (
                    "SELECT facility_name, facility_address, facility_address_continued, "
                    "facility_city, facility_state, facility_zip_code, facility_phone, capacity "
                    f"WHERE caseless_one_of(facility_city, '{escaped_city}')"
                )
            else:
                token = pick_best_name_token(variant)
                if not token:
                    continue
                escaped_token = token.replace("'", "''")
                query = (
                    "SELECT facility_name, facility_address, facility_address_continued, "
                    "facility_city, facility_state, facility_zip_code, facility_phone, capacity "
                    f"WHERE caseless_one_of(facility_city, '{escaped_city}') "
                    f"AND caseless_contains(facility_name, '{escaped_token}')"
                )
            LOGGER.info(
                "Pennsylvania public dataset search for PID=%s provider_variant=%s token=%s city=%s",
                record.get("PID", ""),
                variant,
                token,
                city,
            )
            rows = self.fetch_pennsylvania_public_dataset(
                query=query,
                action_label=f"pennsylvania dataset search [{record.get('PID', '')}::{variant}]",
            )
            LOGGER.info(
                "Pennsylvania public dataset returned %s candidates for PID=%s provider_variant=%s",
                len(rows),
                record.get("PID", ""),
                variant,
            )
            for item in rows:
                if not isinstance(item, dict):
                    continue
                candidate_id = "|".join(
                    [
                        first_non_empty(item, ["facility_name"]),
                        first_non_empty(item, ["facility_address"]),
                        first_non_empty(item, ["facility_zip_code"]),
                    ]
                )
                if candidate_id in seen_ids:
                    continue
                seen_ids.add(candidate_id)
                candidates.append(item)
            if candidates:
                break
        return candidates

    def score_pennsylvania_candidate(self, record: Dict[str, str], candidate: Dict[str, object]) -> int:
        provider_name = first_non_empty(candidate, ["facility_name"])
        address = " ".join(
            [
                first_non_empty(candidate, ["facility_address"]),
                first_non_empty(candidate, ["facility_address_continued"]),
            ]
        )
        city = first_non_empty(candidate, ["facility_city"])
        state = first_non_empty(candidate, ["facility_state"])
        score = token_overlap_score(record.get("Daycare_Name", ""), provider_name) * 4
        if clean_text(record.get("Mailing_City")).lower() == city.lower():
            score += 3
        elif clean_text(record.get("Mailing_City")).lower() in address.lower():
            score += 2
        if clean_text(record.get("Mailing_State")).lower() == state.lower():
            score += 2
        if provider_name and simplify_name(record.get("Daycare_Name", "")) in simplify_name(provider_name):
            score += 2
        if first_non_empty(candidate, ["capacity"]):
            score += 1
        return score

    def enrich_from_pennsylvania_dataset(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_pennsylvania_dataset(record)
        except Exception:
            LOGGER.exception("Pennsylvania dataset search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            LOGGER.info("Pennsylvania dataset returned no candidates for PID=%s", record.get("PID", ""))
            return {}, {}

        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = self.score_pennsylvania_candidate(record, candidate)
            LOGGER.info(
                "Pennsylvania candidate scored %s for PID=%s provider=%s",
                score,
                record.get("PID", ""),
                first_non_empty(candidate, ["facility_name"]),
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
        if not best_candidate or best_score < 6:
            LOGGER.warning(
                "Pennsylvania adapter rejected candidates for PID=%s because best score=%s was below threshold",
                record.get("PID", ""),
                best_score,
            )
            return {}, {}

        address_value = ", ".join(
            part
            for part in [
                first_non_empty(best_candidate, ["facility_address"]),
                first_non_empty(best_candidate, ["facility_address_continued"]),
                first_non_empty(best_candidate, ["facility_city"]),
                first_non_empty(best_candidate, ["facility_state"]) or "PA",
                normalize_zip(first_non_empty(best_candidate, ["facility_zip_code"])),
            ]
            if clean_text(part)
        )
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(first_non_empty(best_candidate, ["facility_zip_code"])),
            "Telephone": normalize_phone(first_non_empty(best_candidate, ["facility_phone"])),
            "URL": "",
            "Capacity (optional)": first_non_empty(best_candidate, ["capacity"]),
            "Age Range (optional)": "",
        }
        matched_provider_name = first_non_empty(best_candidate, ["facility_name"])
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=first_non_empty(best_candidate, ["facility_city"]),
            candidate_address=address_value,
            candidate_phone=values["Telephone"],
            candidate_url="",
            closed_hint="",
            prior_name_hint=False,
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = "https://data.pa.gov/resource/ajn5-kaxt.json?$query="
        sources: Dict[str, Dict[str, str]] = {}
        for field, value in values.items():
            if not clean_text(value):
                continue
            notes = "Pennsylvania official childcare public dataset"
            if field.startswith("Match_") or field == "Matched_Provider_Name":
                notes = "Pennsylvania official childcare public dataset; accepted candidate metadata"
            sources[field] = build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes=notes,
            )
        return values, sources

    def build_headless_portal_driver(self, state: str = "") -> webdriver.Chrome:
        last_error: Optional[Exception] = None
        effective_headless = not clean_text(SINGLE_PID_FILTER) and not FORCE_HEADED
        for attempt in range(1, 4):
            profile_dir = tempfile.mkdtemp(prefix="portal_driver_", dir=os.path.join(BASE_DIR, "chrome_profiles"))
            self.temp_profile_dirs.append(profile_dir)
            options = ChromeOptions()
            options.binary_location = CHROME_BINARY_PATH
            if effective_headless:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--remote-debugging-pipe")
            options.add_argument("--window-size=1440,1400")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-background-networking")
            options.add_argument("--disable-sync")
            options.add_argument("--metrics-recording-only")
            options.add_argument("--mute-audio")
            options.add_argument("--no-first-run")
            options.add_argument("--disable-default-apps")
            if attempt == 1:
                options.add_argument(f"--user-agent={random.choice(USER_AGENT_POOL)}")
                options.add_argument(f"--user-data-dir={profile_dir}")
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option("useAutomationExtension", False)
            elif attempt == 2:
                options.add_argument(f"--user-data-dir={profile_dir}")
            proxy_applied = self.apply_browser_proxy_options(options, "portal", state)
            if not proxy_applied:
                options.add_argument("--disable-extensions")
            try:
                LOGGER.info(
                    "Starting portal Chrome attempt=%s headless=%s profile_dir=%s state=%s",
                    attempt,
                    effective_headless,
                    profile_dir,
                    state,
                )
                driver = webdriver.Chrome(options=options)
                driver.set_page_load_timeout(SELENIUM_PAGELOAD_TIMEOUT)
                self.log_browser_ip(driver, "portal", state)
                with self.driver_lock:
                    self.driver_registry.append(driver)
                return driver
            except (SessionNotCreatedException, WebDriverException) as exc:
                last_error = exc
                LOGGER.warning(
                    "Portal Chrome startup failed on attempt=%s profile_dir=%s error=%s",
                    attempt,
                    profile_dir,
                    exc,
                )
                try:
                    shutil.rmtree(profile_dir, ignore_errors=True)
                except Exception:
                    LOGGER.debug("Failed cleaning portal profile dir after startup error: %s", profile_dir, exc_info=True)
                continue
        if last_error:
            raise last_error
        raise RuntimeError("Failed to start portal Chrome for unknown reason")

    def get_state_portal_driver(self, state: str) -> webdriver.Chrome:
        state = clean_text(state).upper()
        with self.driver_lock:
            existing = self.state_portal_drivers.get(state)
        if existing:
            try:
                _ = existing.title
                return existing
            except Exception:
                LOGGER.warning("Existing %s portal driver became unusable; restarting it", state)
                self.reset_state_portal_driver(state)
        driver = self.build_headless_portal_driver(state)
        with self.driver_lock:
            self.state_portal_drivers[state] = driver
            try:
                self.state_portal_base_handles[state] = driver.current_window_handle
            except Exception:
                self.state_portal_base_handles[state] = ""
        return driver

    def reset_state_portal_driver(self, state: str) -> None:
        state = clean_text(state).upper()
        with self.driver_lock:
            driver = self.state_portal_drivers.pop(state, None)
            self.state_portal_base_handles.pop(state, None)
            self.state_portal_query_handles.pop(state, None)
            self.state_portal_session_flags.pop(state, None)
        if not driver:
            return
        try:
            driver.quit()
        except Exception:
            LOGGER.exception("Failed closing %s portal driver during reset", state)

    def open_state_portal_query_tab(self, state: str, url: str) -> webdriver.Chrome:
        state = clean_text(state).upper()
        for attempt in range(2):
            driver = self.get_state_portal_driver(state)
            try:
                base_handle = self.state_portal_base_handles.get(state, "")
                current_handles = list(driver.window_handles)
                if not base_handle or base_handle not in current_handles:
                    base_handle = driver.current_window_handle
                    with self.driver_lock:
                        self.state_portal_base_handles[state] = base_handle
                driver.switch_to.window(base_handle)
                existing_handles = list(driver.window_handles)
                try:
                    driver.execute_script("window.open(arguments[0], '_blank');", url)
                    WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
                    new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
                    driver.switch_to.window(new_handles[-1] if new_handles else driver.window_handles[-1])
                except Exception:
                    try:
                        driver.get(url)
                    except Exception:
                        if attempt == 0:
                            self.reset_state_portal_driver(state)
                            continue
                        raise
                return driver
            except Exception:
                self.reset_state_portal_driver(state)
                if attempt == 1:
                    raise

    def open_or_reuse_state_portal_query_tab(
        self,
        state: str,
        url: str,
        ready_locator: Optional[Tuple[str, str]] = None,
    ) -> webdriver.Chrome:
        state = clean_text(state).upper()
        for attempt in range(2):
            driver = self.get_state_portal_driver(state)
            try:
                base_handle = self.state_portal_base_handles.get(state, "")
                query_handle = self.state_portal_query_handles.get(state, "")
                handles = list(driver.window_handles)
                if not base_handle or base_handle not in handles:
                    base_handle = driver.current_window_handle
                    with self.driver_lock:
                        self.state_portal_base_handles[state] = base_handle
                if query_handle and query_handle in handles:
                    driver.switch_to.window(query_handle)
                else:
                    driver.switch_to.window(base_handle)
                    existing_handles = list(driver.window_handles)
                    try:
                        driver.execute_script("window.open(arguments[0], '_blank');", url)
                        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
                        new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
                        query_handle = new_handles[-1] if new_handles else driver.window_handles[-1]
                        driver.switch_to.window(query_handle)
                    except Exception:
                        query_handle = base_handle
                        driver.switch_to.window(query_handle)
                        try:
                            driver.get(url)
                        except Exception:
                            if attempt == 0:
                                self.reset_state_portal_driver(state)
                                continue
                            raise
                    with self.driver_lock:
                        self.state_portal_query_handles[state] = query_handle
                if ready_locator:
                    by, value = ready_locator
                    try:
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((by, value)))
                    except Exception:
                        driver.get(url)
                        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 25)).until(
                            EC.presence_of_element_located((by, value))
                        )
                return driver
            except Exception:
                self.reset_state_portal_driver(state)
                if attempt == 1:
                    raise

    def finalize_state_portal_query(self, state: str, home_url: str = "") -> None:
        state = clean_text(state).upper()
        with self.driver_lock:
            driver = self.state_portal_drivers.get(state)
        if not driver:
            return
        try:
            handles = list(driver.window_handles)
            base_handle = self.state_portal_base_handles.get(state, "")
            for handle in handles:
                if handle == base_handle:
                    continue
                try:
                    driver.switch_to.window(handle)
                    driver.close()
                except Exception:
                    continue
            remaining_handles = list(driver.window_handles)
            if base_handle and base_handle in remaining_handles:
                driver.switch_to.window(base_handle)
            elif remaining_handles:
                driver.switch_to.window(remaining_handles[0])
                with self.driver_lock:
                    self.state_portal_base_handles[state] = remaining_handles[0]
        except Exception:
            LOGGER.info("Failed finalizing %s query tab cleanly; resetting shared portal driver", state)
            self.reset_state_portal_driver(state)

    def search(self, query: str) -> List[SearchResult]:
        LOGGER.info("Running search query: %s", query)
        with self.search_lock:
            elapsed = time.time() - self.last_search_time
            if elapsed < SEARCH_MIN_DELAY_SECONDS:
                sleep_seconds = SEARCH_MIN_DELAY_SECONDS - elapsed + random.uniform(0.25, 1.0)
                LOGGER.info("Sleeping %.2f seconds before next search query", sleep_seconds)
                time.sleep(sleep_seconds)
            self.last_search_time = time.time()

        driver = self.get_search_driver()
        search_url = f"{SEARCH_ENGINE_URL}?q={quote_plus(query)}&source=web"
        LOGGER.info("Navigating headless browser to %s", search_url)
        try:
            driver.get(search_url)
            WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            LOGGER.warning("Timed out waiting for Selenium search page body for query: %s", query)
        except WebDriverException:
            LOGGER.exception("Selenium failed while loading search query: %s", query)
            return self.search_via_alternate_providers(query)

        page_source = driver.page_source
        if "Captcha - Brave Search" in driver.title or "captcha" in page_source[:5000].lower():
            LOGGER.warning("Brave search returned captcha for query=%s; falling back to alternate providers", query)
            return self.search_via_alternate_providers(query)

        results = self.parse_brave_results(page_source)
        if not results:
            LOGGER.warning("No Brave search results parsed for query=%s; falling back to alternate providers", query)
            return self.search_via_alternate_providers(query)
        LOGGER.info("Search query returned %s parsed results", len(results))
        return results

    def parse_brave_results(self, page_source: str) -> List[SearchResult]:
        results: List[SearchResult] = []
        pattern = re.compile(
            r'<div class="snippet[^"]*"[^>]*data-type="web"[^>]*>.*?'
            r'<a href="(?P<url>https?://[^"]+)"[^>]*class="[^"]*\bl1\b[^"]*"[^>]*>.*?'
            r'<div class="title[^"]*"[^>]*>(?P<title>.*?)</div>.*?</a>.*?'
            r'<div class="content[^"]*"[^>]*>(?P<snippet>.*?)</div>',
            re.S,
        )
        for rank, match in enumerate(pattern.finditer(page_source), start=1):
            href = normalize_url(match.group("url"))
            title = clean_text(re.sub(r"<.*?>", " ", unescape(match.group("title"))))
            snippet = clean_text(re.sub(r"<.*?>", " ", unescape(match.group("snippet"))))
            if href and title:
                results.append(SearchResult(rank=rank, title=title, url=href, snippet=snippet, provider="brave"))
                LOGGER.debug(
                    "Brave result parsed rank=%s title=%s url=%s snippet=%s",
                    rank,
                    title,
                    href,
                    snippet[:300],
                )
            if len(results) >= SEARCH_RESULTS_LIMIT:
                break
        return results

    def search_via_alternate_providers(self, query: str) -> List[SearchResult]:
        try:
            results = self.search_via_google(query)
        except Exception:
            LOGGER.exception("Google alternate provider failed for query=%s", query)
            return []
        if results:
            LOGGER.info("Google alternate provider returned %s parsed results", len(results))
            return results
        LOGGER.warning("Google alternate provider returned no parsed results for query=%s", query)
        return []

    def remaining_google_budget(self, started_at: float) -> float:
        return max(0.0, GOOGLE_SEARCH_TOTAL_TIMEOUT_SECONDS - (time.monotonic() - started_at))

    def apply_google_page_humanization(self, driver: webdriver.Chrome) -> None:
        try:
            ActionChains(driver).move_by_offset(
                random.randint(5, 25),
                random.randint(5, 25),
            ).pause(random.uniform(0.03, 0.08)).perform()
            driver.execute_script("window.scrollBy(0, arguments[0]);", random.randint(120, 260))
            time.sleep(random.uniform(0.08, 0.18))
            driver.execute_script("window.scrollBy(0, arguments[0]);", random.randint(-40, 80))
        except Exception:
            LOGGER.debug("Failed to apply lightweight Google page humanization", exc_info=True)

    def execute_google_search_interaction(self, driver: webdriver.Chrome, query: str, started_at: float) -> bool:
        if self.remaining_google_budget(started_at) <= 0:
            return False
        try:
            driver.get(GOOGLE_HOME_URL)
            WebDriverWait(driver, min(2.0, max(0.5, self.remaining_google_budget(started_at)))).until(
                EC.presence_of_element_located((By.NAME, "q"))
            )
            search_box = driver.find_element(By.NAME, "q")
            ActionChains(driver).move_to_element(search_box).pause(random.uniform(0.05, 0.15)).click().perform()
            chunks = re.findall(r".{1,6}", query)
            for chunk in chunks:
                if self.remaining_google_budget(started_at) <= 0.4:
                    break
                search_box.send_keys(chunk)
                time.sleep(random.uniform(0.03, 0.09))
            if search_box.get_attribute("value") != query:
                search_box.send_keys(Keys.CONTROL, "a")
                search_box.send_keys(query)
            time.sleep(random.uniform(0.05, 0.15))
            search_box.send_keys(Keys.ENTER)
            self.apply_google_page_humanization(driver)
            WebDriverWait(driver, min(2.0, max(0.5, self.remaining_google_budget(started_at)))).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            return True
        except Exception:
            LOGGER.debug("Google typed interaction failed; falling back to direct URL load for query=%s", query, exc_info=True)
            return False

    def parse_google_results_from_soup(self, soup: BeautifulSoup) -> List[SearchResult]:
        results: List[SearchResult] = []
        seen = set()
        for node in soup.select("a[href]"):
            href = extract_google_target_url(node.get("href", ""))
            title_node = node.find("h3")
            title = clean_text(title_node.get_text(" ", strip=True) if title_node else node.get_text(" ", strip=True))
            snippet = ""
            parent = node.parent
            if parent:
                snippet = clean_text(parent.get_text(" ", strip=True))
            if not is_usable_search_result(href, title, snippet):
                LOGGER.debug("Discarding unusable Google result title=%s url=%s", title, href)
                continue
            if href in seen:
                continue
            if is_blacklisted_official(href) and not is_listing_domain(href):
                continue
            results.append(SearchResult(rank=len(results) + 1, title=title, url=href, snippet=snippet, provider="google"))
            seen.add(href)
            if len(results) >= SEARCH_RESULTS_LIMIT:
                break
        return results

    def extract_google_knowledge_panel_from_soup(self, soup: BeautifulSoup, record: Dict[str, str]) -> Dict[str, str]:
        text = soup.get_text("\n", strip=True)
        website_url = ""
        selectors = [
            'a[data-attrid*="visit_website"]',
            'a[data-attrid*="authority"]',
            'a[href^="http"]',
        ]
        for selector in selectors:
            node = soup.select_one(selector)
            if not node:
                continue
            href = normalize_url(node.get("href", ""))
            if href and not is_internal_search_engine_url(href):
                website_url = href
                break
        panel_name = ""
        for selector in ("div[data-attrid='title'] span", "h2[data-attrid='title']", "div.SPZz6b span"):
            node = soup.select_one(selector)
            if node:
                panel_name = clean_text(node.get_text(" ", strip=True))
                if panel_name:
                    break
        return {
            "Matched_Provider_Name": panel_name,
            "Mailing_Address": self.extract_address_from_text(text, record),
            "Mailing_Zip": self.extract_zip_from_text(text, record),
            "Telephone": self.extract_phone_from_text(text),
            "URL": website_url,
        }

    def fetch_google_search_payload(self, query: str, record: Dict[str, str]) -> Tuple[Dict[str, str], List[SearchResult]]:
        LOGGER.info("Running single-pass Selenium Google search for query: %s", query)
        started_at = time.monotonic()
        with self.search_lock:
            elapsed = time.time() - self.last_search_time
            if elapsed < GOOGLE_SEARCH_MIN_DELAY_SECONDS:
                sleep_seconds = min(
                    GOOGLE_SEARCH_MIN_DELAY_SECONDS - elapsed + random.uniform(0.05, 0.25),
                    max(0.0, GOOGLE_SEARCH_TOTAL_TIMEOUT_SECONDS / 4),
                )
                LOGGER.info("Sleeping %.2f seconds before next Google query", sleep_seconds)
                time.sleep(sleep_seconds)
            self.last_search_time = time.time()

        try:
            driver = self.get_search_driver()
        except Exception:
            LOGGER.exception("Failed to start Selenium Google driver for query: %s", query)
            return {}, []

        search_url = f"{GOOGLE_SEARCH_URL}?q={quote_plus(query)}&hl=en&gl=us&num=10"
        try:
            interaction_ok = self.execute_google_search_interaction(driver, query, started_at)
            if not interaction_ok:
                driver.get(search_url)
                WebDriverWait(driver, min(2.5, max(0.75, self.remaining_google_budget(started_at)))).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            self.apply_google_page_humanization(driver)
            time.sleep(random.uniform(0.08, 0.2))
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            page_title = clean_text(driver.title)
        except TimeoutException:
            LOGGER.warning("Timed out waiting for Selenium Google page body for query: %s", query)
            return {}, []
        except WebDriverException:
            LOGGER.exception("Selenium failed while loading Google query: %s", query)
            return {}, []

        google_block_markers = (
            "Our systems have detected unusual traffic",
            "/httpservice/retry/enablejs",
            "enablejs",
            "detected unusual traffic",
            "/sorry/",
        )
        if (
            any(marker in page_source for marker in google_block_markers)
            or soup.select_one("form#captcha-form")
            or "unusual traffic" in page_title.lower()
            or "sorry" in page_title.lower()
        ):
            LOGGER.warning("Google single-pass search returned anti-bot page for query=%s title=%s", query, page_title)
            return {}, []

        return self.extract_google_knowledge_panel_from_soup(soup, record), self.parse_google_results_from_soup(soup)

    def search_via_google(self, query: str, retries: int = 2) -> List[SearchResult]:
        _, results = self.fetch_google_search_payload(query, {})
        return results

    def extract_google_knowledge_panel(self, query: str, record: Dict[str, str]) -> Dict[str, str]:
        knowledge_panel_data, _ = self.fetch_google_search_payload(query, record)
        return knowledge_panel_data

    def reserve_google_fallback_attempt(self, pid: str) -> bool:
        with self.google_fallback_lock:
            if self.google_fallback_attempts >= GOOGLE_API_MISS_SAMPLE_LIMIT:
                LOGGER.info(
                    "Skipping Google API-miss fallback for PID=%s because sample limit %s is exhausted",
                    pid,
                    GOOGLE_API_MISS_SAMPLE_LIMIT,
                )
                return False
            self.google_fallback_attempts += 1
            LOGGER.info(
                "Reserved Google API-miss fallback slot %s/%s for PID=%s",
                self.google_fallback_attempts,
                GOOGLE_API_MISS_SAMPLE_LIMIT,
                pid,
            )
            return True

    def search_via_bing_html(self, query: str) -> List[SearchResult]:
        LOGGER.info("Running Bing HTML fallback search for query: %s", query)
        response = self.request_with_retries(
            url=BING_SEARCH_URL,
            retries=2,
            action_label=f"bing html fallback search [{query}]",
            params={"q": query, "cc": "us", "setlang": "en-US", "mkt": "en-US", "ensearch": "1"},
            headers={"Accept": "text/html,application/xhtml+xml"},
        )
        soup = BeautifulSoup(response.text, "html.parser")
        results: List[SearchResult] = []
        for node in soup.select("li.b_algo"):
            link = node.select_one("h2 a")
            if not link:
                continue
            href = extract_bing_target_url(link.get("href", ""))
            title = clean_text(link.get_text(" ", strip=True))
            snippet_node = node.select_one(".b_caption p") or node.select_one(".b_caption")
            snippet = clean_text(snippet_node.get_text(" ", strip=True) if snippet_node else "")
            if is_usable_search_result(href, title, snippet):
                results.append(
                    SearchResult(
                        rank=len(results) + 1,
                        title=title,
                        url=href,
                        snippet=snippet,
                        provider="bing_html",
                    )
                )
            if len(results) >= SEARCH_RESULTS_LIMIT:
                break
        return results

    def search_via_bing_rss(self, query: str) -> List[SearchResult]:
        LOGGER.info("Running Bing RSS fallback search for query: %s", query)
        response = self.request_with_retries(
            url=BING_SEARCH_URL,
            retries=2,
            action_label=f"bing rss fallback search [{query}]",
            params={"q": query, "format": "rss", "cc": "us", "setlang": "en-US", "mkt": "en-US"},
            headers={"Accept": "application/rss+xml,application/xml,text/xml"},
        )
        soup = BeautifulSoup(response.text, "xml")
        results: List[SearchResult] = []
        for item in soup.select("item"):
            title = clean_text(item.title.get_text(" ", strip=True) if item.title else "")
            href = normalize_url(item.link.get_text(" ", strip=True) if item.link else "")
            snippet = clean_text(item.description.get_text(" ", strip=True) if item.description else "")
            if is_usable_search_result(href, title, snippet):
                results.append(
                    SearchResult(
                        rank=len(results) + 1,
                        title=title,
                        url=href,
                        snippet=snippet,
                        provider="bing_rss",
                    )
                )
            if len(results) >= SEARCH_RESULTS_LIMIT:
                break
        return results

    def search_via_yahoo_html(self, query: str) -> List[SearchResult]:
        LOGGER.info("Running Yahoo HTML fallback search for query: %s", query)
        response = self.request_with_retries(
            url=YAHOO_SEARCH_URL,
            retries=2,
            action_label=f"yahoo html fallback search [{query}]",
            params={"p": query},
            headers={"Accept": "text/html,application/xhtml+xml"},
        )
        soup = BeautifulSoup(response.text, "html.parser")
        if "Yahoo - 999 Unable to process request at this time" in response.text:
            LOGGER.warning("Yahoo fallback returned anti-bot page for query=%s", query)
            return []
        results: List[SearchResult] = []
        for node in soup.select("div.algo, div#web ol li"):
            link = node.select_one("h3 a") or node.select_one("a")
            if not link:
                continue
            href = normalize_url(link.get("href", ""))
            title = clean_text(link.get_text(" ", strip=True))
            snippet_node = node.select_one(".compText") or node.select_one("p")
            snippet = clean_text(snippet_node.get_text(" ", strip=True) if snippet_node else "")
            if is_usable_search_result(href, title, snippet):
                results.append(
                    SearchResult(
                        rank=len(results) + 1,
                        title=title,
                        url=href,
                        snippet=snippet,
                        provider="yahoo_html",
                    )
                )
            if len(results) >= SEARCH_RESULTS_LIMIT:
                break
        return results

    def fetch_html(self, url: str, retries: int = FETCH_RETRIES) -> Tuple[str, str]:
        LOGGER.info("Fetching URL: %s", url)
        response = self.request_with_retries(
            url=url,
            retries=retries,
            action_label=f"fetch [{url}]",
            allow_redirects=True,
            headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
        )
        content_type = response.headers.get("content-type", "").lower()
        LOGGER.debug(
            "Fetched URL=%s final_url=%s status=%s content_type=%s",
            url,
            response.url,
            response.status_code,
            content_type,
        )
        if "text/html" not in content_type and "xml" not in content_type:
            LOGGER.warning("Skipping non-HTML response for %s with content type %s", response.url, content_type)
            return response.url, ""
        return response.url, response.text

    def request_with_retries(
        self, url: str, retries: int, action_label: str, method: str = "GET", **kwargs
    ) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                LOGGER.debug("Attempt %s/%s for %s method=%s", attempt, retries, action_label, method)
                response = self.session.request(method, url, **kwargs)
                LOGGER.debug(
                    "Successful attempt %s/%s for %s with status %s",
                    attempt,
                    retries,
                    action_label,
                    response.status_code,
                )
                return response
            except requests.RequestException as exc:
                last_error = exc
                should_retry = attempt < retries
                response = getattr(exc, "response", None)
                if response is not None and response.status_code == 429:
                    cooldown = RATE_LIMIT_COOLDOWN_SECONDS * attempt
                    LOGGER.warning(
                        "Rate limited during %s; cooling down for %s seconds",
                        action_label,
                        cooldown,
                    )
                    time.sleep(cooldown)
                LOGGER.warning(
                    "Attempt %s/%s failed for %s: %s",
                    attempt,
                    retries,
                    action_label,
                    exc,
                )
                if should_retry:
                    sleep_seconds = RETRY_BACKOFF_SECONDS * attempt + random.uniform(0.25, 1.0)
                    LOGGER.info(
                        "Retrying %s after %.2f seconds",
                        action_label,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)
        if last_error:
            LOGGER.error(
                "All retry attempts failed for %s",
                action_label,
                exc_info=(type(last_error), last_error, last_error.__traceback__),
            )
        if last_error:
            raise last_error
        raise RuntimeError(f"Unknown failure for {action_label}")

    def parse_json_ld(self, soup: BeautifulSoup) -> List[dict]:
        objects: List[dict] = []
        for node in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
            raw = node.string or node.get_text(" ", strip=True)
            if not raw:
                continue
            raw = raw.strip()
            try:
                parsed = json.loads(raw)
            except Exception:
                LOGGER.debug("Failed to parse one JSON-LD block", exc_info=True)
                continue
            self.flatten_json_ld(parsed, objects)
        LOGGER.debug("Extracted %s JSON-LD objects", len(objects))
        return objects

    def flatten_json_ld(self, value, objects: List[dict]) -> None:
        if isinstance(value, dict):
            if "@graph" in value and isinstance(value["@graph"], list):
                for item in value["@graph"]:
                    self.flatten_json_ld(item, objects)
            objects.append(value)
            for nested in value.values():
                if isinstance(nested, (dict, list)):
                    self.flatten_json_ld(nested, objects)
        elif isinstance(value, list):
            for item in value:
                self.flatten_json_ld(item, objects)

    def extract_structured_data(self, soup: BeautifulSoup) -> Dict[str, str]:
        info = {
            "name": "",
            "Mailing_Address": "",
            "Mailing_Zip": "",
            "Telephone": "",
            "URL": "",
        }
        type_keywords = {
            "organization",
            "localbusiness",
            "preschool",
            "childcare",
            "school",
            "educationalorganization",
            "elementaryschool",
        }
        for obj in self.parse_json_ld(soup):
            raw_type = obj.get("@type", "")
            types = {clean_text(raw_type).lower()} if isinstance(raw_type, str) else {
                clean_text(item).lower() for item in raw_type if isinstance(item, str)
            }
            if not types & type_keywords:
                continue
            if not info["name"]:
                info["name"] = clean_text(obj.get("name"))
            if not info["Telephone"]:
                info["Telephone"] = normalize_phone(obj.get("telephone"))
            if not info["URL"]:
                info["URL"] = normalize_url(obj.get("url"))
            address = obj.get("address")
            if isinstance(address, dict):
                street = clean_text(address.get("streetAddress"))
                city = clean_text(address.get("addressLocality"))
                region = clean_text(address.get("addressRegion"))
                postal = normalize_zip(address.get("postalCode"))
                address_parts = [part for part in [street, city, region, postal] if part]
                if not info["Mailing_Address"] and address_parts:
                    info["Mailing_Address"] = ", ".join(address_parts)
                if not info["Mailing_Zip"] and postal:
                    info["Mailing_Zip"] = postal
        LOGGER.debug("Structured data extracted: %s", info)
        return info

    def extract_contact_links(self, base_url: str, soup: BeautifulSoup) -> List[str]:
        links = []
        for node in soup.find_all("a", href=True):
            href = node.get("href", "")
            text = clean_text(node.get_text(" ", strip=True)).lower()
            url = normalize_url(urljoin(base_url, href))
            if not url.startswith(("http://", "https://")):
                continue
            if domain_of(url) != domain_of(base_url):
                continue
            if any(keyword in url.lower() or keyword in text for keyword in CONTACT_KEYWORDS):
                links.append(url)
        deduped = []
        seen = set()
        for link in links:
            if link not in seen:
                deduped.append(link)
                seen.add(link)
        LOGGER.debug("Extracted %s contact links from %s", len(deduped[:CONTACT_PAGE_LIMIT]), base_url)
        return deduped[:CONTACT_PAGE_LIMIT]

    def extract_phone_from_text(self, text: str) -> str:
        match = PHONE_RE.search(text)
        return normalize_phone(match.group(0)) if match else ""

    def extract_zip_from_text(self, text: str, record: Optional[Dict[str, str]] = None) -> str:
        city = clean_text(record["Mailing_City"]) if record else ""
        state = clean_text(record["Mailing_State"]) if record else ""
        state_name = STATE_NAMES.get(state, "")
        if city and state:
            city_pattern = re.compile(
                rf"\b{re.escape(city)}\b.*?\b(?:{re.escape(state)}|{re.escape(state_name)})\b.*?(\d{{5}}(?:-\d{{4}})?)",
                re.IGNORECASE,
            )
            match = city_pattern.search(text)
            if match:
                return match.group(1)
        matches = ZIP_RE.findall(text)
        return matches[-1] if matches else ""

    def extract_capacity(self, text: str, snippets: List[str]) -> str:
        for blob in [text] + snippets:
            if not blob:
                continue
            match = CAPACITY_RE.search(blob)
            if match:
                LOGGER.debug("Capacity extracted: %s", match.group(1))
                return match.group(1)
        return ""

    def extract_age_range(self, text: str, snippets: List[str]) -> str:
        for blob in [text] + snippets:
            if not blob:
                continue
            specific_match = re.search(
                r"provider accepts children ages of:\s*([A-Za-z0-9 ,/&+\-;]+?)(?:[.\n]|license|$)",
                blob,
                re.IGNORECASE,
            )
            if specific_match:
                value = clean_text(specific_match.group(1).replace(";", ", "))
                value = re.sub(r"\bTotal Capacity:\s*\d+\b", "", value, flags=re.I).strip(" ,:-")
                value = normalize_age_range_value(value)
                if value:
                    LOGGER.debug("Age range extracted from specific pattern: %s", value)
                    return value
            for regex in (AGE_RANGE_RE, AGE_RANGE_ALT_RE):
                match = regex.search(blob)
                if match:
                    value = clean_text(match.group(1))
                    value = re.sub(r"\b(children|child)\b", "", value, flags=re.I).strip(" ,:-")
                    if len(value) > 80 or "too young" in value.lower():
                        continue
                    value = normalize_age_range_value(value)
                    if value:
                        LOGGER.debug("Age range extracted from generic pattern: %s", value)
                        return value
        return ""

    def extract_address_from_text(self, text: str, record: Dict[str, str]) -> str:
        lines = [normalize_space(line) for line in text.splitlines()]
        lines = [line for line in lines if line]
        city = clean_text(record["Mailing_City"])
        state = clean_text(record["Mailing_State"])
        state_name = STATE_NAMES.get(state, "")
        for index, line in enumerate(lines):
            lower = line.lower()
            if city.lower() in lower and (state.lower() in lower or state_name.lower() in lower):
                if ZIP_RE.search(line):
                    previous = lines[index - 1] if index > 0 else ""
                    if re.search(r"\d", previous):
                        return f"{previous}, {line}"
                    return line
        joined = " ".join(lines)
        pattern = re.compile(
            rf"(\d{{1,6}}[^.]*?\b{re.escape(city)}\b[^.]*?\b(?:{re.escape(state)}|{re.escape(state_name)})\b[^.]*?\b\d{{5}}(?:-\d{{4}})?\b)",
            re.IGNORECASE,
        )
        match = pattern.search(joined)
        return clean_text(match.group(1)) if match else ""

    def extract_from_page(self, url: str, html: str, record: Dict[str, str]) -> Dict[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)
        data = self.extract_structured_data(soup)
        data["Telephone"] = data["Telephone"] or self.extract_phone_from_text(text)
        data["Mailing_Zip"] = data["Mailing_Zip"] or self.extract_zip_from_text(text, record)
        data["Mailing_Address"] = data["Mailing_Address"] or self.extract_address_from_text(text, record)
        data["URL"] = data["URL"] or normalize_url(url)
        data["Capacity (optional)"] = self.extract_capacity(text, [])
        data["Age Range (optional)"] = self.extract_age_range(text, [])
        title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
        data["title"] = title
        LOGGER.debug(
            "Extracted page data for PID=%s url=%s title=%s data=%s",
            record.get("PID", ""),
            url,
            title,
            data,
        )
        return data

    def score_official_candidate(
        self, record: Dict[str, str], result: SearchResult, page_data: Optional[Dict[str, str]] = None
    ) -> int:
        score = 0
        if likely_official_domain(result.url):
            score += 5
        if result.rank <= 3:
            score += 4 - result.rank
        combined = " ".join(
            part
            for part in [
                result.title,
                result.snippet,
                page_data.get("title", "") if page_data else "",
                page_data.get("name", "") if page_data else "",
            ]
            if part
        )
        score += token_overlap_score(record["Daycare_Name"], combined) * 3
        if clean_text(record["Mailing_City"]).lower() in combined.lower():
            score += 2
        state = clean_text(record["Mailing_State"])
        state_name = STATE_NAMES.get(state, "")
        if state.lower() in combined.lower() or state_name.lower() in combined.lower():
            score += 1
        if is_listing_domain(result.url):
            score -= 4
        return score

    def score_listing_candidate(self, record: Dict[str, str], result: SearchResult) -> int:
        score = 0
        if is_listing_domain(result.url):
            score += 5
        if "capacity" in result.snippet.lower():
            score += 3
        if "ages" in result.snippet.lower() or "age" in result.snippet.lower():
            score += 2
        score += token_overlap_score(record["Daycare_Name"], f"{result.title} {result.snippet}") * 2
        if clean_text(record["Mailing_City"]).lower() in result.snippet.lower():
            score += 1
        return score

    def merge_values(self, base: Dict[str, str], extra: Dict[str, str], prefer_existing: bool = True) -> Dict[str, str]:
        merged = dict(base)
        for key, value in extra.items():
            if key not in merged:
                continue
            if prefer_existing and merged.get(key):
                continue
            if value:
                merged[key] = value
                LOGGER.debug("Merged field %s with value %s", key, value)
        return merged

    def set_field_value(
        self,
        row: Dict[str, str],
        sources: Dict[str, Dict[str, str]],
        field: str,
        value: str,
        source_url: str,
        source_type: str,
        prefer_existing: bool = True,
        notes: str = "",
    ) -> None:
        value = clean_text(value)
        if not value:
            return
        if prefer_existing and clean_text(row.get(field)):
            return
        row[field] = value
        sources[field] = build_source_entry(value=value, source_url=source_url, source_type=source_type, notes=notes)
        LOGGER.debug("Set field %s to %s from %s", field, value, source_url)

    def set_match_metadata(
        self,
        row: Dict[str, str],
        sources: Dict[str, Dict[str, str]],
        matched_provider_name: str,
        match_status: str,
        match_confidence: str,
        matched_reason: str,
        source_url: str,
        source_type: str,
    ) -> None:
        metadata_values = {
            "Matched_Provider_Name": clean_text(matched_provider_name),
            "Match_Status": clean_text(match_status),
            "Match_Confidence": clean_text(match_confidence),
            "Matched_Reason": clean_text(matched_reason),
        }
        for field, value in metadata_values.items():
            if not value:
                continue
            row[field] = value
            sources[field] = build_source_entry(
                value=value,
                source_url=source_url,
                source_type=source_type,
                notes="Derived from accepted provider match evaluation.",
            )
        LOGGER.info(
            "Recorded match metadata PID=%s status=%s confidence=%s matched_provider=%s",
            row.get("PID", ""),
            row.get("Match_Status", ""),
            row.get("Match_Confidence", ""),
            row.get("Matched_Provider_Name", ""),
        )

    def seed_sources_from_input(self, row: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        sources: Dict[str, Dict[str, str]] = {}
        derived_fields = {
            "Normalized_Name",
            "Search_Name_Primary",
            "Search_Name_Variants",
            "Match_Status",
            "Match_Confidence",
            "Matched_Provider_Name",
            "Matched_Reason",
        }
        for field in OUTPUT_HEADERS:
            value = clean_text(row.get(field))
            if value:
                if field in derived_fields:
                    sources[field] = build_source_entry(
                        value=value,
                        source_type="derived_metadata",
                        notes="Derived during name cleaning or match evaluation.",
                    )
                else:
                    sources[field] = build_source_entry(value=value, source_type="input_file", notes="Loaded from input CSV")
        return sources

    def extract_listing_data(self, html: str, record: Dict[str, str]) -> Dict[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)
        data = {
            "Mailing_Address": self.extract_address_from_text(text, record),
            "Mailing_Zip": self.extract_zip_from_text(text, record),
            "Telephone": self.extract_phone_from_text(text),
            "Capacity (optional)": self.extract_capacity(text, []),
            "Age Range (optional)": self.extract_age_range(text, []),
            "URL": "",
        }
        for node in soup.find_all("a", href=True):
            href = normalize_url(urljoin("https://placeholder.local", node.get("href", "")))
            text_value = clean_text(node.get_text(" ", strip=True)).lower()
            if "website" in text_value or "visit website" in text_value:
                actual = node.get("href", "")
                if actual.startswith(("http://", "https://")):
                    data["URL"] = normalize_url(actual)
                    break
        LOGGER.debug("Listing data extracted for PID=%s: %s", record.get("PID", ""), data)
        return data

    def enrich_from_google_api_miss_fallback(
        self,
        record: Dict[str, str],
        base_output: Dict[str, str],
        field_sources: Dict[str, Dict[str, str]],
    ) -> None:
        pid = clean_text(record.get("PID"))
        state = clean_text(record.get("Mailing_State"))
        if not RUN_GOOGLE_ONLY_SAMPLE_MODE and state not in load_active_api_model_states():
            LOGGER.info(
                "Skipping Google API-miss fallback for PID=%s because state=%s is not an active API state",
                pid,
                state,
            )
            return
        if row_has_found_data(base_output):
            LOGGER.info("Skipping Google API-miss fallback for PID=%s because API data was already found", pid)
            return
        if not ENABLE_GOOGLE_FALLBACK_FOR_API_MISSES:
            LOGGER.info("Google API-miss fallback disabled for PID=%s", pid)
            return
        if not self.reserve_google_fallback_attempt(pid):
            return

        with self.google_fallback_semaphore:
            LOGGER.info(
                "PID=%s entered Google API-miss fallback semaphore (%s concurrent allowed)",
                pid,
                GOOGLE_FALLBACK_MAX_CONCURRENT,
            )
            profile = get_record_name_profile(record)
            if RUN_GOOGLE_ONLY_SAMPLE_MODE:
                queries = [
                    " ".join(
                        part
                        for part in [
                            f'"{profile.search_name_primary}"' if clean_text(profile.search_name_primary) else "",
                            record.get("Mailing_City", ""),
                            record.get("Mailing_State", ""),
                        ]
                        if clean_text(part)
                    )
                ]
            else:
                search_variants = profile.search_name_variants[:3] or [profile.search_name_primary]
                queries = dedupe_preserve_order(
                    [
                        " ".join(
                            part
                            for part in [variant, record.get("Mailing_City", ""), record.get("Mailing_State", ""), "child care"]
                            if clean_text(part)
                        )
                        for variant in search_variants
                    ]
                )
            google_results: List[SearchResult] = []
            attempted_queries: List[str] = []
            knowledge_panel_data: Dict[str, str] = {}
            for query in queries:
                attempted_queries.append(query)
                try:
                    knowledge_panel_data, google_results = self.fetch_google_search_payload(query, record)
                except Exception:
                    LOGGER.exception("Google API-miss fallback search failed for PID=%s query=%s", pid, query)
                    continue
                break
            LOGGER.info(
                "Google API-miss fallback for PID=%s attempted_queries=%s parsed_results=%s",
                pid,
                attempted_queries,
                len(google_results),
            )
            if knowledge_panel_data and any(
                clean_text(knowledge_panel_data.get(field))
                for field in ["Mailing_Address", "Mailing_Zip", "Telephone", "URL"]
            ):
                source_url = knowledge_panel_data.get("URL", "") or GOOGLE_SEARCH_URL
                self.set_field_value(base_output, field_sources, "Mailing_Address", knowledge_panel_data.get("Mailing_Address", ""), source_url, "google_knowledge_panel")
                self.set_field_value(base_output, field_sources, "Mailing_Zip", knowledge_panel_data.get("Mailing_Zip", ""), source_url, "google_knowledge_panel")
                self.set_field_value(base_output, field_sources, "Telephone", knowledge_panel_data.get("Telephone", ""), source_url, "google_knowledge_panel")
                if knowledge_panel_data.get("URL"):
                    self.set_field_value(base_output, field_sources, "URL", knowledge_panel_data.get("URL", ""), source_url, "google_knowledge_panel")
                match_status, match_confidence, match_reason = classify_match_status(
                    record,
                    candidate_name=knowledge_panel_data.get("Matched_Provider_Name", "") or record.get("Daycare_Name", ""),
                    candidate_city="",
                    candidate_address=knowledge_panel_data.get("Mailing_Address", ""),
                    candidate_phone=knowledge_panel_data.get("Telephone", ""),
                    candidate_url=knowledge_panel_data.get("URL", ""),
                )
                self.set_match_metadata(
                    base_output,
                    field_sources,
                    matched_provider_name=knowledge_panel_data.get("Matched_Provider_Name", "") or record.get("Daycare_Name", ""),
                    match_status=match_status,
                    match_confidence=match_confidence,
                    matched_reason="Google knowledge panel matched the facility search and provided direct contact/location details.",
                    source_url=source_url,
                    source_type="google_knowledge_panel",
                )
                LOGGER.info("Google knowledge panel provided data for PID=%s", pid)
                return
            if not google_results:
                return

            snippets = [item.snippet for item in google_results if item.snippet]
            best_official = None
            best_official_score = -999
            official_data: Dict[str, str] = {}
            for result in google_results:
                if not likely_official_domain(result.url):
                    continue
                try:
                    fetched_url, html = self.fetch_html(result.url, retries=1 if RUN_GOOGLE_ONLY_SAMPLE_MODE else FETCH_RETRIES)
                except Exception:
                    LOGGER.exception("PID=%s failed to fetch Google-official candidate %s", pid, result.url)
                    continue
                if not html:
                    continue
                page_data = self.extract_from_page(fetched_url, html, record)
                score = self.score_official_candidate(record, result, page_data)
                if score > best_official_score:
                    best_official_score = score
                    best_official = result
                    official_data = page_data

            if best_official and best_official_score >= 8:
                official_source_url = official_data.get("URL", "") or best_official.url
                self.set_field_value(base_output, field_sources, "Mailing_Address", official_data.get("Mailing_Address", ""), official_source_url, "google_official")
                self.set_field_value(base_output, field_sources, "Mailing_Zip", official_data.get("Mailing_Zip", ""), official_source_url, "google_official")
                self.set_field_value(base_output, field_sources, "Telephone", official_data.get("Telephone", ""), official_source_url, "google_official")
                self.set_field_value(base_output, field_sources, "URL", official_source_url, official_source_url, "google_official")
                self.set_field_value(base_output, field_sources, "Capacity (optional)", official_data.get("Capacity (optional)", ""), official_source_url, "google_official")
                self.set_field_value(base_output, field_sources, "Age Range (optional)", official_data.get("Age Range (optional)", ""), official_source_url, "google_official")
                match_status, match_confidence, match_reason = classify_match_status(
                    record,
                    candidate_name=clean_text(official_data.get("name") or official_data.get("title") or best_official.title),
                    candidate_city="",
                    candidate_address=official_data.get("Mailing_Address", ""),
                    candidate_phone=official_data.get("Telephone", ""),
                    candidate_url=official_source_url,
                )
                self.set_match_metadata(
                    base_output,
                    field_sources,
                    matched_provider_name=clean_text(official_data.get("name") or official_data.get("title") or best_official.title),
                    match_status=match_status,
                    match_confidence=match_confidence,
                    matched_reason=match_reason,
                    source_url=official_source_url,
                    source_type="google_official",
                )
                LOGGER.info("Google API-miss fallback accepted official candidate for PID=%s url=%s", pid, official_source_url)
                return

            best_listing = None
            best_listing_score = -999
            for result in google_results:
                if not (is_trusted_public_source(result.url) or "care.com" in domain_of(result.url)):
                    continue
                score = self.score_listing_candidate(record, result) + (3 if "care.com" in domain_of(result.url) else 0)
                if score > best_listing_score:
                    best_listing_score = score
                    best_listing = result

            if best_listing and best_listing_score >= 4:
                listing_values = {
                    "Capacity (optional)": self.extract_capacity(best_listing.snippet, snippets),
                    "Age Range (optional)": self.extract_age_range(best_listing.snippet, snippets),
                }
                try:
                    _, listing_html = self.fetch_html(best_listing.url, retries=1 if RUN_GOOGLE_ONLY_SAMPLE_MODE else FETCH_RETRIES)
                except Exception:
                    LOGGER.exception("PID=%s failed to fetch Google listing candidate %s", pid, best_listing.url)
                    listing_html = ""
                if listing_html:
                    fetched_listing = self.extract_listing_data(listing_html, record)
                    listing_values = self.merge_values(listing_values, fetched_listing, prefer_existing=True)
                    self.set_field_value(base_output, field_sources, "Mailing_Address", fetched_listing.get("Mailing_Address", ""), best_listing.url, "google_listing")
                    self.set_field_value(base_output, field_sources, "Mailing_Zip", fetched_listing.get("Mailing_Zip", ""), best_listing.url, "google_listing")
                    self.set_field_value(base_output, field_sources, "Telephone", fetched_listing.get("Telephone", ""), best_listing.url, "google_listing")
                    if fetched_listing.get("URL"):
                        self.set_field_value(base_output, field_sources, "URL", fetched_listing.get("URL", ""), fetched_listing.get("URL", ""), "google_listing")
                self.set_field_value(base_output, field_sources, "Capacity (optional)", listing_values.get("Capacity (optional)", ""), best_listing.url, "google_listing")
                self.set_field_value(base_output, field_sources, "Age Range (optional)", listing_values.get("Age Range (optional)", ""), best_listing.url, "google_listing")
                match_status, match_confidence, match_reason = classify_match_status(
                    record,
                    candidate_name=best_listing.title,
                    candidate_city="",
                    candidate_address=base_output.get("Mailing_Address", ""),
                    candidate_phone=base_output.get("Telephone", ""),
                    candidate_url=best_listing.url,
                )
                self.set_match_metadata(
                    base_output,
                    field_sources,
                    matched_provider_name=best_listing.title,
                    match_status=match_status,
                    match_confidence=match_confidence,
                    matched_reason=match_reason,
                    source_url=best_listing.url,
                    source_type="google_listing",
                )
                LOGGER.info("Google API-miss fallback accepted listing candidate for PID=%s url=%s", pid, best_listing.url)

    def enrich_record(self, record: Dict[str, str]) -> Dict[str, str]:
        pid = clean_text(record["PID"])
        search_profile = get_record_name_profile(record)
        LOGGER.info(
            "Starting enrichment for PID=%s daycare=%s city=%s state=%s",
            pid,
            record.get("Daycare_Name", ""),
            record.get("Mailing_City", ""),
            record.get("Mailing_State", ""),
        )
        LOGGER.info(
            "PID=%s name cleaning original=%s normalized=%s primary=%s variants=%s",
            pid,
            search_profile.original_name,
            search_profile.normalized_name,
            search_profile.search_name_primary,
            search_profile.search_name_variants,
        )
        if SINGLE_PID_FILTER:
            staged = {}
            cached = {}
            LOGGER.info("PID=%s fresh-run mode ignoring staged/checkpointed rows for debugging measurement", pid)
        else:
            staged_payload = self.get_staging_row(pid)
            staged, staged_sources = self.extract_checkpoint_payload(staged_payload)
            if staged and has_meaningful_enrichment(staged):
                for header in OUTPUT_HEADERS:
                    staged.setdefault(header, "")
                apply_name_profile_to_row(staged, get_record_name_profile(staged or record))
                LOGGER.info("Returning staged success for PID=%s without hitting the source again", pid)
                return staged
            cached_payload = self.get_checkpoint_row(pid)
            cached, cached_sources = self.extract_checkpoint_payload(cached_payload)
            if cached and has_meaningful_enrichment(cached):
                for header in OUTPUT_HEADERS:
                    cached.setdefault(header, "")
                apply_name_profile_to_row(cached, get_record_name_profile(cached or record))
                LOGGER.info("Returning cached result for PID=%s", pid)
                return cached
            if cached:
                LOGGER.warning("Ignoring stale blank checkpoint row for PID=%s and retrying enrichment", pid)

        base_output = {header: clean_text(record.get(header, "")) for header in OUTPUT_HEADERS}
        apply_name_profile_to_row(base_output, search_profile)
        field_sources = self.seed_sources_from_input(base_output)
        base_output["Telephone"] = normalize_phone(base_output["Telephone"])
        base_output["Mailing_Zip"] = normalize_zip(base_output["Mailing_Zip"])
        search_record = dict(record)
        apply_name_profile_to_row(search_record, search_profile)
        existing_required = [
            clean_text(base_output.get("Mailing_Address")),
            clean_text(base_output.get("Mailing_Zip")),
            clean_text(base_output.get("Telephone")),
            clean_text(base_output.get("URL")),
        ]
        if any(existing_required):
            LOGGER.info("PID=%s already contains required fields in input; skipping live enrichment", pid)
            base_output["Match_Status"] = "exact_match"
            base_output["Match_Confidence"] = "100"
            base_output["Matched_Provider_Name"] = base_output.get("Daycare_Name", "")
            base_output["Matched_Reason"] = "Input CSV already contained direct contact fields; live search was skipped."
            self.set_checkpoint_row(pid, base_output, field_sources)
            return base_output

        if CSV_CLEANING_ONLY_MODE:
            LOGGER.info(
                "PID=%s CSV cleaning only mode enabled; skipping state portals and website fallback searches",
                pid,
            )
            base_output["Match_Status"] = "not_found"
            base_output["Match_Confidence"] = "0"
            base_output["Matched_Provider_Name"] = ""
            base_output["Matched_Reason"] = "CSV cleaning only mode; generated normalized search fields without live website enrichment."
            return base_output

        try:
            portal_values, portal_sources = self.enrich_from_state_portal(search_record)
        except Exception as exc:
            if isinstance(exc, TimeoutException) and clean_text(record.get("Mailing_State")) in ADAPTER_REGISTRY:
                self.queue_adapter_timeout_retry(record)
            LOGGER.exception("Official state portal enrichment failed for PID=%s", pid)
            portal_values, portal_sources = {}, {}

        for field, value in portal_values.items():
            source = portal_sources.get(field, {})
            self.set_field_value(
                base_output,
                field_sources,
                field,
                value,
                source.get("source_url", ""),
                source.get("source_type", "official_state_portal"),
                notes=source.get("notes", ""),
            )

        base_output["Telephone"] = normalize_phone(base_output.get("Telephone"))
        base_output["Mailing_Zip"] = normalize_zip(base_output.get("Mailing_Zip"))
        base_output["URL"] = normalize_url(base_output.get("URL"))
        for field in ("Telephone", "Mailing_Zip", "URL"):
            if field in field_sources:
                field_sources[field]["value"] = base_output.get(field, "")
        LOGGER.info(
            "PID=%s completed using adapter/api-only mode address=%s zip=%s phone=%s url=%s capacity=%s age=%s",
            pid,
            base_output.get("Mailing_Address", ""),
            base_output.get("Mailing_Zip", ""),
            base_output.get("Telephone", ""),
            base_output.get("URL", ""),
            base_output.get("Capacity (optional)", ""),
            base_output.get("Age Range (optional)", ""),
        )
        if not row_has_found_data(base_output):
            base_output["Match_Status"] = "not_found"
            base_output["Match_Confidence"] = "0"
            base_output["Matched_Reason"] = "No datapoints were found from the configured state adapter or API."
        self.set_checkpoint_row(pid, base_output, field_sources)
        self.set_staging_row(pid, base_output, field_sources)
        current_checkpoint_size = self.checkpoint_size()
        if current_checkpoint_size % 20 == 0:
            self.save_checkpoint()
            self.save_staging()
        return base_output

        LOGGER.info("PID=%s official search queries attempted=%s parsed_results=%s", pid, attempted_search_queries, len(search_results))
        snippets = [item.snippet for item in search_results if item.snippet]
        best_official = None
        best_official_score = -999
        official_data = {}

        for result in search_results:
            if not likely_official_domain(result.url):
                LOGGER.debug("PID=%s rejected official candidate url=%s", pid, result.url)
                continue
            try:
                fetched_url, html = self.fetch_html(result.url)
            except Exception:
                LOGGER.exception("PID=%s failed to fetch official candidate %s", pid, result.url)
                continue
            if not html:
                LOGGER.debug("PID=%s official candidate had empty HTML %s", pid, result.url)
                continue
            page_data = self.extract_from_page(fetched_url, html, search_record)
            for contact_url in self.extract_contact_links(fetched_url, BeautifulSoup(html, "html.parser")):
                if page_data.get("Mailing_Address") and page_data.get("Telephone"):
                    break
                try:
                    _, contact_html = self.fetch_html(contact_url)
                except Exception:
                    LOGGER.exception("PID=%s failed to fetch contact page %s", pid, contact_url)
                    continue
                if contact_html:
                    contact_data = self.extract_from_page(contact_url, contact_html, search_record)
                    page_data = self.merge_values(page_data, contact_data)
            score = self.score_official_candidate(record, result, page_data)
            LOGGER.info(
                "PID=%s official candidate scored %s url=%s extracted=%s",
                pid,
                score,
                result.url,
                {
                    "Mailing_Address": page_data.get("Mailing_Address", ""),
                    "Mailing_Zip": page_data.get("Mailing_Zip", ""),
                    "Telephone": page_data.get("Telephone", ""),
                    "URL": page_data.get("URL", ""),
                },
            )
            if score > best_official_score:
                best_official_score = score
                best_official = result
                official_data = page_data
            if best_official_score >= 12:
                LOGGER.info("PID=%s reached official confidence threshold with url=%s", pid, result.url)
                break

        if best_official:
            LOGGER.info(
                "PID=%s selected official candidate score=%s url=%s",
                pid,
                best_official_score,
                best_official.url,
            )
            official_source_url = official_data.get("URL", "") or best_official.url
            official_candidate_name = clean_text(official_data.get("name") or official_data.get("title") or best_official.title)
            self.set_field_value(base_output, field_sources, "Mailing_Address", official_data.get("Mailing_Address", ""), official_source_url, "official_site")
            self.set_field_value(base_output, field_sources, "Mailing_Zip", official_data.get("Mailing_Zip", ""), official_source_url, "official_site")
            self.set_field_value(base_output, field_sources, "Telephone", official_data.get("Telephone", ""), official_source_url, "official_site")
            self.set_field_value(base_output, field_sources, "URL", official_source_url, official_source_url, "official_site")
            self.set_field_value(
                base_output,
                field_sources,
                "Capacity (optional)",
                official_data.get("Capacity (optional)", ""),
                official_source_url,
                "official_site",
            )
            self.set_field_value(
                base_output,
                field_sources,
                "Age Range (optional)",
                official_data.get("Age Range (optional)", ""),
                official_source_url,
                "official_site",
            )
            match_status, match_confidence, match_reason = classify_match_status(
                search_record,
                candidate_name=official_candidate_name,
                candidate_city="",
                candidate_address=official_data.get("Mailing_Address", ""),
                candidate_phone=official_data.get("Telephone", ""),
                candidate_url=official_source_url,
                closed_hint=official_data.get("title", ""),
                prior_name_hint=False,
            )
            self.set_match_metadata(
                base_output,
                field_sources,
                matched_provider_name=official_candidate_name,
                match_status=match_status,
                match_confidence=match_confidence,
                matched_reason=match_reason,
                source_url=official_source_url,
                source_type="official_site",
            )
        else:
            LOGGER.warning("PID=%s did not find a suitable official candidate", pid)

        trusted_public_results: List[SearchResult] = []
        if ENABLE_TRUSTED_PUBLIC_SEARCH:
            public_queries = []
            for variant in search_name_variants[:3]:
                public_queries.append(
                    " ".join(
                        part
                        for part in [f'"{variant}"', record["Mailing_City"], record["Mailing_State"], "site:care.com"]
                        if clean_text(part)
                    )
                )
                public_queries.append(
                    " ".join(
                        part
                        for part in [f'"{variant}"', record["Mailing_City"], record["Mailing_State"], "child care license"]
                        if clean_text(part)
                    )
                )
            public_queries = dedupe_preserve_order(public_queries)
            seen_public_urls = set()
            for public_query in public_queries:
                try:
                    public_results = self.search(public_query)
                except Exception:
                    LOGGER.exception("Trusted public search failed for PID=%s query=%s", pid, public_query)
                    continue
                for result in public_results:
                    if result.url in seen_public_urls:
                        continue
                    seen_public_urls.add(result.url)
                    trusted_public_results.append(result)
        else:
            LOGGER.info("Trusted public search disabled for PID=%s to reduce rate pressure", pid)

        best_listing = None
        best_listing_score = -999
        for result in trusted_public_results:
            if not is_trusted_public_source(result.url):
                LOGGER.debug("PID=%s rejected non-trusted public candidate url=%s", pid, result.url)
                continue
            score = self.score_listing_candidate(record, result) + 4
            LOGGER.debug("PID=%s trusted public candidate score=%s url=%s", pid, score, result.url)
            if score > best_listing_score:
                best_listing_score = score
                best_listing = result

        if best_listing and best_listing_score >= 4:
            LOGGER.info(
                "PID=%s selected trusted public candidate score=%s url=%s",
                pid,
                best_listing_score,
                best_listing.url,
            )
            listing_values = {
                "Capacity (optional)": self.extract_capacity(best_listing.snippet, snippets),
                "Age Range (optional)": self.extract_age_range(best_listing.snippet, snippets),
            }
            try:
                _, listing_html = self.fetch_html(best_listing.url)
            except Exception:
                LOGGER.exception("PID=%s failed to fetch trusted public candidate %s", pid, best_listing.url)
                listing_html = ""
            if listing_html:
                fetched_listing = self.extract_listing_data(listing_html, search_record)
                listing_values = self.merge_values(listing_values, fetched_listing, prefer_existing=True)
                if not base_output.get("Telephone") or not base_output.get("Mailing_Address"):
                    self.set_field_value(
                        base_output, field_sources, "Mailing_Address", fetched_listing.get("Mailing_Address", ""), best_listing.url, "trusted_public"
                    )
                    self.set_field_value(
                        base_output, field_sources, "Mailing_Zip", fetched_listing.get("Mailing_Zip", ""), best_listing.url, "trusted_public"
                    )
                    self.set_field_value(
                        base_output, field_sources, "Telephone", fetched_listing.get("Telephone", ""), best_listing.url, "trusted_public"
                    )
                if not base_output.get("URL") and fetched_listing.get("URL") and likely_official_domain(
                    fetched_listing["URL"]
                ):
                    self.set_field_value(base_output, field_sources, "URL", fetched_listing["URL"], fetched_listing["URL"], "trusted_public_directory")

            self.set_field_value(
                base_output,
                field_sources,
                "Capacity (optional)",
                listing_values.get("Capacity (optional)", ""),
                best_listing.url,
                "trusted_public",
            )
            self.set_field_value(
                base_output,
                field_sources,
                "Age Range (optional)",
                listing_values.get("Age Range (optional)", ""),
                best_listing.url,
                "trusted_public",
            )
            listing_match_status, listing_match_confidence, listing_match_reason = classify_match_status(
                search_record,
                candidate_name=best_listing.title,
                candidate_city="",
                candidate_address=listing_values.get("Mailing_Address", ""),
                candidate_phone=listing_values.get("Telephone", ""),
                candidate_url=best_listing.url,
            )
            if listing_match_status != "not_found" or not base_output.get("Match_Status"):
                self.set_match_metadata(
                    base_output,
                    field_sources,
                    matched_provider_name=best_listing.title,
                    match_status=listing_match_status,
                    match_confidence=listing_match_confidence,
                    matched_reason=listing_match_reason,
                    source_url=best_listing.url,
                    source_type="trusted_public",
                )
        else:
            LOGGER.warning("PID=%s did not find a suitable trusted public candidate", pid)

        base_output["Telephone"] = normalize_phone(base_output.get("Telephone"))
        base_output["Mailing_Zip"] = normalize_zip(base_output.get("Mailing_Zip"))
        base_output["URL"] = normalize_url(base_output.get("URL"))
        for field in ("Telephone", "Mailing_Zip", "URL"):
            if field in field_sources:
                field_sources[field]["value"] = base_output.get(field, "")
        if not row_has_found_data(base_output) and not base_output.get("Match_Status"):
            base_output["Match_Status"] = "not_found"
            base_output["Match_Confidence"] = "0"
            base_output["Matched_Reason"] = "No datapoints were found from official or trusted public sources."
        LOGGER.info(
            "PID=%s final output address=%s zip=%s phone=%s url=%s capacity=%s age=%s match_status=%s matched_provider=%s",
            pid,
            base_output.get("Mailing_Address", ""),
            base_output.get("Mailing_Zip", ""),
            base_output.get("Telephone", ""),
            base_output.get("URL", ""),
            base_output.get("Capacity (optional)", ""),
            base_output.get("Age Range (optional)", ""),
            base_output.get("Match_Status", ""),
            base_output.get("Matched_Provider_Name", ""),
        )

        self.set_checkpoint_row(pid, base_output, field_sources)
        self.set_staging_row(pid, base_output, field_sources)
        current_checkpoint_size = self.checkpoint_size()
        if current_checkpoint_size % 20 == 0:
            self.save_checkpoint()
            self.save_staging()
        return base_output


def read_rows(path: str) -> List[Dict[str, str]]:
    LOGGER.info("Reading input rows from %s", path)
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        rows = list(reader)
    normalized_rows = []
    for row in rows:
        original_name = clean_text(row.get("Daycare_Name"))
        profile = get_record_name_profile(row) if clean_text(row.get("Search_Name_Primary")) else build_name_search_profile(original_name)
        normalized_rows.append(
            {
                "PID": clean_text(row.get("PID")),
                "DayCareType": clean_text(row.get("DayCareType")),
                "Daycare_Name": original_name,
                "Original_Name": clean_text(row.get("Original_Name")) or original_name,
                "Normalized_Name": profile.normalized_name,
                "Search_Name_Primary": profile.search_name_primary,
                "Search_Name_Variants": " || ".join(profile.search_name_variants),
                "Mailing_City": clean_text(row.get("Mailing_City")),
                "Mailing_State": clean_text(row.get("Mailing_State")),
                "Mailing_Address": clean_text(row.get("Mailing_Address")),
                "Mailing_Zip": clean_text(row.get("Mailing_Zip")),
                "Telephone": clean_text(row.get("Telephone")),
                "URL": clean_text(row.get("URL")),
                "Capacity (optional)": clean_text(row.get("Capacity (optional)")),
                "Age Range (optional)": clean_text(row.get("Age Range (optional),,")),
                "Match_Status": clean_text(row.get("Match_Status")),
                "Match_Confidence": clean_text(row.get("Match_Confidence")),
                "Matched_Provider_Name": clean_text(row.get("Matched_Provider_Name")),
                "Matched_Reason": clean_text(row.get("Matched_Reason")),
            }
        )
    LOGGER.info("Read %s rows from input file", len(normalized_rows))
    return normalized_rows


def write_rows(path: str, rows: List[Dict[str, str]]) -> None:
    output_rows = rows
    official_cache = load_checkpoint_file(CHECKPOINT_FILE)
    google_cache = load_checkpoint_file(GOOGLE_CHECKPOINT_FILE)
    if official_cache or google_cache:
        google_merge_fields = ("Mailing_Address", "Mailing_Zip", "Telephone", "URL")
        merged_rows: List[Dict[str, str]] = []
        for row in output_rows:
            pid = clean_text(row.get("PID"))
            official_payload = official_cache.get(pid, {})
            google_payload = google_cache.get(pid, {})
            official_row = dict(official_payload.get("row", {})) if isinstance(official_payload, dict) else {}
            google_row = dict(google_payload.get("row", {})) if isinstance(google_payload, dict) else {}
            base_row = dict(official_row or row)
            if not google_row or not row_has_found_data(google_row):
                merged_rows.append(base_row)
                continue
            daycare_name = clean_text(base_row.get("Daycare_Name", "")) or clean_text(google_row.get("Daycare_Name", ""))
            official_name = clean_text(base_row.get("Matched_Provider_Name", ""))
            google_name = clean_text(google_row.get("Matched_Provider_Name", ""))
            official_score = int(clean_text(base_row.get("Match_Confidence", "0")) or "0")
            google_score = int(clean_text(google_row.get("Match_Confidence", "0")) or "0")
            _, official_recall, official_precision = token_overlap_metrics(daycare_name, official_name)
            _, google_recall, google_precision = token_overlap_metrics(daycare_name, google_name)
            official_name_score = official_recall + official_precision
            google_name_score = google_recall + google_precision
            google_wins = google_name_score > official_name_score or (
                google_name_score == official_name_score and google_score > official_score
            )
            merged_row = dict(base_row)
            for field in google_merge_fields:
                official_value = clean_text(base_row.get(field))
                google_value = clean_text(google_row.get(field))
                if official_value and google_value and official_value != google_value:
                    merged_row[field] = google_value if google_wins else official_value
                elif not official_value and google_value:
                    merged_row[field] = google_value
            merged_rows.append(merged_row)
        output_rows = merged_rows
    if ADAPTER_ONLY_TEST_STATES:
        output_rows = [row for row in output_rows if clean_text(row.get("Mailing_State")) in ADAPTER_ONLY_TEST_STATES]
        LOGGER.info(
            "Adapter-only test output filtered from %s rows down to %s rows for states=%s",
            len(rows),
            len(output_rows),
            sorted(ADAPTER_ONLY_TEST_STATES),
        )
    elif RUN_API_STATE_TEST_MODE:
        api_states = set(load_active_api_model_states())
        output_rows = [
            row for row in output_rows if clean_text(row.get("Mailing_State")) in api_states
        ]
    LOGGER.info("Writing %s rows to %s", len(output_rows), path)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS, delimiter=";", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)
    LOGGER.info("Completed writing output CSV to %s", path)


def select_rows_for_run(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    if CSV_CLEANING_ONLY_MODE:
        LOGGER.info("CSV cleaning only mode enabled; using all %s processable rows without sampling", len(rows))
        return rows
    if SINGLE_PID_FILTER:
        filtered_rows = [row for row in rows if clean_text(row.get("PID")) == SINGLE_PID_FILTER]
        LOGGER.info(
            "Running single-PID mode for pid=%s with %s matched CSV rows",
            SINGLE_PID_FILTER,
            len(filtered_rows),
        )
        return filtered_rows
    if ADAPTER_ONLY_TEST_STATES:
        filtered_rows = [row for row in rows if clean_text(row.get("Mailing_State")) in ADAPTER_ONLY_TEST_STATES]
        LOGGER.info(
            "Running adapter-only test mode for states=%s with %s matched CSV rows",
            sorted(ADAPTER_ONLY_TEST_STATES),
            len(filtered_rows),
        )
        return filtered_rows
    if RUN_API_STATE_TEST_MODE:
        api_states = set(load_active_api_model_states())
        filtered_rows = [row for row in rows if clean_text(row.get("Mailing_State")) in api_states]
        LOGGER.info(
            "Running API-state test mode for states=%s with %s matching rows",
            sorted(api_states),
            len(filtered_rows),
        )
        return filtered_rows
    LOGGER.info("Running full dataset with %s processable rows", len(rows))
    return rows


def get_output_path() -> str:
    if CSV_CLEANING_ONLY_MODE:
        return CLEANED_INPUT_CSV
    if SINGLE_PID_FILTER:
        return os.path.join(OUTPUT_DIR, f"DaycareBuildings_Enriched_PID_{SINGLE_PID_FILTER}.csv")
    if ADAPTER_ONLY_TEST_STATES:
        suffix = "_".join(sorted(ADAPTER_ONLY_TEST_STATES))
        return os.path.join(OUTPUT_DIR, f"DaycareBuildings_Enriched_AdapterOnly_{suffix}.csv")
    if RUN_API_STATE_TEST_MODE:
        return os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_API.csv")
    return OUTPUT_CSV


def row_has_found_data(row: Dict[str, str]) -> bool:
    return any(clean_text(row.get(field)) for field in ENRICHMENT_VALUE_FIELDS)


def load_state_scraper_models() -> Dict[str, Dict[str, object]]:
    if not os.path.exists(STATE_SCRAPER_MODELS_FILE):
        LOGGER.warning("State scraper models file not found at %s", STATE_SCRAPER_MODELS_FILE)
        return {}
    try:
        with open(STATE_SCRAPER_MODELS_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        LOGGER.exception("Failed to load state scraper models from %s", STATE_SCRAPER_MODELS_FILE)
        return {}
    states = data.get("states", {}) if isinstance(data, dict) else {}
    return {code: item for code, item in states.items() if isinstance(item, dict)}


def load_active_model_states() -> List[str]:
    states = load_state_scraper_models()
    active_states = [
        code
        for code, item in states.items()
        if isinstance(item, dict) and clean_text(item.get("status")) == "active"
    ]
    LOGGER.info("Loaded %s active model states from %s", len(active_states), STATE_SCRAPER_MODELS_FILE)
    return sorted(active_states)


def load_portal_validation_sample_rows() -> List[Dict[str, str]]:
    states = load_state_scraper_models()
    selected: List[Dict[str, str]] = []
    for state in sorted(PORTAL_VALIDATION_SAMPLE_STATES):
        item = states.get(state, {})
        if not isinstance(item, dict) or clean_text(item.get("mode")) != "portal":
            continue
        samples = item.get("samples") or []
        if not samples:
            continue
        sample = samples[0]
        if not isinstance(sample, dict):
            continue
        selected.append(
            {
                "PID": clean_text(sample.get("pid")),
                "Daycare_Name": clean_text(sample.get("daycare_name")),
                "Mailing_City": clean_text(sample.get("city")),
                "Mailing_State": state,
            }
        )
    LOGGER.info("Loaded %s portal validation sample rows for states=%s", len(selected), sorted(PORTAL_VALIDATION_SAMPLE_STATES))
    return selected


def load_active_api_model_states() -> List[str]:
    states = load_state_scraper_models()
    active_states = [
        code
        for code, item in states.items()
        if isinstance(item, dict)
        and clean_text(item.get("status")) == "active"
        and clean_text(item.get("mode")) == "open_data_api"
    ]
    LOGGER.info("Loaded %s active API model states from %s", len(active_states), STATE_SCRAPER_MODELS_FILE)
    return sorted(active_states)


def get_effective_max_workers() -> int:
    return STATE_BATCH_MAX_WORKERS


def process_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    checkpoint_override = GOOGLE_CHECKPOINT_FILE if RUN_GOOGLE_QUERY_MODE else CHECKPOINT_FILE
    enricher = DaycareEnricher(checkpoint_file_override=checkpoint_override)
    output_rows: List[Dict[str, str]] = []
    work_rows = rows
    pid_to_index = {clean_text(row.get("PID")): index for index, row in enumerate(work_rows, start=1)}
    completed = 0
    found_count = 0
    total_to_process = len(work_rows)
    max_workers = get_effective_max_workers()
    LOGGER.info("Starting threaded processing for %s rows with %s workers", len(work_rows), max_workers)
    LOGGER.info(
        "Run counters initialized total_facilities=%s processed=%s found_data=%s",
        total_to_process,
        completed,
        found_count,
    )

    def run_google_side_pass() -> None:
        google_enricher = DaycareEnricher(checkpoint_file_override=GOOGLE_CHECKPOINT_FILE)
        google_adapter = GoogleSearchAdapter()
        try:
            for row in work_rows:
                pid = clean_text(row.get("PID"))
                cached_payload = google_enricher.get_checkpoint_row(pid)
                cached_row, _cached_sources = google_enricher.extract_checkpoint_payload(cached_payload)
                if cached_row and row_has_found_data(cached_row):
                    google_enricher.clear_google_miss(pid)
                    continue
                if google_enricher.is_google_miss(pid):
                    LOGGER.info("Skipping Google side pass for PID=%s because it is recorded in %s", pid, GOOGLE_MISS_FILE)
                    continue
                base_row = {header: clean_text(row.get(header, "")) for header in OUTPUT_HEADERS}
                apply_name_profile_to_row(base_row, get_record_name_profile(row))
                try:
                    values, sources = google_adapter.run(google_enricher, row)
                except Exception:
                    LOGGER.exception("Google side worker failed for PID=%s", pid)
                    values, sources = {}, {}
                for field, value in values.items():
                    base_row[field] = clean_text(value)
                if not row_has_found_data(base_row) and not base_row.get("Match_Status"):
                    base_row["Match_Status"] = "not_found"
                    base_row["Match_Confidence"] = "0"
                    base_row["Matched_Reason"] = "No datapoints were found from Google knowledge panel or top search results."
                if row_has_found_data(base_row):
                    google_enricher.clear_google_miss(pid)
                else:
                    google_enricher.mark_google_miss(pid)
                google_enricher.set_checkpoint_row(pid, base_row, sources)
                google_enricher.save_checkpoint()
            retry_rows = google_enricher.pop_google_antibot_retries()
            if retry_rows:
                LOGGER.info(
                    "Retrying %s Google anti-bot queued PIDs at end of Google side pass",
                    len(retry_rows),
                )
            for pid, row in retry_rows.items():
                cached_payload = google_enricher.get_checkpoint_row(pid)
                cached_row, _cached_sources = google_enricher.extract_checkpoint_payload(cached_payload)
                if cached_row and row_has_found_data(cached_row):
                    google_enricher.clear_google_miss(pid)
                    continue
                if google_enricher.is_google_miss(pid):
                    LOGGER.info("Skipping Google side retry for PID=%s because it is recorded in %s", pid, GOOGLE_MISS_FILE)
                    continue
                base_row = {header: clean_text(row.get(header, "")) for header in OUTPUT_HEADERS}
                apply_name_profile_to_row(base_row, get_record_name_profile(row))
                try:
                    values, sources = google_adapter.run(google_enricher, row)
                except Exception:
                    LOGGER.exception("Google side retry worker failed for PID=%s", pid)
                    values, sources = {}, {}
                for field, value in values.items():
                    base_row[field] = clean_text(value)
                if not row_has_found_data(base_row) and not base_row.get("Match_Status"):
                    base_row["Match_Status"] = "not_found"
                    base_row["Match_Confidence"] = "0"
                    base_row["Matched_Reason"] = "No datapoints were found from Google knowledge panel or top search results."
                if row_has_found_data(base_row):
                    google_enricher.clear_google_miss(pid)
                else:
                    google_enricher.mark_google_miss(pid)
                google_enricher.set_checkpoint_row(pid, base_row, sources)
                google_enricher.save_checkpoint()
            google_enricher.save_checkpoint()
            google_enricher.save_staging()
        finally:
            google_enricher.close()

    if RUN_GOOGLE_QUERY_MODE:
        google_adapter = GoogleSearchAdapter()
        try:
            for index, row in enumerate(work_rows, start=1):
                pid = clean_text(row.get("PID"))
                cached_payload = enricher.get_checkpoint_row(pid)
                cached_row, _cached_sources = enricher.extract_checkpoint_payload(cached_payload)
                if cached_row and row_has_found_data(cached_row):
                    enricher.clear_google_miss(pid)
                    output_rows.append(cached_row)
                    completed += 1
                    found_count += 1
                    continue
                base_row = {header: clean_text(row.get(header, "")) for header in OUTPUT_HEADERS}
                apply_name_profile_to_row(base_row, get_record_name_profile(row))
                if enricher.is_google_miss(pid):
                    LOGGER.info("Skipping Google query for PID=%s because it is recorded in %s", pid, GOOGLE_MISS_FILE)
                    base_row["Match_Status"] = "not_found"
                    base_row["Match_Confidence"] = "0"
                    base_row["Matched_Reason"] = "Skipped because this PID was previously recorded as a Google miss."
                    output_rows.append(base_row)
                    completed += 1
                    continue
                try:
                    values, sources = google_adapter.run(enricher, row)
                except Exception:
                    LOGGER.exception("Google adapter worker failed for PID=%s", row.get("PID", ""))
                    values, sources = {}, {}
                for field, value in values.items():
                    base_row[field] = clean_text(value)
                if not row_has_found_data(base_row) and not base_row.get("Match_Status"):
                    base_row["Match_Status"] = "not_found"
                    base_row["Match_Confidence"] = "0"
                    base_row["Matched_Reason"] = "No datapoints were found from Google knowledge panel or top search results."
                if row_has_found_data(base_row):
                    enricher.clear_google_miss(pid)
                else:
                    enricher.mark_google_miss(pid)
                enricher.set_checkpoint_row(pid, base_row, sources)
                enricher.set_staging_row(pid, base_row, sources)
                enricher.save_checkpoint()
                ordered_results = base_row
                output_rows.append(ordered_results)
                completed += 1
                if row_has_found_data(base_row):
                    found_count += 1
                if completed % 25 == 0 or completed == len(work_rows):
                    LOGGER.info(
                        "Google mode progress counters total_facilities=%s processed=%s found_data=%s remaining=%s",
                        total_to_process,
                        completed,
                        found_count,
                        total_to_process - completed,
                    )
                    print(f"Processed {completed}/{len(work_rows)} rows")
                    enricher.save_checkpoint()
            retry_rows = enricher.pop_google_antibot_retries()
            if retry_rows:
                LOGGER.info(
                    "Retrying %s Google anti-bot queued PIDs at end of Google-only pass",
                    len(retry_rows),
                )
            for pid, row in retry_rows.items():
                cached_payload = enricher.get_checkpoint_row(pid)
                cached_row, _cached_sources = enricher.extract_checkpoint_payload(cached_payload)
                if cached_row and row_has_found_data(cached_row):
                    enricher.clear_google_miss(pid)
                    continue
                if enricher.is_google_miss(pid):
                    LOGGER.info("Skipping Google retry for PID=%s because it is recorded in %s", pid, GOOGLE_MISS_FILE)
                    continue
                base_row = {header: clean_text(row.get(header, "")) for header in OUTPUT_HEADERS}
                apply_name_profile_to_row(base_row, get_record_name_profile(row))
                try:
                    values, sources = google_adapter.run(enricher, row)
                except Exception:
                    LOGGER.exception("Google adapter retry worker failed for PID=%s", row.get("PID", ""))
                    values, sources = {}, {}
                for field, value in values.items():
                    base_row[field] = clean_text(value)
                if not row_has_found_data(base_row) and not base_row.get("Match_Status"):
                    base_row["Match_Status"] = "not_found"
                    base_row["Match_Confidence"] = "0"
                    base_row["Matched_Reason"] = "No datapoints were found from Google knowledge panel or top search results."
                if row_has_found_data(base_row):
                    enricher.clear_google_miss(pid)
                else:
                    enricher.mark_google_miss(pid)
                enricher.set_checkpoint_row(pid, base_row, sources)
                enricher.set_staging_row(pid, base_row, sources)
                enricher.save_checkpoint()
            enricher.save_checkpoint()
            enricher.save_staging()
            LOGGER.info(
                "Completed Google adapter processing total_facilities=%s processed=%s found_data=%s",
                total_to_process,
                completed,
                found_count,
            )
            return output_rows
        finally:
            enricher.close()

    def process_state_batch(state_rows: List[Tuple[int, Dict[str, str]]]) -> List[Tuple[int, Dict[str, str]]]:
        state_results: List[Tuple[int, Dict[str, str]]] = []
        if not state_rows:
            return state_results
        batch_state = clean_text(state_rows[0][1].get("Mailing_State"))
        for index, row in state_rows:
            try:
                state_results.append((index, enricher.enrich_record(row)))
            except Exception:
                LOGGER.exception("Worker failed for row index=%s PID=%s", index, row.get("PID", ""))
                state_results.append((index, {header: clean_text(row.get(header, "")) for header in OUTPUT_HEADERS}))
        timeout_retry_rows = enricher.pop_adapter_timeout_retries(batch_state)
        if timeout_retry_rows:
            LOGGER.info("Retrying %s timeout-hit adapter rows at end of state batch state=%s", len(timeout_retry_rows), batch_state)
            retry_index_map = {clean_text(row.get("PID")): index for index, row in state_rows}
            for pid, row in timeout_retry_rows.items():
                index = retry_index_map.get(pid)
                if not index:
                    continue
                try:
                    retry_result = enricher.enrich_record(row)
                    for pos, (existing_index, _existing_result) in enumerate(state_results):
                        if existing_index == index:
                            state_results[pos] = (index, retry_result)
                            break
                except Exception:
                    LOGGER.exception("Timeout retry failed for state=%s PID=%s", batch_state, pid)
        return state_results

    try:
        google_future = None
        google_executor = ThreadPoolExecutor(max_workers=1)
        google_future = google_executor.submit(run_google_side_pass)
        ordered_results: Dict[int, Dict[str, str]] = {}
        if RUN_API_STATE_TEST_MODE or USE_STATE_PORTAL_ADAPTERS_ONLY:
            grouped_rows: Dict[str, List[Tuple[int, Dict[str, str]]]] = {}
            for index, row in enumerate(work_rows, start=1):
                grouped_rows.setdefault(clean_text(row.get("Mailing_State")), []).append((index, row))
            batch_label = "API-state" if RUN_API_STATE_TEST_MODE else "state-portal"
            LOGGER.info(
                "%s mode batching %s rows across %s states with one worker batch per state",
                batch_label,
                len(work_rows),
                len(grouped_rows),
            )
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_state = {
                    executor.submit(process_state_batch, state_rows): state
                    for state, state_rows in grouped_rows.items()
                }
                for future in as_completed(future_to_state):
                    state = future_to_state[future]
                    try:
                        state_results = future.result()
                    except Exception:
                        LOGGER.exception("State batch failed for state=%s", state)
                        state_results = []
                    for index, result in state_results:
                        ordered_results[index] = result
                        completed += 1
                        if row_has_found_data(result):
                            found_count += 1
                        if completed % 25 == 0 or completed == len(work_rows):
                            LOGGER.info(
                                "Progress counters total_facilities=%s processed=%s found_data=%s remaining=%s",
                                total_to_process,
                                completed,
                                found_count,
                                total_to_process - completed,
                            )
                            print(f"Processed {completed}/{len(work_rows)} rows")
                            enricher.save_checkpoint()
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_index = {
                    executor.submit(enricher.enrich_record, row): index for index, row in enumerate(work_rows, start=1)
                }
                for future in as_completed(future_to_index):
                    index = future_to_index[future]
                    try:
                        ordered_results[index] = future.result()
                    except Exception:
                        LOGGER.exception("Worker failed for row index=%s PID=%s", index, work_rows[index - 1].get("PID", ""))
                        ordered_results[index] = {header: clean_text(work_rows[index - 1].get(header, "")) for header in OUTPUT_HEADERS}
                    completed += 1
                    if row_has_found_data(ordered_results[index]):
                        found_count += 1
                    if completed % 25 == 0 or completed == len(work_rows):
                        LOGGER.info(
                            "Progress counters total_facilities=%s processed=%s found_data=%s remaining=%s",
                            total_to_process,
                            completed,
                            found_count,
                            total_to_process - completed,
                        )
                        print(f"Processed {completed}/{len(work_rows)} rows")
                        enricher.save_checkpoint()

        pending_retry_states = []
        with enricher.api_retry_lock:
            pending_retry_states = list(enricher.pending_api_city_only_retries.keys())
        for state in pending_retry_states:
            api = API_REGISTRY.get(state)
            if not api or not api.supports_post_run_city_retry():
                continue
            pending_city_retries = enricher.pop_api_city_only_retries(state)
            if not pending_city_retries:
                continue
            retry_count = sum(len(city_records) for city_records in pending_city_retries.values())
            LOGGER.info(
                "Starting post-run %s city-only retry for %s PIDs across %s cities",
                state,
                retry_count,
                len(pending_city_retries),
            )
            enricher.api_city_only_retry_active_states.add(state)
            try:
                for city_key, city_records in pending_city_retries.items():
                    unresolved_records = {}
                    for pid, retry_record in city_records.items():
                        index = pid_to_index.get(pid)
                        if not index:
                            continue
                        previous_row = ordered_results.get(index, {})
                        if row_has_found_data(previous_row):
                            continue
                        unresolved_records[pid] = retry_record
                    if not unresolved_records:
                        continue
                    try:
                        resolved = api.run_city_retry(enricher, city_key, unresolved_records)
                    except Exception:
                        LOGGER.exception("%s city-based retry failed for city=%s", state, city_key)
                        continue
                    for pid, (values, _sources) in resolved.items():
                        index = pid_to_index.get(pid)
                        if not index:
                            continue
                        retry_record = unresolved_records[pid]
                        merged_row = {header: clean_text(retry_record.get(header, "")) for header in OUTPUT_HEADERS}
                        apply_name_profile_to_row(merged_row, get_record_name_profile(retry_record))
                        for field, value in values.items():
                            merged_row[field] = clean_text(value)
                        merged_row["Telephone"] = normalize_phone(merged_row.get("Telephone", ""))
                        merged_row["Mailing_Zip"] = normalize_zip(merged_row.get("Mailing_Zip", ""))
                        merged_row["URL"] = normalize_url(merged_row.get("URL", ""))
                        previously_found = row_has_found_data(ordered_results.get(index, {}))
                        ordered_results[index] = merged_row
                        if row_has_found_data(ordered_results[index]) and not previously_found:
                            found_count += 1
                            LOGGER.info("%s city-only retry succeeded for PID=%s city=%s", state, pid, city_key)
            finally:
                enricher.api_city_only_retry_active_states.discard(state)

        for index, row in enumerate(work_rows, start=1):
            current_result = ordered_results.get(index, {})
            if row_has_found_data(current_result):
                continue
            enricher.queue_winnie_retry(row)

        pending_winnie_states = []
        with enricher.winnie_retry_lock:
            pending_winnie_states = list(enricher.pending_winnie_retries.keys())
        if pending_winnie_states:
            winnie_adapter = WinnieFallbackAdapter()
            with enricher.winnie_run_lock:
                previous_winnie_state = ""
                for state in pending_winnie_states:
                    if previous_winnie_state and previous_winnie_state != state:
                        LOGGER.info(
                            "Winnie fallback switching state from %s to %s; sleeping 30 seconds",
                            previous_winnie_state,
                            state,
                        )
                        time.sleep(30.0)
                    pending_city_retries = enricher.pop_winnie_retries(state)
                    if not pending_city_retries:
                        previous_winnie_state = state
                        continue
                    retry_count = sum(len(city_records) for city_records in pending_city_retries.values())
                    LOGGER.info(
                        "Starting Winnie fallback for %s PIDs across %s cities in state=%s",
                        retry_count,
                        len(pending_city_retries),
                        state,
                    )
                    for city_key, city_records in pending_city_retries.items():
                        unresolved_records = {}
                        for pid, retry_record in city_records.items():
                            index = pid_to_index.get(pid)
                            if not index:
                                continue
                            previous_row = ordered_results.get(index, {})
                            if row_has_found_data(previous_row):
                                continue
                            unresolved_records[pid] = retry_record
                        if not unresolved_records:
                            continue
                        try:
                            resolved = winnie_adapter.run_city_retry(enricher, state, city_key, unresolved_records)
                        except Exception:
                            LOGGER.exception("Winnie fallback failed for state=%s city=%s", state, city_key)
                            continue
                        for pid, (values, _sources) in resolved.items():
                            index = pid_to_index.get(pid)
                            if not index:
                                continue
                            retry_record = unresolved_records[pid]
                            merged_row = {header: clean_text(retry_record.get(header, "")) for header in OUTPUT_HEADERS}
                            apply_name_profile_to_row(merged_row, get_record_name_profile(retry_record))
                            for field, value in values.items():
                                merged_row[field] = clean_text(value)
                            merged_row["Telephone"] = normalize_phone(merged_row.get("Telephone", ""))
                            merged_row["Mailing_Zip"] = normalize_zip(merged_row.get("Mailing_Zip", ""))
                            merged_row["URL"] = normalize_url(merged_row.get("URL", ""))
                            previously_found = row_has_found_data(ordered_results.get(index, {}))
                            ordered_results[index] = merged_row
                            enricher.set_checkpoint_row(pid, merged_row, _sources)
                            enricher.set_staging_row(pid, merged_row, _sources)
                            if row_has_found_data(ordered_results[index]) and not previously_found:
                                found_count += 1
                                LOGGER.info("Winnie fallback succeeded for PID=%s state=%s city=%s", pid, state, city_key)
                    previous_winnie_state = state

        for index in range(1, len(work_rows) + 1):
            output_rows.append(ordered_results[index])
        enricher.save_checkpoint()
        enricher.save_staging()
        if google_future:
            google_future.result()
        google_executor.shutdown(wait=True)
        LOGGER.info(
            "Completed processing all rows total_facilities=%s processed=%s found_data=%s",
            total_to_process,
            completed,
            found_count,
        )
        return output_rows
    finally:
        try:
            if 'google_future' in locals() and google_future:
                google_future.result()
        except Exception:
            LOGGER.exception("Google side worker failed during general run")
        try:
            if 'google_executor' in locals():
                google_executor.shutdown(wait=True)
        except Exception:
            pass
        enricher.close()


def summarize(rows: List[Dict[str, str]]) -> None:
    populated_counts = Counter()
    found_count = 0
    for row in rows:
        if row_has_found_data(row):
            found_count += 1
        for field in ENRICHMENT_VALUE_FIELDS:
            if clean_text(row.get(field)):
                populated_counts[field] += 1
    LOGGER.info(
        "Run summary total_facilities=%s processed=%s found_data=%s populated_fields=%s",
        len(rows),
        len(rows),
        found_count,
        dict(populated_counts),
    )
    print("Filled field counts:")
    for field in ENRICHMENT_VALUE_FIELDS:
        print(f"  {field}: {populated_counts[field]}")


def prepare_cleaned_input() -> str:
    LOGGER.info("Preparing cleaned input CSV using clean_daycare_names.py helpers")
    raw_rows = name_cleaner.read_rows(INPUT_CSV)
    cleaned_rows = name_cleaner.clean_rows(raw_rows)
    name_cleaner.write_rows(CLEANED_INPUT_CSV, cleaned_rows)
    LOGGER.info("Prepared cleaned input CSV at %s with %s rows", CLEANED_INPUT_CSV, len(cleaned_rows))
    return CLEANED_INPUT_CSV


def main() -> None:
    global SINGLE_PID_FILTER, ADAPTER_ONLY_TEST_STATES, FORCE_HEADED, RUN_GOOGLE_QUERY_MODE

    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--pid", dest="pid", default="", help="Run only the specified PID.")
    parser.add_argument("--states", dest="states", default="", help="Comma-separated state list, for example CA,MA,VA.")
    parser.add_argument("--headed", dest="headed", action="store_true", help="Run browsers in headed mode for testing.")
    parser.add_argument("--google", dest="google", action="store_true", help="Run Google adapter mode on the selected rows.")
    args = parser.parse_args()

    if clean_text(args.pid):
        SINGLE_PID_FILTER = clean_text(args.pid)
    if clean_text(args.states):
        ADAPTER_ONLY_TEST_STATES = {
            clean_text(state).upper()
            for state in args.states.split(",")
            if clean_text(state)
        }
    if args.headed:
        FORCE_HEADED = True
    if args.google:
        RUN_GOOGLE_QUERY_MODE = True

    LOGGER.info("Starting daycare enrichment run")
    LOGGER.info(
        "CLI overrides active pid=%s states=%s google_mode=%s",
        SINGLE_PID_FILTER,
        sorted(ADAPTER_ONLY_TEST_STATES) if ADAPTER_ONLY_TEST_STATES else [],
        RUN_GOOGLE_QUERY_MODE,
    )
    cleaned_input_path = prepare_cleaned_input()
    rows = read_rows(cleaned_input_path)
    rows = select_rows_for_run(rows)
    enriched_rows = process_rows(rows)
    output_path = get_output_path()
    write_rows(output_path, enriched_rows)
    summarize(enriched_rows)
    LOGGER.info("Run completed successfully; output file=%s", output_path)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()

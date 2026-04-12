import csv
import json
import logging
import os
import random
import re
import shutil
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
from proxy_pool import ProxyPool

ADAPTER_ONLY_TEST_STATES = {"NH"}
SINGLE_PID_FILTER = "2581751"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CHROME_PROFILE_DIR = os.path.join(BASE_DIR, "chrome_profiles", "google_search")

INPUT_CSV = os.path.join(BASE_DIR, "DaycareBuildings_Input(in).csv")
CLEANED_INPUT_CSV = os.path.join(OUTPUT_DIR, "DaycareBuildings_Cleaned.csv")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched.csv")
SAMPLE_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_sample50.csv")
STAGING_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_staging.csv")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "enrichment.json")
STAGING_DIR = os.path.join(BASE_DIR, "enrichment_staging")
LEGACY_STAGING_FILE = os.path.join(BASE_DIR, "enrichment_staging.json")
LEGACY_CHECKPOINT_FILES = (
    os.path.join(BASE_DIR, "enrichment_checkpoint.json"),
    os.path.join(OUTPUT_DIR, "enrichment_checkpoint.json"),
)
LOG_FILE = os.path.join(LOG_DIR, "enrichment.log")
STATE_SCRAPER_MODELS_FILE = os.path.join(BASE_DIR, "state_scraper_models.json")
SEARCH_ENGINE_URL = "https://search.brave.com/search"
GOOGLE_SEARCH_URL = "https://www.google.com/search"
BING_SEARCH_URL = "https://www.bing.com/search"
YAHOO_SEARCH_URL = "https://search.yahoo.com/search"
CHECKPOINT_SCHEMA_VERSION = 3
RUN_VALIDATION_SAMPLE = False
VALIDATION_SAMPLE_SIZE = 100
VALIDATION_RANDOM_SEED = 42
VALIDATION_STATE_FILTER = ""
PORTAL_VALIDATION_SAMPLE_ONLY = False
PORTAL_VALIDATION_SAMPLE_STATES = {"IL", "VA"}
PORTAL_VALIDATION_ALL_ROWS_STATES = set()
RUN_MODEL_STATE_STAGING = False
MODEL_STATE_SAMPLE_MIN = 2
MODEL_STATE_SAMPLE_MAX = 3
USE_STATE_PORTAL_ADAPTERS_ONLY = True
CSV_CLEANING_ONLY_MODE = False
RUN_API_STATE_TEST_MODE = False
RUN_GOOGLE_ONLY_SAMPLE_MODE = False
ENABLE_GOOGLE_FALLBACK_FOR_API_MISSES = False
GOOGLE_API_MISS_SAMPLE_LIMIT = 100
GOOGLE_SEARCH_RETRIES = 1

OUTPUT_HEADERS = [
    "PID",
    "DayCareType",
    "Daycare_Name",
    "Original_Name",
    "Normalized_Name",
    "Search_Name_Primary",
    "Search_Name_Variants",
    "Mailing_City",
    "Mailing_State",
    "Mailing_Address",
    "Mailing_Zip",
    "Telephone",
    "URL",
    "Capacity (optional)",
    "Age Range (optional)",
    "Match_Status",
    "Match_Confidence",
    "Matched_Provider_Name",
    "Matched_Reason",
]

ENRICHMENT_VALUE_FIELDS = [
    "Mailing_Address",
    "Mailing_Zip",
    "Telephone",
    "URL",
    "Capacity (optional)",
    "Age Range (optional)",
]

USER_AGENT_POOL = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
]
USER_AGENT = USER_AGENT_POOL[0]
ACCEPT_LANGUAGE_POOL = ["en-US,en;q=0.9", "en-US,en;q=0.8", "en-GB,en-US;q=0.9,en;q=0.8"]
HEADER_ACCEPT_POOL = [
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
]

REQUEST_TIMEOUT = 25
DEFAULT_MAX_WORKERS = 1
API_TEST_MAX_WORKERS = 8
SEARCH_RESULTS_LIMIT = 8
CONTACT_PAGE_LIMIT = 2
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 3
SEARCH_RETRIES = 4
FETCH_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3.0
SEARCH_MIN_DELAY_SECONDS = 6.0
HTTP_MIN_DELAY_SECONDS = 0.0
ENABLE_TRUSTED_PUBLIC_SEARCH = True
RATE_LIMIT_COOLDOWN_SECONDS = 90
SELENIUM_PAGELOAD_TIMEOUT = 45
SELENIUM_WAIT_TIMEOUT = 20
CHROME_BINARY_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
GOOGLE_FALLBACK_MAX_CONCURRENT = 1
GOOGLE_USE_PERSISTENT_PROFILE = False
GOOGLE_USE_HEADLESS = True
GOOGLE_HOME_URL = "https://www.google.com/"
GOOGLE_SEARCH_MIN_DELAY_SECONDS = 0.75
GOOGLE_SEARCH_TOTAL_TIMEOUT_SECONDS = 5.0
ENABLE_PROXY_POOL = False
PROXY_LIST_CSV = r"C:\Users\deepa\Downloads\Free_Proxy_List.csv"
VALIDATED_PROXY_JSON = os.path.join(OUTPUT_DIR, "validated_proxies.json")

STATE_PORTAL_URLS = {
    "TX": "https://childcare.hhs.texas.gov",
    "CA": "https://mychildcareplan.org/provider-search/",
    "NY": "https://ocfs.ny.gov/programs/childcare/looking/",
    "FL": "https://caressearch.myflfamilies.com/PublicSearch",
    "MA": "https://childcare.mass.gov/findchildcare",
}
GENERIC_OPEN_DATA_API_STATES = {"CO", "DE", "PA", "UT", "WA"}
TEXAS_PROVIDER_SEARCH_API_URL = "https://data.texas.gov/resource/bc5r-88dy.json"
TEXAS_PROVIDER_DETAIL_URL_TEMPLATE = "https://childcare.hhs.texas.gov/Public/Operation?operationId={provider_id}"
CALIFORNIA_PROVIDER_SEARCH_API_URL = "https://data.ca.gov/api/3/action/datastore_search_sql"
CONNECTICUT_PROVIDER_SEARCH_API_URL = "https://data.ct.gov/resource/h8mr-dn95.json"

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
    def __init__(self, min_delay_seconds: float = 1.0, proxy_pool: Optional[ProxyPool] = None) -> None:
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
        self.proxy_pool = proxy_pool

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        proxy = None
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay_seconds:
                time.sleep(self.min_delay_seconds - elapsed + random.uniform(0.05, 0.25))
            merged_headers = dict(build_random_request_headers())
            if isinstance(kwargs.get("headers"), dict):
                merged_headers.update(kwargs["headers"])
            kwargs["headers"] = merged_headers
            if self.proxy_pool and not kwargs.get("proxies"):
                proxy = self.proxy_pool.get_next_proxy()
                if proxy:
                    kwargs["proxies"] = proxy.to_requests_proxies()
                    LOGGER.info("Routing %s request to %s through proxy=%s", method.upper(), url, proxy.key)
            try:
                response = self.session.request(method=method.upper(), url=url, timeout=REQUEST_TIMEOUT, **kwargs)
                self.last_request_time = time.time()
                if proxy:
                    self.proxy_pool.mark_success(proxy.key)
            except Exception as exc:
                if proxy:
                    self.proxy_pool.mark_failure(proxy.key, str(exc))
                raise
        response.raise_for_status()
        return response

    def get(self, url: str, **kwargs) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        return self.request("POST", url, **kwargs)


class DaycareEnricher:
    def __init__(self) -> None:
        self.proxy_pool = self.load_proxy_pool()
        self.session = RateLimitedSession(min_delay_seconds=HTTP_MIN_DELAY_SECONDS, proxy_pool=self.proxy_pool)
        self.checkpoint_lock = threading.Lock()
        self.search_lock = threading.Lock()
        self.last_search_time = 0.0
        self.google_fallback_lock = threading.Lock()
        self.google_fallback_attempts = 0
        self.google_fallback_semaphore = threading.Semaphore(GOOGLE_FALLBACK_MAX_CONCURRENT)
        self.state_scraper_models = load_state_scraper_models()
        self.texas_api_token = clean_text(os.getenv("TEXAS_API_BEARER_TOKEN"))
        self.driver_local = threading.local()
        self.driver_lock = threading.Lock()
        self.driver_registry: List[webdriver.Chrome] = []
        self.state_portal_drivers: Dict[str, webdriver.Chrome] = {}
        self.state_portal_base_handles: Dict[str, str] = {}
        self.state_portal_query_handles: Dict[str, str] = {}
        self.state_portal_session_flags: Dict[str, Dict[str, object]] = {}
        self.temp_profile_dirs: List[str] = []
        self.staging_lock = threading.Lock()
        self.checkpoint = self.load_checkpoint()
        self.staging = self.load_staging()
        LOGGER.info(
            "DaycareEnricher initialized with %s checkpoint rows, %s staging rows, %s state scraper models and proxy_pool=%s",
            len(self.checkpoint),
            len(self.staging),
            len(self.state_scraper_models),
            bool(self.proxy_pool),
        )

    def load_proxy_pool(self) -> Optional[ProxyPool]:
        if not ENABLE_PROXY_POOL:
            LOGGER.info("Proxy pool disabled")
            return None
        try:
            if os.path.exists(VALIDATED_PROXY_JSON):
                pool = ProxyPool.from_validated_json(VALIDATED_PROXY_JSON)
                LOGGER.info("Loaded validated proxy pool from %s stats=%s", VALIDATED_PROXY_JSON, pool.stats())
                return pool
            if os.path.exists(PROXY_LIST_CSV):
                pool = ProxyPool.from_csv(
                    PROXY_LIST_CSV,
                    allowed_protocols=["http", "https"],
                    allowed_anonymity=["elite", "anonymous"],
                    min_uptime=90,
                    max_latency=5000,
                    http_only_for_requests=True,
                )
                LOGGER.info("Loaded raw proxy pool from %s stats=%s", PROXY_LIST_CSV, pool.stats())
                return pool
        except Exception:
            LOGGER.exception("Failed loading proxy pool")
        return None

    def get_browser_profile(self) -> Dict[str, str]:
        profile = getattr(self.driver_local, "browser_profile", None)
        if profile:
            return profile
        profile = build_random_browser_profile()
        self.driver_local.browser_profile = profile
        return profile

    def load_checkpoint(self) -> Dict[str, Dict[str, str]]:
        checkpoint_path = CHECKPOINT_FILE
        if not os.path.exists(checkpoint_path):
            for legacy_path in LEGACY_CHECKPOINT_FILES:
                if os.path.exists(legacy_path):
                    checkpoint_path = legacy_path
                    LOGGER.info("Primary checkpoint missing; loading legacy checkpoint from %s", legacy_path)
                    break
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
        rows: Dict[str, Dict[str, str]] = {}
        if os.path.isdir(STAGING_DIR):
            for entry in sorted(os.listdir(STAGING_DIR)):
                if not entry.lower().endswith(".json"):
                    continue
                path = os.path.join(STAGING_DIR, entry)
                LOGGER.debug("Loading staging shard from %s", path)
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        data = json.load(handle)
                    if (
                        isinstance(data, dict)
                        and data.get("_meta", {}).get("schema_version") == CHECKPOINT_SCHEMA_VERSION
                        and isinstance(data.get("rows"), dict)
                    ):
                        for pid, payload in data["rows"].items():
                            if (
                                isinstance(payload, dict)
                                and isinstance(payload.get("row"), dict)
                                and has_fetched_enrichment(payload.get("row", {}), payload.get("sources", {}))
                            ):
                                rows[pid] = payload
                    else:
                        LOGGER.warning("Ignoring staging shard with unexpected schema: %s", path)
                except Exception:
                    LOGGER.exception("Failed loading staging shard %s", path)
        elif os.path.exists(LEGACY_STAGING_FILE):
            LOGGER.debug("Loading legacy staging file from %s", LEGACY_STAGING_FILE)
            try:
                with open(LEGACY_STAGING_FILE, "r", encoding="utf-8") as handle:
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
                else:
                    LOGGER.warning("Legacy staging file did not contain the expected schema; ignoring it")
            except Exception:
                LOGGER.exception("Failed to load legacy staging; starting with empty staging state")
        else:
            LOGGER.info("No staging directory found at startup")
            return {}
        LOGGER.info("Loaded staging with %s rows across %s shard files", len(rows), len(os.listdir(STAGING_DIR)) if os.path.isdir(STAGING_DIR) else (1 if rows else 0))
        return rows

    def save_checkpoint(self) -> None:
        with self.checkpoint_lock:
            snapshot = {
                pid: dict(payload)
                for pid, payload in self.checkpoint.items()
                if isinstance(payload, dict)
                and isinstance(payload.get("row"), dict)
                and has_fetched_enrichment(payload.get("row", {}), payload.get("sources", {}))
            }
            temp_path = f"{CHECKPOINT_FILE}.tmp"
            LOGGER.debug("Saving checkpoint with %s rows to %s", len(snapshot), temp_path)
            with open(temp_path, "w", encoding="utf-8") as handle:
                json.dump(
                    {"_meta": {"schema_version": CHECKPOINT_SCHEMA_VERSION}, "rows": snapshot},
                    handle,
                    ensure_ascii=False,
                    indent=2,
                )
            os.replace(temp_path, CHECKPOINT_FILE)
            LOGGER.info("Checkpoint saved with %s rows", len(snapshot))

    def save_staging(self) -> None:
        with self.staging_lock:
            os.makedirs(STAGING_DIR, exist_ok=True)
            shard_snapshots: Dict[str, Dict[str, Dict[str, str]]] = {}
            for pid, payload in self.staging.items():
                if not (
                    isinstance(payload, dict)
                    and isinstance(payload.get("row"), dict)
                    and has_fetched_enrichment(payload.get("row", {}), payload.get("sources", {}))
                ):
                    continue
                state = clean_text(payload.get("row", {}).get("Mailing_State")).upper() or "UNKNOWN"
                shard_snapshots.setdefault(state, {})[pid] = dict(payload)

            existing_files = {
                entry
                for entry in os.listdir(STAGING_DIR)
                if entry.lower().endswith(".json")
            } if os.path.isdir(STAGING_DIR) else set()
            target_files = {f"{state}.json" for state in shard_snapshots}

            for state, snapshot in shard_snapshots.items():
                path = os.path.join(STAGING_DIR, f"{state}.json")
                temp_path = f"{path}.tmp"
                LOGGER.debug("Saving staging shard state=%s rows=%s to %s", state, len(snapshot), temp_path)
                with open(temp_path, "w", encoding="utf-8") as handle:
                    json.dump(
                        {"_meta": {"schema_version": CHECKPOINT_SCHEMA_VERSION, "state": state}, "rows": snapshot},
                        handle,
                        ensure_ascii=False,
                        indent=2,
                    )
                os.replace(temp_path, path)

            for stale_file in existing_files - target_files:
                stale_path = os.path.join(STAGING_DIR, stale_file)
                try:
                    os.remove(stale_path)
                except FileNotFoundError:
                    pass
                except Exception:
                    LOGGER.exception("Failed removing stale staging shard %s", stale_path)

            if os.path.exists(LEGACY_STAGING_FILE):
                try:
                    os.remove(LEGACY_STAGING_FILE)
                except Exception:
                    LOGGER.exception("Failed removing legacy staging file %s", LEGACY_STAGING_FILE)

            LOGGER.info("Staging saved with %s rows across %s state shard files", len(self.staging), len(shard_snapshots))

    def get_checkpoint_row(self, pid: str) -> Optional[Dict[str, str]]:
        with self.checkpoint_lock:
            value = self.checkpoint.get(pid)
            if value:
                LOGGER.debug("Checkpoint hit for PID=%s", pid)
            return dict(value) if value else None

    def get_staging_row(self, pid: str) -> Optional[Dict[str, str]]:
        with self.staging_lock:
            value = self.staging.get(pid)
            if value:
                LOGGER.debug("Staging hit for PID=%s", pid)
            return dict(value) if value else None

    def set_checkpoint_row(self, pid: str, row: Dict[str, str], sources: Optional[Dict[str, Dict[str, str]]] = None) -> None:
        with self.checkpoint_lock:
            if not has_fetched_enrichment(row, sources):
                if pid in self.checkpoint:
                    del self.checkpoint[pid]
                    LOGGER.debug("Checkpoint row removed for PID=%s because no fetched data was found", pid)
                else:
                    LOGGER.debug("Checkpoint row skipped for PID=%s because no fetched data was found", pid)
                return
            self.checkpoint[pid] = {
                "row": dict(row),
                "sources": {key: dict(value) for key, value in (sources or {}).items()},
            }
            LOGGER.debug("Checkpoint updated for PID=%s", pid)

    def set_staging_row(self, pid: str, row: Dict[str, str], sources: Optional[Dict[str, Dict[str, str]]] = None) -> None:
        with self.staging_lock:
            if not has_fetched_enrichment(row, sources):
                if pid in self.staging:
                    del self.staging[pid]
                    LOGGER.debug("Staging row removed for PID=%s because no fetched data was found", pid)
                else:
                    LOGGER.debug("Staging row skipped for PID=%s because no fetched data was found", pid)
                return
            self.staging[pid] = {
                "row": dict(row),
                "sources": {key: dict(value) for key, value in (sources or {}).items()},
            }
            LOGGER.debug("Staging updated for PID=%s", pid)

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
        if GOOGLE_USE_HEADLESS:
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
                GOOGLE_USE_HEADLESS,
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
        with self.driver_lock:
            drivers = list(self.driver_registry)
            self.driver_registry.clear()
        for driver in state_drivers + drivers:
            try:
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

    def enrich_from_state_portal(self, record: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        state = clean_text(record.get("Mailing_State"))
        if state == "TX":
            return self.enrich_from_texas_portal(record)
        if state == "CA":
            return self.enrich_from_california_dataset(record)
        if state == "CT":
            return self.enrich_from_connecticut_dataset(record)
        if state == "AZ":
            return self.enrich_from_arizona_portal(record)
        if state == "MD":
            return self.enrich_from_maryland_portal(record)
        if state == "MI":
            return self.enrich_from_michigan_portal(record)
        if state == "MN":
            return self.enrich_from_minnesota_portal(record)
        if state == "NH":
            return self.enrich_from_new_hampshire_portal(record)
        if state == "SC":
            return self.enrich_from_south_carolina_portal(record)
        if state == "IL":
            return self.enrich_from_illinois_portal(record)
        if state == "NC":
            return self.enrich_from_north_carolina_portal(record)
        if state == "NJ":
            return self.enrich_from_new_jersey_portal(record)
        if state == "OK":
            return self.enrich_from_oklahoma_portal(record)
        if state == "MA":
            return self.enrich_from_massachusetts_portal(record)
        if state == "PA":
            return self.enrich_from_pennsylvania_dataset(record)
        if state == "VA":
            return self.enrich_from_virginia_portal(record)
        if state in GENERIC_OPEN_DATA_API_STATES:
            return self.enrich_from_generic_open_data_api(record)
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

    def build_open_data_query(self, state: str, model: Dict[str, object], name_variant: str, city: str) -> Tuple[str, Dict[str, str]]:
        query_template = clean_text(str(model.get("query_template", "")))
        api_type = clean_text(str(model.get("api_type", "")))
        if not query_template:
            return "", {}
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
            if any(provider_simple and variant and (variant in provider_simple or provider_simple in variant) for variant in variants):
                filtered.append(item)
        return filtered

    def search_generic_open_data_api(self, record: Dict[str, str]) -> List[Dict[str, object]]:
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
            request_kind, params = self.build_open_data_query(state, model, profile.search_name_primary, city)
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
            )
            LOGGER.info("Generic API search for state=%s PID=%s produced %s candidates", state, record.get("PID", ""), len(rows))
            return rows
        for variant in profile.search_name_variants:
            request_kind, params = self.build_open_data_query(state, model, variant, city)
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

    def search_texas_portal_api(self, record: Dict[str, str]) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        provider_name = profile.search_name_primary
        city = clean_text(record.get("Mailing_City"))
        provider_variants = profile.search_name_variants
        seen_provider_ids = set()
        candidates: List[Dict[str, object]] = []

        for variant in provider_variants:
            escaped_variant = variant.replace('"', '""')
            escaped_city = city.replace('"', '""')
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

    def search_california_dataset(self, record: Dict[str, str]) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        provider_variants = profile.search_name_variants
        city_variants = build_city_search_variants(city)
        candidates: List[Dict[str, object]] = []
        seen_ids = set()
        for variant in provider_variants:
            for city_variant in city_variants:
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

    def search_connecticut_dataset(self, record: Dict[str, str]) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        provider_name = profile.search_name_primary
        city = clean_text(record.get("Mailing_City"))
        provider_variants = profile.search_name_variants
        candidates: List[Dict[str, object]] = []
        seen_ids = set()
        for variant in provider_variants:
            escaped_variant = variant.replace("'", "''")
            escaped_city = city.replace("'", "''")
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

    def search_pennsylvania_dataset(self, record: Dict[str, str]) -> List[Dict[str, object]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        candidates: List[Dict[str, object]] = []
        seen_ids = set()
        for variant in profile.search_name_variants[:4]:
            token = pick_best_name_token(variant)
            if not token:
                continue
            escaped_token = token.replace("'", "''")
            escaped_city = city.replace("'", "''")
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

    def build_headless_portal_driver(self) -> webdriver.Chrome:
        last_error: Optional[Exception] = None
        for attempt in range(1, 4):
            profile_dir = tempfile.mkdtemp(prefix="portal_driver_", dir=os.path.join(BASE_DIR, "chrome_profiles"))
            self.temp_profile_dirs.append(profile_dir)
            options = ChromeOptions()
            options.binary_location = CHROME_BINARY_PATH
            # options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--remote-debugging-pipe")
            options.add_argument("--window-size=1440,1400")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-background-networking")
            options.add_argument("--disable-extensions")
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
            try:
                LOGGER.info("Starting headless portal Chrome attempt=%s profile_dir=%s", attempt, profile_dir)
                driver = webdriver.Chrome(options=options)
                driver.set_page_load_timeout(SELENIUM_PAGELOAD_TIMEOUT)
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
        raise RuntimeError("Failed to start headless portal Chrome for unknown reason")

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
        driver = self.build_headless_portal_driver()
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
            driver.execute_script("window.open(arguments[0], '_blank');", url)
            WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
            new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
            driver.switch_to.window(new_handles[-1] if new_handles else driver.window_handles[-1])
            return driver
        except Exception:
            self.reset_state_portal_driver(state)
            raise

    def open_or_reuse_state_portal_query_tab(
        self,
        state: str,
        url: str,
        ready_locator: Optional[Tuple[str, str]] = None,
    ) -> webdriver.Chrome:
        state = clean_text(state).upper()
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
                driver.execute_script("window.open(arguments[0], '_blank');", url)
                WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
                new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
                query_handle = new_handles[-1] if new_handles else driver.window_handles[-1]
                driver.switch_to.window(query_handle)
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
            raise

    def finalize_state_portal_query(self, state: str, home_url: str = "") -> None:
        state = clean_text(state).upper()
        driver = self.get_state_portal_driver(state)
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
            if home_url:
                driver.get(home_url)
        except Exception:
            LOGGER.info("Failed finalizing %s query tab cleanly; resetting shared portal driver", state)
            self.reset_state_portal_driver(state)

    def search_illinois_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        home_url = "https://sunshine.dcfs.illinois.gov/Content/Licensing/Daycare/ProviderLookup.aspx"
        try:
            for variant in profile.search_name_variants:
                driver = self.open_state_portal_query_tab("IL", home_url)
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolderContent_ASPxProviderName_I"))
                )
                provider_input = driver.find_element(By.ID, "ctl00_ContentPlaceHolderContent_ASPxProviderName_I")
                city_input = driver.find_element(By.ID, "ctl00_ContentPlaceHolderContent_ASPxCity_I")
                provider_input.clear()
                provider_input.send_keys(variant)
                city_input.clear()
                city_input.send_keys(city)
                LOGGER.info(
                    "Illinois portal searching PID=%s with provider_variant=%s city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                )
                driver.execute_script(
                    """
                    if (typeof dcfssearch === 'function') {
                        dcfssearch();
                    } else {
                        const btn = document.getElementById('ctl00_ContentPlaceHolderContent_ASPxSearch_I');
                        if (btn) { btn.click(); }
                    }
                    """
                )
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolderContent_ASPxGridView1"))
                )
                time.sleep(1.0)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.select_one("#ctl00_ContentPlaceHolderContent_ASPxGridView1_DXMainTable")
                if not table:
                    continue
                rows = table.select("tr.dxgvDataRow, tr.dxgvDataRowAlt")
                if not rows:
                    rows = [row for row in table.select("tr") if row.select("td.dxgv, td.dxgvFixedColumn")]
                results: List[Dict[str, str]] = []
                for row in rows:
                    cells = [
                        clean_text(cell.get_text(" ", strip=True))
                        for cell in row.select("td.dxgv, td.dxgvFixedColumn")
                    ]
                    if len(cells) < 12:
                        continue
                    if cells[0] in {"Doing Business as", "Street", "City", "County", "Zip", "Phone"}:
                        continue
                    candidate = {
                        "provider_name": cells[0],
                        "address": cells[1],
                        "city": cells[2],
                        "zip": cells[4],
                        "phone": cells[5],
                        "age_range": cells[8],
                        "capacity": cells[10],
                    }
                    results.append(candidate)
                    LOGGER.info("Illinois portal candidate PID=%s variant=%s data=%s", record.get("PID", ""), variant, candidate)
                if results:
                    LOGGER.info(
                        "Illinois portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    self.finalize_state_portal_query("IL", home_url)
                    return results
                self.finalize_state_portal_query("IL", home_url)
            LOGGER.info("Illinois portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("IL")
            raise

    def enrich_from_illinois_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_illinois_portal(record)
        except Exception:
            LOGGER.exception("Illinois portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        best_candidate = None
        best_score = -999
        for candidate in candidates:
            overlap = token_overlap_score(record.get("Daycare_Name", ""), candidate.get("provider_name", ""))
            city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(candidate.get("city")).lower()
            score = overlap * 4
            if city_match:
                score += 3
            if candidate.get("capacity"):
                score += 1
            LOGGER.info(
                "Illinois portal candidate scored %s for PID=%s provider=%s overlap=%.3f city_match=%s",
                score,
                record.get("PID", ""),
                candidate.get("provider_name", ""),
                overlap,
                city_match,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
        if not best_candidate:
            return {}, {}
        best_city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(best_candidate.get("city")).lower()
        best_overlap = token_overlap_score(record.get("Daycare_Name", ""), best_candidate.get("provider_name", ""))
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            LOGGER.warning(
                "Illinois portal rejected best candidate for PID=%s provider=%s score=%s overlap=%.3f city_match=%s",
                record.get("PID", ""),
                best_candidate.get("provider_name", ""),
                best_score,
                best_overlap,
                best_city_match,
            )
            return {}, {}
        address_value = ", ".join(
            part for part in [best_candidate.get("address", ""), best_candidate.get("city", ""), "IL", best_candidate.get("zip", "")]
            if clean_text(part)
        )
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(best_candidate.get("zip", "")),
            "Telephone": normalize_phone(best_candidate.get("phone", "")),
            "URL": "",
            "Capacity (optional)": best_candidate.get("capacity", ""),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(best_candidate.get("age_range", "")),
        }
        matched_provider_name = best_candidate.get("provider_name", "")
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=best_candidate.get("city", ""),
            candidate_address=address_value,
            candidate_phone=values["Telephone"],
            candidate_url="",
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = "https://sunshine.dcfs.illinois.gov/Content/Licensing/Daycare/ProviderLookup.aspx"
        sources = {
            field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Illinois official childcare portal")
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def search_virginia_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        home_url = "https://www.dss.virginia.gov/facility/search/cc2.cgi?rm=Search"
        try:
            for variant in profile.search_name_variants[:4]:
                driver = self.open_state_portal_query_tab("VA", home_url)
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.NAME, "search_keywords_name"))
                )
                name_input = driver.find_element(By.NAME, "search_keywords_name")
                name_input.clear()
                name_input.send_keys(variant)
                LOGGER.info(
                    "Virginia portal searching PID=%s with provider_variant=%s",
                    record.get("PID", ""),
                    variant,
                )
                name_input.submit()
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(1.0)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                results: List[Dict[str, str]] = []
                seen_urls = set()
                for link in soup.select("a[href*='cc2.cgi?rm=Details;ID=']"):
                    href = clean_text(link.get("href", ""))
                    href = urljoin("https://www.dss.virginia.gov/facility/search/cc2.cgi?rm=Search", href)
                    name = clean_text(link.get_text(" ", strip=True))
                    if not href or href in seen_urls:
                        continue
                    if not name:
                        parent_text = clean_text(link.parent.get_text(" ", strip=True) if link.parent else "")
                        name = parent_text.split("  ")[0].strip() if parent_text else ""
                    if not name:
                        continue
                    container = link.find_parent("tr") or link.find_parent("td") or link.parent
                    container_text = clean_text(container.get_text("\n", strip=True) if container else "")
                    container_lines = [
                        clean_text(item)
                        for item in (container.stripped_strings if container else [])
                        if clean_text(item)
                    ]
                    address_line = ""
                    city = ""
                    zip_code = ""
                    for line in container_lines:
                        if line == name:
                            continue
                        if not address_line and re.search(r"\b\d{1,6}\s+[A-Za-z0-9#.\- ]+\b", line):
                            address_line = line
                        city_state_zip_match = re.search(r"\b([A-Z][A-Z .'-]+),\s*VA\s+(\d{5}(?:-\d{4})?)\b", line, re.IGNORECASE)
                        if city_state_zip_match:
                            city = clean_text(city_state_zip_match.group(1))
                            zip_code = normalize_zip(city_state_zip_match.group(2))
                    address_value = ", ".join(
                        part for part in [address_line, city, "VA", zip_code] if clean_text(part)
                    )
                    phone_value = normalize_phone(container_text)
                    seen_urls.add(href)
                    results.append(
                        {
                            "provider_name": name,
                            "detail_url": href,
                            "address": address_value,
                            "city": city,
                            "zip": zip_code,
                            "phone": phone_value,
                        }
                    )
                if results:
                    LOGGER.info(
                        "Virginia portal returned %s detail links for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                self.finalize_state_portal_query("VA", home_url)
            LOGGER.info("Virginia portal returned 0 detail links for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("VA")
            raise

    def fetch_virginia_detail_page(
        self,
        detail_url: str,
        action_label: str,
        driver: Optional[webdriver.Chrome] = None,
    ) -> Dict[str, str]:
        driver = driver or self.get_state_portal_driver("VA")
        try:
            LOGGER.info("Fetching Virginia detail page via Selenium action=%s url=%s", action_label, detail_url)
            driver.get(detail_url)
            WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1.0)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            text = soup.get_text("\n", strip=True)
        except Exception:
            self.reset_state_portal_driver("VA")
            raise

        labeled_values: Dict[str, str] = {}
        for row in soup.select("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = clean_text(cells[0].get_text(" ", strip=True)).rstrip(":")
            value = clean_text(" ".join(cell.get_text(" ", strip=True) for cell in cells[1:]))
            if not label or not value:
                continue
            labeled_values[label.lower()] = value

        for dt in soup.select("dt"):
            label = clean_text(dt.get_text(" ", strip=True)).rstrip(":").lower()
            dd = dt.find_next_sibling("dd")
            value = clean_text(dd.get_text(" ", strip=True) if dd else "")
            if label and value:
                labeled_values[label] = value

        def extract_labeled_value(label: str) -> str:
            direct = labeled_values.get(label.lower(), "")
            if direct:
                return direct
            for key, value in labeled_values.items():
                if label.lower() in key:
                    return value
            patterns = [
                re.compile(rf"{re.escape(label)}\s*[:\t]\s*(.+)", re.IGNORECASE),
                re.compile(rf"{re.escape(label)}\s*\n\s*(.+)", re.IGNORECASE),
            ]
            for pattern in patterns:
                match = pattern.search(text)
                if match:
                    return clean_text(match.group(1))
            return ""

        def extract_structured_address() -> str:
            for label in (
                "Address",
                "Facility Address",
                "Street Address",
                "Location",
                "Physical Address",
            ):
                value = extract_labeled_value(label)
                if value:
                    return value
            lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]
            for index, line in enumerate(lines):
                if re.search(r"\b\d{1,6}\s+[A-Za-z0-9#.\-]+\b", line):
                    if index + 1 < len(lines) and re.search(r"\b[A-Z][A-Za-z.\- ]+,\s*[A-Z]{2}\s+\d{5}", lines[index + 1]):
                        return f"{line}, {lines[index + 1]}"
                    return line
            return self.extract_address_from_text(text, {})

        def extract_structured_phone() -> str:
            for label in ("Phone", "Telephone", "Phone Number", "Business Phone"):
                value = normalize_phone(extract_labeled_value(label))
                if value:
                    return value
            for key, value in labeled_values.items():
                if "phone" in key or "telephone" in key:
                    normalized = normalize_phone(value)
                    if normalized:
                        return normalized
            return normalize_phone(self.extract_phone_from_text(text))

        detail_values = {
            "Mailing_Address": extract_structured_address(),
            "Mailing_Zip": self.extract_zip_from_text(extract_structured_address() or text, {}),
            "Telephone": extract_structured_phone(),
            "URL": "",
            "Capacity (optional)": extract_labeled_value("Capacity"),
            "Age Range (optional)": extract_labeled_value("Ages"),
            "Business_Hours": extract_labeled_value("Business Hours"),
            "Facility_Type": extract_labeled_value("Facility Type"),
            "License_Type": extract_labeled_value("License Type"),
            "Administrator": extract_labeled_value("Administrator"),
            "Inspector": extract_labeled_value("Inspector"),
            "Facility_ID": extract_labeled_value("License/Facility ID#"),
        }
        LOGGER.info("Virginia detail page parsed url=%s values=%s", detail_url, detail_values)
        return detail_values

    def enrich_from_virginia_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_virginia_portal(record)
        except Exception:
            LOGGER.exception("Virginia portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_variant_hit = False
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            score = token_overlap_score(record.get("Daycare_Name", ""), provider_name) * 4
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants[:4]
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "Virginia portal candidate scored %s for PID=%s provider=%s variant_hit=%s",
                score,
                record.get("PID", ""),
                provider_name,
                variant_hit,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_variant_hit = variant_hit
        if not best_candidate:
            return {}, {}
        if best_score < 6 and not (best_variant_hit and len(candidates) == 1):
            LOGGER.warning(
                "Virginia portal rejected best candidate for PID=%s provider=%s score=%s variant_hit=%s candidate_count=%s",
                record.get("PID", ""),
                clean_text(best_candidate.get("provider_name", "")),
                best_score,
                best_variant_hit,
                len(candidates),
            )
            return {}, {}
        detail_url = best_candidate.get("detail_url", "")
        shared_driver = self.get_state_portal_driver("VA")
        try:
            detail_values = self.fetch_virginia_detail_page(
                detail_url=detail_url,
                action_label=f"virginia detail page [{record.get('PID', '')}]",
                driver=shared_driver,
            )
        except Exception:
            LOGGER.exception("Virginia detail page fetch failed for PID=%s url=%s", record.get("PID", ""), detail_url)
            detail_values = {}
        finally:
            self.finalize_state_portal_query("VA", "https://www.dss.virginia.gov/facility/search/cc2.cgi?rm=Search")
        detail_address = clean_text(detail_values.get("Mailing_Address", ""))
        candidate_address = clean_text(best_candidate.get("address", ""))
        selected_address = detail_address if looks_like_street_address(detail_address) else candidate_address
        detail_zip = normalize_zip(detail_values.get("Mailing_Zip", ""))
        candidate_zip = normalize_zip(best_candidate.get("zip", ""))
        selected_zip = detail_zip if detail_zip else candidate_zip
        values = {
            "Mailing_Address": selected_address,
            "Mailing_Zip": selected_zip,
            "Telephone": normalize_phone(detail_values.get("Telephone", "")) or normalize_phone(best_candidate.get("phone", "")) or normalize_phone(detail_values.get("Inspector", "")),
            "URL": "",
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", "")),
        }
        matched_provider_name = best_candidate.get("provider_name", "")
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(record.get("Mailing_City")),
            candidate_address=values.get("Mailing_Address", ""),
            candidate_phone=values.get("Telephone", ""),
            candidate_url=detail_url,
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = detail_url or "https://www.dss.virginia.gov/facility/search/cc2.cgi?rm=Search"
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes="Virginia official childcare portal"
                if field not in {"Matched_Provider_Name", "Match_Status", "Match_Confidence", "Matched_Reason"}
                else "Virginia official childcare portal; accepted candidate metadata",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def search_new_jersey_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://childcareexplorer.njccis.com/portal/"
        try:
            for variant in profile.search_name_variants[:4]:
                driver = self.open_state_portal_query_tab("NJ", portal_url)
                LOGGER.info(
                    "Reloading New Jersey portal landing page for PID=%s before provider_variant=%s",
                    record.get("PID", ""),
                    variant,
                )
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.NAME, "facilityName"))
                )
                time.sleep(1.0)
                provider_input = driver.find_element(By.NAME, "facilityName")
                city_input = driver.find_element(By.NAME, "city")
                provider_input.clear()
                provider_input.send_keys(variant)
                city_input.clear()
                city_input.send_keys(city)
                LOGGER.info(
                    "New Jersey portal searching PID=%s with provider_variant=%s city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                )
                driver.find_element(By.ID, "submit").click()
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    lambda d: d.execute_script(
                        "const grid = document.getElementById('grdUsers'); return !!grid && !grid.hasAttribute('hidden');"
                    )
                )
                time.sleep(1.0)
                candidate_rows = driver.execute_script(
                    """
const table = document.querySelector('#grdUsers tbody.ui-datatable-data');
if (!table) return [];
const rows = [];
Array.from(table.querySelectorAll('tr')).forEach((row, rowIndex) => {
  if (row.innerText.toLowerCase().includes('no records found')) return;
  const cells = Array.from(row.querySelectorAll('td'));
  if (cells.length < 6) return;
  const selectNode = cells[0].querySelector('button.btn-sm, a.btn-sm, button, a, input[type=\"button\"], input[type=\"submit\"]');
  const selectText = ((selectNode && (selectNode.innerText || selectNode.value)) || '').trim();
  const cellValue = (cellIndex) => {
    const cell = cells[cellIndex];
    if (!cell) return '';
    const dataNode = cell.querySelector('.ui-cell-data');
    return ((dataNode && dataNode.innerText) || cell.innerText || '').trim();
  };
  rows.push({
    row_index: rowIndex,
    has_select: !!selectNode,
    select_text: selectText,
    provider_name: cellValue(1),
    address: cellValue(2),
    city: cellValue(3),
    zip: cellValue(4),
    county: cellValue(5),
    text: (row.innerText || '').trim()
  });
});
return rows;
"""
                )
                results: List[Dict[str, str]] = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    address_line = clean_text(item.get("address", ""))
                    result_city = clean_text(item.get("city", "")) or city
                    zip_code = normalize_zip(item.get("zip", ""))
                    container_text = clean_text(item.get("text", ""))
                    address_value = ", ".join(
                        part for part in [address_line, result_city or city, "NJ", zip_code] if clean_text(part)
                    )
                    row_index = str(item.get("row_index", ""))
                    if clean_text(str(item.get("has_select", ""))).lower() not in {"true", "1"}:
                        continue
                    results.append(
                        {
                            "provider_name": provider_name,
                            "address": address_value,
                            "city": result_city or city,
                            "zip": zip_code,
                            "phone": "",
                            "row_index": row_index,
                        }
                    )
                if results:
                    LOGGER.info(
                        "New Jersey portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                self.finalize_state_portal_query("NJ", portal_url)
            LOGGER.info("New Jersey portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("NJ")
            raise

    def fetch_new_jersey_detail_page(
        self,
        driver: webdriver.Chrome,
        row_index: str,
        action_label: str,
        record: Dict[str, str],
    ) -> Dict[str, str]:
        LOGGER.info("Fetching New Jersey detail page via Selenium action=%s row_index=%s", action_label, row_index)
        rows = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "#grdUsers tbody.ui-datatable-data tr")
        )
        index = int(row_index)
        if index < 0 or index >= len(rows):
            raise RuntimeError(f"New Jersey row_index={row_index} is out of bounds for {len(rows)} rows")
        row = rows[index]
        button = row.find_element(By.CSS_SELECTOR, "button.btn-sm, a.btn-sm, button, a, input[type='button'], input[type='submit']")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        time.sleep(0.5)
        try:
            button.click()
        except Exception:
            LOGGER.info("Normal Selenium click failed for New Jersey row_index=%s; trying JS click", row_index)
            driver.execute_script("arguments[0].click();", button)
        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
            lambda d: "/provider-details/" in clean_text(d.current_url)
            or d.execute_script(
                """
const phoneBlock = Array.from(document.querySelectorAll('.labelIt')).find(
  el => ((el.innerText || '').toLowerCase().includes('phone')) && el.querySelector('a[href^="tel:"]')
);
const ageBlock = Array.from(document.querySelectorAll('.panel-footer h3, .panel-footer, h3')).find(
  el => (el.innerText || '').toLowerCase().includes('ages served')
);
return !!phoneBlock || !!ageBlock;
"""
            )
        )
        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
            lambda d: d.execute_script(
                """
const phoneBlock = Array.from(document.querySelectorAll('.labelIt')).find(
  el => ((el.innerText || '').toLowerCase().includes('phone')) && el.querySelector('a[href^="tel:"]')
);
const ageBlock = Array.from(document.querySelectorAll('.panel-footer h3, .panel-footer, h3')).find(
  el => (el.innerText || '').toLowerCase().includes('ages served')
);
return !!phoneBlock || !!ageBlock;
"""
            )
        )
        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
            lambda d: d.execute_script(
                """
const bodyText = (document.body && document.body.innerText) || '';
return bodyText.includes('Ages Served') || bodyText.includes('Phone');
"""
            )
        )
        time.sleep(1.0)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        provider_panel = soup.select_one("portal-provider-header .panel.panel-info") or soup.select_one(
            "portal-provider-header"
        )
        panel_soup = provider_panel if provider_panel is not None else soup

        labeled_values: Dict[str, str] = {}
        for block in panel_soup.select(".labelIt"):
            label_node = block.find("strong")
            label = clean_text(label_node.get_text(" ", strip=True) if label_node else "").rstrip(":").lower()
            if not label:
                continue
            for node in block.find_all("strong"):
                node.extract()
            value = clean_text(block.get_text(" ", strip=True))
            if value and "no " in value.lower() and " on record" in value.lower():
                value = ""
            labeled_values[label] = value

        def extract_labeled_value(label: str) -> str:
            direct = clean_text(labeled_values.get(label.lower(), ""))
            return "" if ("no " in direct.lower() and " on record" in direct.lower()) else direct

        detail_phone = ""
        phone_block = next(
            (
                block
                for block in panel_soup.select(".labelIt")
                if clean_text((block.find("strong").get_text(" ", strip=True) if block.find("strong") else "")).lower()
                == "phone"
            ),
            None,
        )
        if phone_block is not None:
            phone_link = phone_block.select_one("a[href^='tel:']")
            if phone_link:
                detail_phone = normalize_phone(phone_link.get_text(" ", strip=True) or phone_link.get("href", ""))
        if not detail_phone:
            try:
                phone_xpath = "/html/body/app-root/div/main/div/portal-provider-details/div/div[2]/div/p-accordion/div/p-accordiontab[1]/div[2]/div/div/div/portal-provider-header/div/div[2]/div/div/div/div[3]/span/a"
                phone_node = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, phone_xpath))
                )
                detail_phone = normalize_phone(phone_node.text or phone_node.get_attribute("href") or "")
            except Exception:
                detail_phone = ""

        detail_age = ""
        for node in panel_soup.select(".panel-footer h3, .panel-footer, h3"):
            node_text = clean_text(node.get_text(" ", strip=True))
            if not node_text or "ages served" not in node_text.lower():
                continue
            detail_age = re.sub(r"^\s*Ages Served\s*", "", node_text, flags=re.IGNORECASE).strip(" :-")
            if detail_age:
                break
        if not detail_age:
            try:
                age_xpath = "/html/body/app-root/div/main/div/portal-provider-details/div/div[2]/div/p-accordion/div/p-accordiontab[1]/div[2]/div/div/div/portal-provider-header/div/div[3]/h3"
                age_node = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, age_xpath))
                )
                age_text = clean_text(age_node.text)
                detail_age = re.sub(r"^\s*Ages Served\s*", "", age_text, flags=re.IGNORECASE).strip(" :-")
            except Exception:
                detail_age = ""

        detail_values = {
            "Mailing_Address": "",
            "Mailing_Zip": "",
            "Telephone": detail_phone,
            "Detail_URL": normalize_url(driver.current_url),
            "Capacity (optional)": extract_labeled_value("capacity"),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_age or extract_labeled_value("Ages Served")),
        }
        LOGGER.info("New Jersey detail page parsed url=%s values=%s", driver.current_url, detail_values)
        return detail_values

    def enrich_from_new_jersey_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_new_jersey_portal(record)
        except Exception:
            LOGGER.exception("New Jersey portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_city_match = False
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(candidate.get("city", "")).lower()
            if city_match:
                score += 3
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants[:4]
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "New Jersey portal candidate scored %s for PID=%s provider=%s overlap=%.3f city_match=%s",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
                city_match,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate:
            return {}, {}
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            LOGGER.warning(
                "New Jersey portal rejected best candidate for PID=%s provider=%s score=%s overlap=%.3f city_match=%s",
                record.get("PID", ""),
                best_candidate.get("provider_name", ""),
                best_score,
                best_overlap,
                best_city_match,
            )
            return {}, {}
        driver = self.get_state_portal_driver("NJ")
        try:
            detail_values = self.fetch_new_jersey_detail_page(
                driver=driver,
                row_index=best_candidate.get("row_index", ""),
                action_label=f"new jersey detail page [{record.get('PID', '')}]",
                record=record,
            )
        except Exception:
            LOGGER.exception("New Jersey detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            self.finalize_state_portal_query("NJ", "https://childcareexplorer.njccis.com/portal/")
        values = {
            "Mailing_Address": clean_text(best_candidate.get("address", "")),
            "Mailing_Zip": normalize_zip(best_candidate.get("zip", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "")),
            "URL": "",
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", "")),
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(best_candidate.get("city", "")),
            candidate_address=values.get("Mailing_Address", ""),
            candidate_phone=values.get("Telephone", ""),
            candidate_url=clean_text(detail_values.get("Detail_URL", "")),
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = clean_text(detail_values.get("Detail_URL", "")) or "https://childcareexplorer.njccis.com/portal/"
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes="New Jersey official childcare portal",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def try_select_north_carolina_city(self, driver: webdriver.Chrome, city: str) -> bool:
        city = clean_text(city)
        if not city:
            return False
        city_input = driver.find_element(By.XPATH, '//*[@id="dnn_ctr1464_View_cboCity_Input"]')
        city_input.send_keys(Keys.CONTROL, "a")
        city_input.send_keys(Keys.DELETE)
        city_input.send_keys(city)
        time.sleep(1.0)

        # First try the common combo-box keyboard path.
        try:
            city_input.send_keys(Keys.ARROW_DOWN)
            time.sleep(0.3)
            city_input.send_keys(Keys.ENTER)
            time.sleep(0.8)
            current_value = clean_text(city_input.get_attribute("value") or city_input.get_attribute("title") or city_input.text)
            if current_value.lower() == city.lower():
                return True
        except Exception:
            pass

        dropdown_exact_xpaths = [
            f"//*[contains(@id,'dnn_ctr1464_View_cboCity_DropDown')]//*[self::li or self::div or self::td or self::span][normalize-space()={json.dumps(city)}]",
            f"//*[contains(@id,'dnn_ctr1464_View_cboCity_DropDown')]//*[self::li or self::div or self::td or self::span][contains(normalize-space(), {json.dumps(city)})]",
            f"//*[contains(@id,'dnn_ctr1464_View_cboCity')]//*[self::li or self::div or self::td or self::span][normalize-space()={json.dumps(city)}]",
        ]
        for xpath in dropdown_exact_xpaths:
            try:
                candidate = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'nearest'});", candidate)
                try:
                    candidate.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", candidate)
                time.sleep(0.8)
                current_value = clean_text(city_input.get_attribute("value") or city_input.get_attribute("title") or city_input.text)
                if current_value.lower() == city.lower():
                    return True
            except Exception:
                continue

        dropdown_candidates = driver.find_elements(
            By.XPATH,
            (
                "//*[contains(@id,'dnn_ctr1464_View_cboCity_DropDown')]"
                "//*[self::li or self::div or self::td or self::span]"
            ),
        )
        for candidate in dropdown_candidates:
            candidate_text = clean_text(candidate.text)
            if candidate_text.lower() != city.lower():
                continue
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'nearest'});", candidate)
                candidate.click()
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", candidate)
                except Exception:
                    continue
            time.sleep(0.5)
            current_value = clean_text(city_input.get_attribute("value") or city_input.get_attribute("title") or city_input.text)
            if current_value.lower() == city.lower():
                return True

        city_input.send_keys(Keys.CONTROL, "a")
        city_input.send_keys(Keys.DELETE)
        return False

    def search_north_carolina_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://ncchildcare.ncdhhs.gov/childcaresearch"
        table_id = "dnn_ctr1464_View_rgSearchResults_ctl00"
        no_results_xpath = "/html/body/form/div[6]/div[3]/div[3]/div/div[1]/div/div[2]/div[2]/div[1]/div[2]/div"
        nc_wait_timeout = max(SELENIUM_WAIT_TIMEOUT, 45)
        try:
            for variant in profile.search_name_variants[:4]:
                driver = self.open_state_portal_query_tab("NC", portal_url)
                LOGGER.info(
                    "Reloading North Carolina portal landing page for PID=%s before provider_variant=%s",
                    record.get("PID", ""),
                    variant,
                )
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="dnn_ctr1464_View_txtFacilityName"]'))
                )
                time.sleep(1.0)
                provider_input = driver.find_element(By.XPATH, '//*[@id="dnn_ctr1464_View_txtFacilityName"]')
                provider_input.clear()
                provider_input.send_keys(variant)
                city_selected = self.try_select_north_carolina_city(driver, city)
                if city_selected:
                    LOGGER.info(
                        "North Carolina portal selected dropdown city=%s for PID=%s",
                        city,
                        record.get("PID", ""),
                    )
                else:
                    LOGGER.info(
                        "North Carolina portal could not match city=%s in dropdown for PID=%s; searching without city filter",
                        city,
                        record.get("PID", ""),
                    )
                LOGGER.info(
                    "North Carolina portal searching PID=%s with provider_variant=%s city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                )
                driver.find_element(By.XPATH, '//*[@id="dnn_ctr1464_View_btnSearch"]').click()
                WebDriverWait(driver, nc_wait_timeout).until(
                    lambda d: d.execute_script(
                        """
const table = document.getElementById(arguments[0]);
const noResultsNode = document.evaluate(arguments[1], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (noResultsNode) {
  const msg = (noResultsNode.innerText || '').trim().toLowerCase();
  if (msg.includes('the search did not return any results')) return true;
}
if (!table) return false;
const visible = table.offsetParent !== null;
const resultLink = table.querySelector('tbody tr td a, tr td a');
return visible && !!resultLink;
""",
                        table_id,
                        no_results_xpath,
                    )
                )
                no_results_text = ""
                try:
                    no_results_node = driver.find_element(By.XPATH, no_results_xpath)
                    no_results_text = clean_text(no_results_node.text)
                except Exception:
                    no_results_text = ""
                if "the search did not return any results" in no_results_text.lower():
                    LOGGER.info(
                        "North Carolina portal returned explicit no-results banner for PID=%s using provider_variant=%s",
                        record.get("PID", ""),
                        variant,
                    )
                    self.finalize_state_portal_query("NC", portal_url)
                    continue
                time.sleep(2.0)
                candidate_rows = driver.execute_script(
                    """
const table = document.getElementById(arguments[0]);
if (!table) return [];
const rows = [];
Array.from(table.querySelectorAll('tbody tr, tr')).forEach((row, rowIndex) => {
  const cells = Array.from(row.querySelectorAll('td'));
  const rowText = (row.innerText || '').trim();
  if (!rowText || rowText.toLowerCase().includes('no records')) return;
  const cellTexts = cells.map(cell => ((cell.innerText || '').trim()));
  const links = Array.from(row.querySelectorAll('a'));
  const meaningfulLinks = links
    .map(link => ((link.innerText || '').trim()))
    .filter(text => text && !/^\d+$/.test(text));
  let providerName = '';
  if (meaningfulLinks.length) {
    providerName = meaningfulLinks.sort((a, b) => b.length - a.length)[0];
  } else if (cellTexts.length > 1) {
    providerName = cellTexts[1];
  } else if (cellTexts.length) {
    providerName = cellTexts[0];
  }
  if (!providerName) return;
  rows.push({
    row_index: rows.length,
    dom_row_index: rowIndex,
    provider_name: providerName,
    row_text: rowText,
    cell_texts: cellTexts
  });
});
return rows;
""",
                    table_id,
                )
                results: List[Dict[str, str]] = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    row_text = clean_text(item.get("row_text", ""))
                    cell_texts = [clean_text(value) for value in (item.get("cell_texts") or []) if clean_text(value)]
                    city_match = city.lower() in row_text.lower() if city else False
                    results.append(
                        {
                            "provider_name": provider_name,
                            "row_index": str(item.get("row_index", "")),
                            "row_text": row_text,
                            "city": city if city_match else "",
                            "cell_texts": " || ".join(cell_texts),
                        }
                    )
                if results:
                    LOGGER.info(
                        "North Carolina portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                self.finalize_state_portal_query("NC", portal_url)
            LOGGER.info("North Carolina portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("NC")
            raise

    def fetch_north_carolina_detail_page(
        self,
        driver: webdriver.Chrome,
        row_index: str,
        action_label: str,
        record: Dict[str, str],
    ) -> Dict[str, str]:
        LOGGER.info("Fetching North Carolina detail page via Selenium action=%s row_index=%s", action_label, row_index)
        rows = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "#dnn_ctr1464_View_rgSearchResults_ctl00 tr")
        )
        candidate_rows = [row for row in rows if row.find_elements(By.TAG_NAME, "a")]
        index = int(row_index)
        if index < 0 or index >= len(candidate_rows):
            raise RuntimeError(
                f"North Carolina row_index={row_index} is out of bounds for {len(candidate_rows)} candidate rows"
            )
        row = candidate_rows[index]
        detail_link = row.find_element(By.TAG_NAME, "a")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_link)
        time.sleep(0.5)
        try:
            detail_link.click()
        except Exception:
            LOGGER.info("Normal Selenium click failed for North Carolina row_index=%s; trying JS click", row_index)
            driver.execute_script("arguments[0].click();", detail_link)

        address_xpath = "/html/body/form/div[6]/div[3]/div[3]/div/div[1]/div/div[2]/div[2]/div[3]/div/div[2]/div/div/div[3]/div[2]"
        phone_xpath = "/html/body/form/div[6]/div[3]/div[3]/div/div[1]/div/div[2]/div[2]/div[3]/div/div[2]/div/div/div[7]/div[2]"
        license_tab_xpath = "/html/body/form/div[6]/div[3]/div[3]/div/div[1]/div/div[2]/div[2]/div[3]/div/div[3]"
        age_id = "dnn_ctr1464_View_FacilityDetail_rptLicenseInfo_lblAgeRange_0"
        capacity_id = "dnn_ctr1464_View_FacilityDetail_rptLicenseInfo_lblFirstShiftCapacity_0"

        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, address_xpath))
        )
        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, phone_xpath))
        )
        time.sleep(1.0)

        address_text = clean_text(driver.find_element(By.XPATH, address_xpath).text)
        phone_text = normalize_phone(driver.find_element(By.XPATH, phone_xpath).text)

        try:
            license_tab = driver.find_element(By.XPATH, license_tab_xpath)
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", license_tab)
            license_tab.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", driver.find_element(By.XPATH, license_tab_xpath))
            except Exception:
                LOGGER.info("North Carolina license information section click failed for PID=%s", record.get("PID", ""))

        age_text = ""
        capacity_text = ""
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, age_id)))
            age_node = driver.find_element(By.ID, age_id)
            age_text = clean_text(
                age_node.text
                or age_node.get_attribute("innerText")
                or age_node.get_attribute("textContent")
                or age_node.get_attribute("innerHTML")
            )
        except Exception:
            age_text = ""
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, capacity_id)))
            capacity_node = driver.find_element(By.ID, capacity_id)
            capacity_text = clean_text(
                capacity_node.text or capacity_node.get_attribute("innerText") or capacity_node.get_attribute("textContent")
            )
        except Exception:
            capacity_text = ""

        normalized_age_text = normalize_age_groups_text_to_numeric_range(age_text)
        if not normalized_age_text and age_text:
            fallback_age = clean_text(age_text)
            fallback_age = re.sub(r"\bthrough\b", " - ", fallback_age, flags=re.IGNORECASE)
            fallback_age = re.sub(r"\bto\b", " - ", fallback_age, flags=re.IGNORECASE)
            fallback_age = re.sub(r"\s*-\s*", " - ", fallback_age)
            normalized_age_text = fallback_age
        LOGGER.info(
            "North Carolina detail raw age text for PID=%s age_text=%s normalized_age_text=%s",
            record.get("PID", ""),
            age_text,
            normalized_age_text,
        )

        detail_values = {
            "Mailing_Address": address_text,
            "Mailing_Zip": normalize_zip(address_text),
            "Telephone": phone_text,
            "Detail_URL": normalize_url(driver.current_url),
            "Capacity (optional)": capacity_text,
            "Age Range (optional)": normalized_age_text,
        }
        LOGGER.info("North Carolina detail page parsed url=%s values=%s", driver.current_url, detail_values)
        return detail_values

    def enrich_from_north_carolina_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_north_carolina_portal(record)
        except Exception:
            LOGGER.exception("North Carolina portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_city_match = False
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            city_match = clean_text(record.get("Mailing_City")).lower() in clean_text(candidate.get("row_text", "")).lower()
            if city_match:
                score += 3
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants[:4]
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "North Carolina portal candidate scored %s for PID=%s provider=%s overlap=%.3f city_match=%s",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
                city_match,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate:
            return {}, {}
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            LOGGER.warning(
                "North Carolina portal rejected best candidate for PID=%s provider=%s score=%s overlap=%.3f city_match=%s",
                record.get("PID", ""),
                best_candidate.get("provider_name", ""),
                best_score,
                best_overlap,
                best_city_match,
            )
            return {}, {}
        driver = self.get_state_portal_driver("NC")
        try:
            detail_values = self.fetch_north_carolina_detail_page(
                driver=driver,
                row_index=best_candidate.get("row_index", ""),
                action_label=f"north carolina detail page [{record.get('PID', '')}]",
                record=record,
            )
        except Exception:
            LOGGER.exception("North Carolina detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            self.finalize_state_portal_query("NC", "https://ncchildcare.ncdhhs.gov/childcaresearch")
        values = {
            "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")),
            "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "") or detail_values.get("Mailing_Address", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "")),
            "URL": "",
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": (
                format_numeric_age_range(*[part.strip() for part in clean_text(detail_values.get("Age Range (optional)", "")).split("-", 1)], unit="years")
                if re.fullmatch(r"\s*\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*", clean_text(detail_values.get("Age Range (optional)", "")))
                else normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", ""))
            ),
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(record.get("Mailing_City", "")) if best_city_match else "",
            candidate_address=values.get("Mailing_Address", ""),
            candidate_phone=values.get("Telephone", ""),
            candidate_url=clean_text(detail_values.get("Detail_URL", "")),
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = clean_text(detail_values.get("Detail_URL", "")) or "https://ncchildcare.ncdhhs.gov/childcaresearch"
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes="North Carolina official childcare portal",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def search_arizona_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://azchildcaresearch.azdes.gov/s/providersearch?language=en_US"
        toast_xpath = "/html/body/div[4]/div[1]/div/div"
        results_xpath = "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/section/div[1]/lightning-layout"
        try:
            for variant in profile.search_name_variants[:4]:
                driver = self.open_state_portal_query_tab("AZ", portal_url)
                LOGGER.info(
                    "Arizona portal searching PID=%s with provider_variant=%s city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                )
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/c-pvm-header-component-application/header/div[1]/nav/div/div[3]/input",
                        )
                    )
                )
                city_input = driver.find_element(
                    By.XPATH,
                    "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/c-pvm-header-component-application/header/div[1]/nav/div/div[1]/div/input",
                )
                provider_input = driver.find_element(
                    By.XPATH,
                    "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/c-pvm-header-component-application/header/div[1]/nav/div/div[3]/input",
                )
                city_input.send_keys(Keys.CONTROL, "a")
                city_input.send_keys(Keys.DELETE)
                if city:
                    city_input.send_keys(city)
                provider_input.send_keys(Keys.CONTROL, "a")
                provider_input.send_keys(Keys.DELETE)
                provider_input.send_keys(variant)
                baseline_signature = driver.execute_script(
                    """
const root = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!root) return '';
const names = Array.from(root.querySelectorAll('lightning-layout-item a'))
  .map(node => ((node.innerText || node.textContent || '').trim()))
  .filter(Boolean)
  .slice(0, 10);
return `${names.length}::${names.join('|')}`;
""",
                    results_xpath,
                )
                driver.find_element(
                    By.XPATH,
                    "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/c-pvm-header-component-application/header/div[1]/nav/div/button",
                ).click()

                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    lambda d: d.execute_script(
                        """
const toast = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (toast && (toast.innerText || '').trim()) return true;
const results = document.evaluate(arguments[1], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!results) return false;
const names = Array.from(results.querySelectorAll('lightning-layout-item a'))
  .map(node => ((node.innerText || node.textContent || '').trim()))
  .filter(Boolean)
  .slice(0, 10);
const signature = `${names.length}::${names.join('|')}`;
return !!names.length && signature !== arguments[2];
""",
                        toast_xpath,
                        results_xpath,
                        baseline_signature,
                    )
                )
                toast_text = ""
                try:
                    toast_text = clean_text(driver.find_element(By.XPATH, toast_xpath).text)
                except Exception:
                    toast_text = ""
                if toast_text and "no data" in toast_text.lower():
                    LOGGER.info(
                        "Arizona portal returned no-data toast for PID=%s using provider_variant=%s toast=%s",
                        record.get("PID", ""),
                        variant,
                        toast_text,
                    )
                    self.finalize_state_portal_query("AZ")
                    continue

                time.sleep(2.0)
                candidate_rows = driver.execute_script(
                    """
const root = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!root) return [];
const cards = Array.from(root.querySelectorAll('lightning-layout-item'));
const results = [];
cards.forEach((card, index) => {
  const nameLink = card.querySelector('div a');
  const addressLink = card.querySelector('a.multiline.provider-address-link');
  const addressLine1Node = addressLink ? addressLink.querySelector('.addr-line1') : null;
  const addressLine2Node = addressLink ? addressLink.querySelector('.addr-line2') : null;
  const addressNode = card.querySelector('div div:nth-of-type(2) div:nth-of-type(1) p:nth-of-type(1) a span:nth-of-type(1)');
  const phoneNode = card.querySelector('div div:nth-of-type(2) div:nth-of-type(1) p:nth-of-type(2) a');
  const cardText = (card.innerText || '').trim();
  const providerName = ((nameLink && nameLink.innerText) || '').trim();
  if (!providerName) return;
  const href = ((nameLink && nameLink.href) || '').trim();
  const address = ((addressLine1Node && addressLine1Node.innerText) || (addressNode && addressNode.innerText) || '').trim();
  const addressLine2 = ((addressLine2Node && addressLine2Node.innerText) || '').trim();
  const phone = ((phoneNode && phoneNode.innerText) || '').trim();
  let capacity = '';
  const paragraphs = Array.from(card.querySelectorAll('p'));
  for (const paragraph of paragraphs) {
    const paragraphText = ((paragraph.innerText || '') + ' ' + (paragraph.textContent || '')).trim();
    if (!/capacity/i.test(paragraphText)) continue;
    const spans = Array.from(paragraph.querySelectorAll('span')).map(span => ((span.innerText || span.textContent || '').trim())).filter(Boolean);
    if (spans.length >= 2) {
      capacity = spans[spans.length - 1];
      break;
    }
    const match = paragraphText.match(/capacity\s*:?\s*([0-9]+(?:\.[0-9]+)?)/i);
    if (match) {
      capacity = match[1];
      break;
    }
  }
  results.push({
    candidate_index: index,
    provider_name: providerName,
    address: address,
    address_line2: addressLine2,
    city: arguments[1],
    phone: phone,
    capacity: capacity,
    detail_url: href,
    row_text: cardText
  });
});
return results;
""",
                    results_xpath,
                    city,
                )
                results: List[Dict[str, str]] = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    raw_address_value = clean_text(item.get("address", ""))
                    address_value = raw_address_value
                    address_line2_value = clean_text(item.get("address_line2", ""))
                    row_text = clean_text(item.get("row_text", ""))
                    phone_value = normalize_phone(item.get("phone", ""))
                    raw_capacity_value = clean_text(item.get("capacity", ""))
                    capacity_match = re.search(r"\b\d+(?:\.\d+)?\b", raw_capacity_value)
                    capacity_value = capacity_match.group(0) if capacity_match else ""
                    href_value = clean_text(item.get("detail_url", ""))
                    result_city = city
                    zip_source = address_line2_value.split(",")[-1] if "," in address_line2_value else address_line2_value
                    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", zip_source or "")
                    if not zip_match:
                        zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", row_text or "")
                    zip_code = zip_match.group(0) if zip_match else ""
                    results.append(
                        {
                            "candidate_index": str(item.get("candidate_index", "")),
                            "provider_name": provider_name,
                            "address": address_value,
                            "city": result_city,
                            "zip": zip_code,
                            "phone": phone_value,
                            "capacity": capacity_value,
                            "detail_url": href_value,
                            "row_text": row_text,
                        }
                    )
                if results:
                    LOGGER.info(
                        "Arizona portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                self.finalize_state_portal_query("AZ")
            LOGGER.info("Arizona portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("AZ")
            raise

    def fetch_arizona_detail_popup(
        self,
        driver: webdriver.Chrome,
        candidate_index: str,
        action_label: str,
    ) -> Dict[str, str]:
        try:
            index_value = int(clean_text(candidate_index))
        except (TypeError, ValueError):
            raise RuntimeError(f"Invalid Arizona candidate_index={candidate_index!r}")
        LOGGER.info(
            "Fetching Arizona detail popup via Selenium action=%s candidate_index=%s",
            action_label,
            index_value,
        )
        popup_trigger_xpath = (
            f"/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/"
            f"section/div[1]/lightning-layout/slot/lightning-layout-item/slot/lightning-layout/slot/"
            f"lightning-layout-item[{index_value + 1}]/slot/div/div[1]"
        )
        popup_container_xpath = "//*[contains(@class,'popup-container') and contains(@class,'slide-in')]"
        contact_info_xpath = "//*[contains(@class,'popup-container') and contains(@class,'slide-in')]//*[contains(@class,'contact-info')]"
        trigger = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.XPATH, popup_trigger_xpath))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", trigger)
        time.sleep(0.5)
        try:
            trigger.click()
        except Exception:
            driver.execute_script("arguments[0].click();", trigger)
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.XPATH, popup_container_xpath))
        )
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.XPATH, contact_info_xpath))
        )
        time.sleep(1.0)
        contact_details = driver.execute_script(
            """
const popup = document.querySelector('.popup-container.slide-in');
if (!popup) return {};
const contactInfo = popup.querySelector('.contact-info');
if (!contactInfo) return {};
const addressLink = contactInfo.querySelector('p a');
const addressText = ((addressLink && (addressLink.textContent || addressLink.innerText)) || '').trim();
return {
  address_text: addressText
};
"""
        ) or {}
        address_text = clean_text(contact_details.get("address_text", ""))
        address_parts = [clean_text(part) for part in address_text.split(",") if clean_text(part)]
        detail_values = {
            "Mailing_Address": address_parts[0] if address_parts else "",
            "Mailing_Zip": normalize_zip(address_parts[-1] if address_parts else ""),
        }
        LOGGER.info("Arizona detail popup parsed values=%s", detail_values)
        return detail_values

    def enrich_from_arizona_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_arizona_portal(record)
        except Exception:
            LOGGER.exception("Arizona portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_city_match = False
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(candidate.get("city", "")).lower()
            if city_match:
                score += 3
            if clean_text(candidate.get("phone", "")):
                score += 1
            if clean_text(candidate.get("capacity", "")):
                score += 1
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants[:4]
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "Arizona portal candidate scored %s for PID=%s provider=%s overlap=%.3f city_match=%s",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
                city_match,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate:
            return {}, {}
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            LOGGER.warning(
                "Arizona portal rejected best candidate for PID=%s provider=%s score=%s overlap=%.3f city_match=%s",
                record.get("PID", ""),
                best_candidate.get("provider_name", ""),
                best_score,
                best_overlap,
                best_city_match,
            )
            self.finalize_state_portal_query("AZ")
            return {}, {}
        try:
            driver = self.get_state_portal_driver("AZ")
            try:
                detail_values = self.fetch_arizona_detail_popup(
                    driver=driver,
                    candidate_index=best_candidate.get("candidate_index", ""),
                    action_label=f"arizona detail popup [{record.get('PID', '')}]",
                )
            except Exception:
                LOGGER.exception("Arizona detail popup fetch failed for PID=%s", record.get("PID", ""))
                detail_values = {}
            values = {
                "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "") or best_candidate.get("address", "")),
                "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "") or best_candidate.get("zip", "")),
                "Telephone": normalize_phone(best_candidate.get("phone", "")),
                "URL": clean_text(best_candidate.get("detail_url", "")),
                "Capacity (optional)": clean_text(best_candidate.get("capacity", "")),
                "Age Range (optional)": "",
            }
            matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
            match_status, match_confidence, match_reason = classify_match_status(
                record,
                candidate_name=matched_provider_name,
                candidate_city=clean_text(best_candidate.get("city", "")),
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
            source_url = values.get("URL", "") or "https://azchildcaresearch.azdes.gov/s/providersearch?language=en_US"
            sources = {
                field: build_source_entry(
                    value=value,
                    source_url=source_url,
                    source_type="official_state_portal",
                    notes="Arizona official childcare portal",
                )
                for field, value in values.items()
                if clean_text(value)
            }
            return values, sources
        finally:
            self.finalize_state_portal_query("AZ")

    def search_michigan_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://greatstarttoquality.org/find-programs/"
        no_results_text = "Your search returned no matches, please check your search criteria and try again."
        try:
            for variant in profile.search_name_variants[:4]:
                driver = self.open_state_portal_query_tab("MI", portal_url)
                LOGGER.info(
                    "Michigan portal searching PID=%s with provider_variant=%s",
                    record.get("PID", ""),
                    variant,
                )
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "/html/body/div[2]/div/div/div/main/article/div[1]/div/div[3]/div/form/div[3]/div[2]/div/div[2]/div[2]/div/div/input",
                        )
                    )
                )
                provider_input = driver.find_element(
                    By.XPATH,
                    "/html/body/div[2]/div/div/div/main/article/div[1]/div/div[3]/div/form/div[3]/div[2]/div/div[2]/div[2]/div/div/input",
                )
                provider_input.send_keys(Keys.CONTROL, "a")
                provider_input.send_keys(Keys.DELETE)
                provider_input.send_keys(variant)
                existing_handles = set(driver.window_handles)
                submit_button = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="submitAgency4ReferralForm"]'))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", submit_button)
                time.sleep(0.5)
                try:
                    WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="submitAgency4ReferralForm"]'))
                    )
                    submit_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", submit_button)
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    lambda d: len(set(d.window_handles) - existing_handles) >= 1
                    or "UpdateReferral" in clean_text(d.current_url)
                )
                new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
                if new_handles:
                    driver.switch_to.window(new_handles[-1])
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    lambda d: d.execute_script(
                        """
const body = document.body;
const bodyText = ((body && (body.innerText || body.textContent)) || '').trim().toLowerCase();
if (bodyText.includes(arguments[0])) return 'no_results';
const root = document.getElementById('PSResults');
if (!root) return false;
const cards = root.querySelectorAll('.provider-panel.countTotal');
if (cards.length > 0) return 'results';
return false;
""",
                        no_results_text.lower(),
                    )
                )
                body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text)
                if no_results_text.lower() in body_text.lower():
                    LOGGER.info(
                        "Michigan portal returned no matches for PID=%s using provider_variant=%s",
                        record.get("PID", ""),
                        variant,
                    )
                    self.finalize_state_portal_query("MI")
                    continue
                time.sleep(1.5)
                candidate_rows = driver.execute_script(
                    """
const root = document.getElementById('PSResults');
if (!root) return [];
const cards = Array.from(root.querySelectorAll('.provider-panel.countTotal'));
const results = [];
cards.forEach((card, index) => {
  const nameLink = card.querySelector('a.moreINFO');
  const name = ((nameLink && (nameLink.innerText || nameLink.textContent)) || '').trim();
  if (!name) return;
  const href = ((nameLink && nameLink.href) || '').trim();
  const mapNode = card.querySelector('a.viewInMap');
  const addressText = ((mapNode && mapNode.getAttribute('address')) || '').trim();
  const cityText = ((mapNode && mapNode.getAttribute('city')) || '').trim();
  const zipText = ((mapNode && mapNode.getAttribute('zip')) || '').trim();
  const phoneText = ((mapNode && mapNode.getAttribute('phone')) || '').trim();
  const ageTextAttr = ((mapNode && mapNode.getAttribute('ages')) || '').trim();
  const infoDivs = Array.from(card.querySelectorAll('div'));
  const capacityNode = infoDivs.find(node => {
    const bold = node.querySelector('b');
    const boldText = ((bold && (bold.innerText || bold.textContent)) || '').trim().toLowerCase();
    return boldText === 'capacity';
  });
  const ageNode = infoDivs.find(node => {
    const bold = node.querySelector('b');
    const boldText = ((bold && (bold.innerText || bold.textContent)) || '').trim().toLowerCase();
    return boldText === 'ages served';
  });
  const capacityText = ((capacityNode && (capacityNode.innerText || capacityNode.textContent)) || '').trim();
  const ageText = ageTextAttr || ((ageNode && (ageNode.innerText || ageNode.textContent)) || '').trim();
  results.push({
    candidate_index: index,
    provider_name: name,
    detail_url: href,
    address_text: addressText,
    city_text: cityText,
    zip_text: zipText,
    phone: phoneText,
    capacity: capacityText,
    age: ageText,
    row_text: (card.innerText || '').trim()
  });
});
return results;
"""
                )
                results: List[Dict[str, str]] = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    address_text = clean_text(item.get("address_text", ""))
                    parsed_city = clean_text(item.get("city_text", "")) or city
                    parsed_zip = normalize_zip(item.get("zip_text", ""))
                    row_text = clean_text(item.get("row_text", ""))
                    if not address_text or any(label in address_text for label in [
                        "Type:",
                        "Capacity:",
                        "Ages Served:",
                        "In Operation:",
                        "Licensing Inspection Report",
                        "Program Quality Guide",
                        "Program Details",
                    ]):
                        address_text = row_text
                    if city and parsed_city and city.lower() != parsed_city.lower() and city.lower() not in address_text.lower():
                        continue
                    address_match = re.search(
                        r"(\d{1,6}\s+.+?\b" + re.escape(parsed_city or city) + r"\b\s+MI\s+\d{5}(?:-\d{4})?)",
                        address_text,
                        re.IGNORECASE,
                    )
                    if not address_match:
                        address_match = re.search(
                            r"(\d{1,6}\s+.+?\bMI\b\s+\d{5}(?:-\d{4})?)",
                            address_text,
                            re.IGNORECASE,
                        )
                    address_block = clean_text(address_match.group(1)) if address_match else ""
                    address_line = clean_text(address_block or address_text)
                    if parsed_city and parsed_city.lower() in address_line.lower():
                        address_line = re.split(r"\b" + re.escape(parsed_city) + r"\b", address_line, maxsplit=1, flags=re.IGNORECASE)[0].strip(" ,")
                    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", parsed_zip or address_block or address_text or row_text)
                    raw_phone_text = clean_text(item.get("phone", "")) or row_text
                    phone_value = normalize_phone(raw_phone_text)
                    capacity_text = clean_text(item.get("capacity", ""))
                    if not capacity_text:
                        capacity_match = re.search(r"Capacity:\s*([0-9]+)", row_text, re.IGNORECASE)
                        capacity_text = capacity_match.group(1) if capacity_match else ""
                    age_text = clean_text(item.get("age", ""))
                    if not age_text:
                        age_match = re.search(
                            r"Ages Served:\s*(.+?)(?=\s+(?:Monday\s*-\s*Friday|In Operation:|Licensing Inspection Report|Program Quality Guide|Free PreK|Message to Families:|Program Details|$))",
                            row_text,
                            re.IGNORECASE,
                        )
                        age_text = clean_text(age_match.group(1)) if age_match else ""
                    results.append(
                        {
                            "provider_name": provider_name,
                            "address": address_line,
                            "city": parsed_city or city,
                            "zip": parsed_zip or (zip_match.group(0) if zip_match else ""),
                            "phone": phone_value,
                            "capacity": re.sub(r"^\s*Capacity\s*:?\s*", "", capacity_text, flags=re.IGNORECASE).strip(),
                            "age": re.sub(r"^\s*Ages?\s*:?\s*", "", age_text, flags=re.IGNORECASE).strip(),
                            "detail_url": "" if clean_text(item.get("detail_url", "")).startswith("javascript:") else clean_text(item.get("detail_url", "")),
                            "row_text": row_text,
                        }
                    )
                if results:
                    LOGGER.info(
                        "Michigan portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                self.finalize_state_portal_query("MI")
            LOGGER.info("Michigan portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("MI")
            raise

    def enrich_from_michigan_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_michigan_portal(record)
        except Exception:
            LOGGER.exception("Michigan portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_city_match = False
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(candidate.get("city", "")).lower()
            if city_match:
                score += 3
            if clean_text(candidate.get("phone", "")):
                score += 1
            if clean_text(candidate.get("capacity", "")):
                score += 1
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants[:4]
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "Michigan portal candidate scored %s for PID=%s provider=%s overlap=%.3f city_match=%s",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
                city_match,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate:
            return {}, {}
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            LOGGER.warning(
                "Michigan portal rejected best candidate for PID=%s provider=%s score=%s overlap=%.3f city_match=%s",
                record.get("PID", ""),
                best_candidate.get("provider_name", ""),
                best_score,
                best_overlap,
                best_city_match,
            )
            self.finalize_state_portal_query("MI")
            return {}, {}
        try:
            values = {
                "Mailing_Address": clean_text(best_candidate.get("address", "")),
                "Mailing_Zip": normalize_zip(best_candidate.get("zip", "")),
                "Telephone": normalize_phone(best_candidate.get("phone", "")),
                "URL": clean_text(best_candidate.get("detail_url", "")),
                "Capacity (optional)": clean_text(best_candidate.get("capacity", "")),
                "Age Range (optional)": normalize_age_groups_text_to_numeric_range(best_candidate.get("age", "")),
            }
            matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
            match_status, match_confidence, match_reason = classify_match_status(
                record,
                candidate_name=matched_provider_name,
                candidate_city=clean_text(best_candidate.get("city", "")),
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
            source_url = values.get("URL", "") or "https://greatstarttoquality.org/find-programs/"
            sources = {
                field: build_source_entry(
                    value=value,
                    source_url=source_url,
                    source_type="official_state_portal",
                    notes="Michigan Great Start to Quality portal",
                )
                for field, value in values.items()
                if clean_text(value)
            }
            return values, sources
        finally:
            self.finalize_state_portal_query("MI")

    def search_minnesota_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://www.parentaware.org/search/#/"
        no_results_text = "Showing 0 programs that match your search"
        try:
            for variant in profile.search_name_variants[:4]:
                driver = self.open_state_portal_query_tab("MN", portal_url)
                LOGGER.info(
                    "Minnesota portal searching PID=%s with provider_variant=%s city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                )
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="name-type"]'))
                )
                try:
                    by_name_label = driver.find_element(
                        By.XPATH,
                        "//label[@for='name-type']",
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", by_name_label)
                    time.sleep(0.3)
                    try:
                        by_name_label.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", by_name_label)
                    WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                        lambda d: d.execute_script(
                            """
const radio = document.getElementById('name-type');
const nameInput = document.getElementById('name');
return !!radio && radio.checked && !!nameInput;
"""
                        )
                    )
                except Exception:
                    LOGGER.debug("Minnesota By Name selector not clicked explicitly; continuing")
                name_input = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="name"]'))
                )
                name_input.send_keys(Keys.CONTROL, "a")
                name_input.send_keys(Keys.DELETE)
                name_input.send_keys(variant)
                search_button = driver.find_element(
                    By.XPATH,
                    "/html/body/main/div/div/div/div[2]/form/button",
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_button)
                time.sleep(0.3)
                try:
                    search_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", search_button)
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    lambda d: d.execute_script(
                        """
const bodyText = ((document.body && (document.body.innerText || document.body.textContent)) || '').trim();
if (bodyText.includes(arguments[0])) return true;
const container = document.querySelector('.search-results-list-container');
if (!container) return false;
return container.querySelectorAll('article.result-item').length > 0;
""",
                        no_results_text,
                    )
                )
                body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text)
                if no_results_text.lower() in body_text.lower():
                    LOGGER.info(
                        "Minnesota portal returned no matches for PID=%s using provider_variant=%s",
                        record.get("PID", ""),
                        variant,
                    )
                    self.finalize_state_portal_query("MN")
                    continue

                # Load the full result list before parsing.
                while True:
                    try:
                        load_more_buttons = driver.find_elements(By.XPATH, "//button[contains(., 'Load More')]")
                        visible_buttons = [button for button in load_more_buttons if button.is_displayed()]
                        if not visible_buttons:
                            break
                        button = visible_buttons[0]
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                        time.sleep(0.5)
                        existing_count = len(driver.find_elements(By.CSS_SELECTOR, "article.result-item"))
                        try:
                            button.click()
                        except Exception:
                            driver.execute_script("arguments[0].click();", button)
                        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                            lambda d: len(d.find_elements(By.CSS_SELECTOR, "article.result-item")) > existing_count
                            or not any(btn.is_displayed() for btn in d.find_elements(By.XPATH, "//button[contains(., 'Load More')]"))
                        )
                        time.sleep(1.0)
                    except Exception:
                        break

                candidate_rows = driver.execute_script(
                    """
const container = document.querySelector('.search-results-list-container');
if (!container) return [];
const cards = Array.from(container.querySelectorAll('article.result-item'));
return cards.map((card, index) => {
  const titleNode = card.querySelector('h2.title');
  const titleText = ((titleNode && (titleNode.innerText || titleNode.textContent)) || '').trim();
  const linkNode = titleNode ? titleNode.closest('a') : card.querySelector('a[href*="#/detail/"]');
  const detailHref = ((linkNode && linkNode.getAttribute('href')) || '').trim();
  const detailUrl = detailHref ? new URL(detailHref, window.location.href).href : '';
  const addressNode = card.querySelector('.address');
  const addressText = ((addressNode && (addressNode.innerText || addressNode.textContent)) || '').trim();
  return {
    candidate_index: index,
    provider_name: titleText,
    detail_url: detailUrl,
    address_text: addressText,
    row_text: (card.innerText || '').trim()
  };
});
"""
                )
                results: List[Dict[str, str]] = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    address_text = clean_text(item.get("address_text", ""))
                    if city and city.lower() not in address_text.lower():
                        continue
                    address_lines = [clean_text(line) for line in address_text.splitlines() if clean_text(line)]
                    street_line = address_lines[0] if address_lines else ""
                    city_state_zip_line = address_lines[1] if len(address_lines) > 1 else ""
                    phone_line = address_lines[2] if len(address_lines) > 2 else ""
                    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", city_state_zip_line)
                    results.append(
                        {
                            "candidate_index": str(item.get("candidate_index", "")),
                            "provider_name": provider_name,
                            "address": street_line,
                            "city": city,
                            "zip": zip_match.group(0) if zip_match else "",
                            "phone": normalize_phone(phone_line),
                            "detail_url": clean_text(item.get("detail_url", "")),
                            "row_text": clean_text(item.get("row_text", "")),
                        }
                    )
                if results:
                    LOGGER.info(
                        "Minnesota portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                self.finalize_state_portal_query("MN")
            LOGGER.info("Minnesota portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("MN")
            raise

    def fetch_minnesota_detail_page(
        self,
        driver: webdriver.Chrome,
        detail_url: str,
        action_label: str,
    ) -> Dict[str, str]:
        LOGGER.info("Fetching Minnesota detail page via Selenium action=%s url=%s", action_label, detail_url)
        existing_handles = set(driver.window_handles)
        driver.execute_script("window.open(arguments[0], '_blank');", detail_url)
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
        new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
        if new_handles:
            driver.switch_to.window(new_handles[-1])
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
            lambda d: d.find_elements(By.XPATH, "/html/body/main/div/div/div/div[2]/div[2]/div[1]/div")
            or d.find_elements(By.XPATH, "/html/body/main/div/div/div/div[2]/div[3]/dl[1]/dd[1]")
        )
        time.sleep(1.0)
        detail_values = driver.execute_script(
            """
const addressNode = document.evaluate('/html/body/main/div/div/div/div[2]/div[2]/div[1]/div', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
const phoneAnchor = document.querySelector('a[href^="tel:"]');
const websiteNode = document.evaluate('/html/body/main/div/div/div/div[2]/div[1]/div[1]/div[3]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
const capacityNode = document.evaluate('/html/body/main/div/div/div/div[2]/div[3]/dl[1]/dd[1]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
const ageNode = document.evaluate('/html/body/main/div/div/div/div[2]/div[3]/dl[1]/dd[2]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
const addressLines = (() => {
  if (!addressNode) return [];
  const lines = [];
  let current = '';
  for (const node of addressNode.childNodes) {
    if (node.nodeType === Node.TEXT_NODE) {
      current += node.textContent || '';
      continue;
    }
    if (node.nodeType === Node.ELEMENT_NODE && node.tagName === 'BR') {
      if ((current || '').trim()) {
        lines.push(current.trim());
      }
      current = '';
      continue;
    }
    current += node.textContent || '';
  }
  if ((current || '').trim()) {
    lines.push(current.trim());
  }
  return lines.filter(Boolean);
})();
return {
  address_html: addressNode ? addressNode.innerHTML : '',
  address_text: addressNode ? (addressNode.innerText || addressNode.textContent || '') : '',
  address_lines: addressLines,
  phone_text: phoneAnchor ? (phoneAnchor.innerText || phoneAnchor.textContent || '') : '',
  website_text: websiteNode ? (websiteNode.innerText || websiteNode.textContent || '') : '',
  website_href: (() => {
    if (!websiteNode) return '';
    const anchor = websiteNode.querySelector('a');
    return anchor ? (anchor.href || anchor.getAttribute('href') || '') : '';
  })(),
  capacity_text: capacityNode ? (capacityNode.innerText || capacityNode.textContent || '') : '',
  age_text: ageNode ? (ageNode.innerText || ageNode.textContent || '') : ''
};
"""
        ) or {}
        address_lines = [clean_text(line) for line in (detail_values.get("address_lines") or []) if clean_text(str(line))]
        if not address_lines:
            address_lines = [clean_text(line) for line in clean_text(detail_values.get("address_text", "")).splitlines() if clean_text(line)]
        street_line = address_lines[0] if address_lines else ""
        city_state_zip_line = address_lines[1] if len(address_lines) > 1 else ""
        city_state_zip_tokens = city_state_zip_line.split()
        zip_candidate = city_state_zip_tokens[-1] if city_state_zip_tokens else ""
        zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", zip_candidate) or re.search(r"\b\d{5}(?:-\d{4})?\b", city_state_zip_line)
        capacity_text = clean_text(detail_values.get("capacity_text", ""))
        age_text = clean_text(detail_values.get("age_text", ""))
        values = {
            "Mailing_Address": street_line,
            "Mailing_Zip": zip_match.group(0) if zip_match else "",
            "Telephone": normalize_phone(detail_values.get("phone_text", "")),
            "URL": clean_text(detail_values.get("website_href", "") or detail_values.get("website_text", "")),
            "Capacity (optional)": clean_text(capacity_text.split(" ", 1)[0] if capacity_text else ""),
            "Age Range (optional)": clean_text(age_text),
        }
        LOGGER.info("Minnesota detail page parsed url=%s values=%s", detail_url, values)
        return values

    def enrich_from_minnesota_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        portal_url = "https://www.parentaware.org/search/#/"
        try:
            candidates = self.search_minnesota_portal(record)
        except Exception:
            LOGGER.exception("Minnesota portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_city_match = False
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(candidate.get("city", "")).lower()
            if city_match:
                score += 3
            if clean_text(candidate.get("phone", "")):
                score += 1
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants[:4]
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "Minnesota portal candidate scored %s for PID=%s provider=%s overlap=%.3f city_match=%s",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
                city_match,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate:
            return {}, {}
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            LOGGER.warning(
                "Minnesota portal rejected best candidate for PID=%s provider=%s score=%s overlap=%.3f city_match=%s",
                record.get("PID", ""),
                best_candidate.get("provider_name", ""),
                best_score,
                best_overlap,
                best_city_match,
            )
            self.finalize_state_portal_query("MN")
            return {}, {}
        driver = self.get_state_portal_driver("MN")
        try:
            detail_values = self.fetch_minnesota_detail_page(
                driver=driver,
                detail_url=best_candidate.get("detail_url", ""),
                action_label=f"minnesota detail page [{record.get('PID', '')}]",
            )
        except Exception:
            LOGGER.exception("Minnesota detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            self.finalize_state_portal_query("MN")
        values = {
            "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "") or best_candidate.get("address", "")),
            "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "") or best_candidate.get("zip", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "") or best_candidate.get("phone", "")),
            "URL": clean_text(detail_values.get("URL", "")),
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", "")),
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(best_candidate.get("city", "")),
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
        source_url = values.get("URL", "") or best_candidate.get("detail_url", "") or portal_url
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes="Minnesota Parent Aware portal",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def search_new_hampshire_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch"
        no_results_text = "We're sorry we could not find any results based on this criteria. Please consider refining your search criteria and try again"
        try:
            for variant in profile.search_name_variants:
                driver = self.open_or_reuse_state_portal_query_tab(
                    "NH",
                    portal_url,
                    ready_locator=(By.XPATH, '//*[@id="j_id0:j_id3:j_id96:accountName"]'),
                )
                name_input = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="j_id0:j_id3:j_id96:accountName"]'))
                )
                city_select = driver.find_element(By.XPATH, '//*[@id="j_id0:j_id3:j_id96:city"]')
                name_input.send_keys(Keys.CONTROL, "a")
                name_input.send_keys(Keys.DELETE)
                name_input.send_keys(variant)

                selected_city = ""
                try:
                    options = city_select.find_elements(By.TAG_NAME, "option")
                    for option in options:
                        option_text = clean_text(option.text)
                        if option_text and option_text.lower() == city.lower():
                            option.click()
                            selected_city = option_text
                            break
                    if not selected_city:
                        for option in options:
                            if clean_text(option.get_attribute("value")) == "":
                                option.click()
                                break
                except Exception:
                    selected_city = ""

                LOGGER.info(
                    "New Hampshire portal searching PID=%s with provider_variant=%s city=%s selected_city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                    selected_city,
                )
                search_button = driver.find_element(
                    By.XPATH,
                    '//*[@id="j_id0:j_id3:j_id96"]/div[2]/section/div/div[2]/div/div/div[1]/div[5]/button[1]',
                )
                try:
                    search_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", search_button)
                time.sleep(1.0)
                no_result = no_results_text.lower() in clean_text(driver.find_element(By.TAG_NAME, "body").text).lower()
                if no_result:
                    self.finalize_state_portal_query("NH")
                    continue
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="accountTable"]'))
                )
                candidate_rows = driver.execute_script(
                    """
const rows = [];
document.querySelectorAll('#dtbody article > div > div').forEach((card, index) => {
  const link = card.querySelector('div p a');
  if (!link) return;
  rows.push({
    candidate_index: index,
    provider_name: (link.innerText || link.textContent || '').trim(),
    detail_url: link.href || link.getAttribute('href') || '',
    row_text: (card.innerText || '').trim()
  });
});
return rows;
"""
                )
                results = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    results.append(
                        {
                            "candidate_index": str(item.get("candidate_index", "")),
                            "provider_name": provider_name,
                            "detail_url": clean_text(item.get("detail_url", "")),
                            "row_text": clean_text(item.get("row_text", "")),
                        }
                    )
                if results:
                    LOGGER.info(
                        "New Hampshire portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                self.finalize_state_portal_query("NH")
            LOGGER.info("New Hampshire portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("NH")
            raise

    def fetch_new_hampshire_detail_page(
        self,
        driver: webdriver.Chrome,
        detail_url: str,
        action_label: str,
    ) -> Dict[str, str]:
        LOGGER.info("Fetching New Hampshire detail page via Selenium action=%s url=%s", action_label, detail_url)
        existing_handles = set(driver.window_handles)
        driver.execute_script("window.open(arguments[0], '_blank');", detail_url)
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
        new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
        if new_handles:
            driver.switch_to.window(new_handles[-1])
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="j_id0:j_id7:j_id98"]/div[1]/div/div/div/div[1]/div[1]/div/div/div[1]/div/p'))
        )
        address_lines = driver.execute_script(
            """
const node = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!node) return [];
const lines = [];
let current = '';
Array.from(node.childNodes).forEach((child) => {
  if (child.nodeName && child.nodeName.toLowerCase() === 'br') {
    if (current.trim()) lines.push(current.trim());
    current = '';
    return;
  }
  current += child.textContent || '';
});
if (current.trim()) lines.push(current.trim());
return lines;
""",
            '//*[@id="j_id0:j_id7:j_id98"]/div[1]/div/div/div/div[1]/div[1]/div/div/div[1]/div/p',
        ) or []
        website_href = ""
        try:
            website_href = driver.find_element(
                By.XPATH,
                '//*[@id="j_id0:j_id7:j_id98:j_id120"]/a',
            ).get_attribute("href") or ""
        except Exception:
            pass
        phone_text = clean_text(
            driver.find_element(
                By.XPATH,
                '//*[@id="j_id0:j_id7:j_id98"]/div[1]/div/div/div/div[1]/div[2]/div[1]/span[2]',
            ).text
        )
        capacity_text = ""
        try:
            capacity_text = clean_text(
                driver.find_element(
                    By.XPATH,
                    '//*[@id="j_id0:j_id7:j_id98"]/div[2]/div[1]/div[3]/div[1]/div/div[6]',
                ).text
            )
        except Exception:
            pass
        lines = [clean_text(str(line)) for line in address_lines if clean_text(str(line))]
        first_line = lines[0] if lines else ""
        second_line = lines[1] if len(lines) > 1 else ""
        values = {
            "Mailing_Address": first_line.replace(",", "").strip(),
            "Mailing_Zip": normalize_zip(second_line.split(" ")[-1] if second_line.split(" ") else ""),
            "Telephone": normalize_phone(phone_text),
            "URL": normalize_url(website_href),
            "Capacity (optional)": re.sub(r"^\D+", "", capacity_text).strip(),
            "Age Range (optional)": "",
            "Detail_URL": normalize_url(driver.current_url),
        }
        try:
            driver.close()
        except Exception:
            pass
        remaining_handles = driver.window_handles
        if remaining_handles:
            driver.switch_to.window(remaining_handles[0])
        LOGGER.info("New Hampshire detail page parsed url=%s values=%s", detail_url, values)
        return values

    def enrich_from_new_hampshire_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        portal_url = "https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch"
        try:
            candidates = self.search_new_hampshire_portal(record)
        except Exception:
            LOGGER.exception("New Hampshire portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "New Hampshire portal candidate scored %s for PID=%s provider=%s overlap=%.3f",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_overlap = recall
        if not best_candidate or (best_score < 6 and best_overlap < 0.35):
            return {}, {}
        driver = self.get_state_portal_driver("NH")
        try:
            detail_values = self.fetch_new_hampshire_detail_page(
                driver=driver,
                detail_url=best_candidate.get("detail_url", ""),
                action_label=f"new hampshire detail page [{record.get('PID', '')}]",
            )
        except Exception:
            LOGGER.exception("New Hampshire detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            self.finalize_state_portal_query("NH")
        values = {
            "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")),
            "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "")),
            "URL": clean_text(detail_values.get("URL", "")),
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": "",
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(record.get("Mailing_City", "")),
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
        source_url = clean_text(detail_values.get("Detail_URL", "")) or values.get("URL", "") or portal_url
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
            )
            for field, value in values.items()
            if field in OUTPUT_HEADERS and clean_text(value)
        }
        return values, sources

    def search_south_carolina_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        home_url = "https://search.sc-ccrr.org/search"
        try:
            session_flags = self.state_portal_session_flags.setdefault("SC", {})
            for variant in profile.search_name_variants[:4]:
                driver = self.open_or_reuse_state_portal_query_tab(
                    "SC",
                    home_url,
                    ready_locator=(By.ID, "formly_3_input_name_2"),
                )
                previous_signature = driver.execute_script(
                    """
return Array.from(document.querySelectorAll('app-program-public-search-result-card h3 .item, app-program-public-search-result-card h2.title'))
  .map((node) => (node.innerText || node.textContent || '').trim())
  .filter(Boolean)
  .join(' || ');
"""
                ) or ""
                name_input = driver.find_element(By.ID, "formly_3_input_name_2")
                location_input = driver.find_element(By.ID, "mat-input-36")
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", name_input)
                driver.execute_script("arguments[0].click();", name_input)
                name_input.send_keys(Keys.CONTROL, "a")
                name_input.send_keys(Keys.DELETE)
                name_input.send_keys(variant)

                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", location_input)
                driver.execute_script("arguments[0].click();", location_input)
                location_input.send_keys(Keys.CONTROL, "a")
                location_input.send_keys(Keys.DELETE)
                location_input.send_keys(city)

                try:
                    suggestion = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                        lambda d: next(
                            (
                                element
                                for element in d.find_elements(By.CSS_SELECTOR, ".pac-item")
                                if element.is_displayed()
                                and city.lower() in clean_text(element.text).lower()
                                and "sc" in clean_text(element.text).lower()
                            ),
                            None,
                        )
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", suggestion)
                    try:
                        suggestion.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", suggestion)
                    WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                        lambda d: city.lower() in clean_text(location_input.get_attribute("value")).lower()
                    )
                except Exception:
                    LOGGER.info(
                        "South Carolina portal city suggestion was not selected for PID=%s city=%s; continuing with typed location",
                        record.get("PID", ""),
                        city,
                    )

                if not session_flags.get("centers_checked"):
                    checkbox = driver.find_element(By.ID, "formly_4_checkboxes_publicProgramType_0_0-input")
                    if not checkbox.is_selected():
                        driver.execute_script("arguments[0].click();", checkbox)
                    session_flags["centers_checked"] = True

                LOGGER.info(
                    "South Carolina portal searching PID=%s with provider_variant=%s city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                )
                search_button = driver.find_element(By.CSS_SELECTOR, "button.search-btn")
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_button)
                try:
                    search_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", search_button)

                time.sleep(2.0)
                driver.execute_script("window.scrollTo(0, Math.max(700, document.body.scrollHeight * 0.45));")
                wait_timed_out = False
                try:
                    WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                        lambda d: (
                            "showing 0 programs that match your search" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower()
                            or "no results found" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower()
                            or len(d.find_elements(By.CSS_SELECTOR, "app-program-public-search-result-card")) > 0
                        )
                        and (
                            "showing 0 programs that match your search" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower()
                            or "no results found" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower()
                            or (
                                (
                                    d.execute_script(
                                        """
return Array.from(document.querySelectorAll('app-program-public-search-result-card h3 .item, app-program-public-search-result-card h2.title'))
  .map((node) => (node.innerText || node.textContent || '').trim())
  .filter(Boolean)
  .join(' || ');
"""
                                    )
                                    or ""
                                )
                                != previous_signature
                                or not previous_signature
                            )
                        )
                    )
                except TimeoutException:
                    wait_timed_out = True
                    LOGGER.info(
                        "South Carolina portal wait timed out for PID=%s using provider_variant=%s; inspecting current DOM before skipping",
                        record.get("PID", ""),
                        variant,
                    )
                body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text).lower()
                if "showing 0 programs that match your search" in body_text or "no results found" in body_text:
                    LOGGER.info(
                        "South Carolina portal returned no rows for PID=%s using provider_variant=%s",
                        record.get("PID", ""),
                        variant,
                    )
                    continue

                candidate_rows = driver.execute_script(
                    """
const rows = [];
Array.from(document.querySelectorAll('app-program-public-search-result-card')).forEach((card, index) => {
  const titleNode = card.querySelector('h3 .item, h2.title');
  const streetNode = card.querySelector('app-view-address-block .street-number');
  const cityNode = card.querySelector('app-view-address-block .city');
  const stateNode = card.querySelector('app-view-address-block .state');
  const zipNode = card.querySelector('app-view-address-block .zip');
  const phoneNode = card.querySelector('app-view-contact-block .phone-display a, app-view-contact-block a[href^="tel:"]');
  const addressText = [
    (streetNode && (streetNode.innerText || streetNode.textContent || '')) || '',
    [cityNode && (cityNode.innerText || cityNode.textContent || ''), stateNode && (stateNode.innerText || stateNode.textContent || ''), zipNode && (zipNode.innerText || zipNode.textContent || '')].filter(Boolean).join(' ')
  ].filter(Boolean).join('\n');
  const profileButton = Array.from(card.querySelectorAll('a,button')).find(
    (element) => /view profile/i.test((element.innerText || element.textContent || '').trim())
  );
  rows.push({
    candidate_index: index,
    provider_name: titleNode ? (titleNode.innerText || titleNode.textContent || '') : '',
    address_text: addressText,
    phone_text: phoneNode ? (phoneNode.innerText || phoneNode.textContent || '') : '',
    has_profile_button: !!profileButton
  });
});
return rows;
"""
                )
                results: List[Dict[str, str]] = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    address_text = clean_text(item.get("address_text", ""))
                    if not provider_name or not bool(item.get("has_profile_button")):
                        continue
                    results.append(
                        {
                            "candidate_index": str(item.get("candidate_index", "")),
                            "provider_name": provider_name,
                            "address": address_text,
                            "city": city if city and city.lower() in address_text.lower() else "",
                            "zip": normalize_zip(address_text),
                            "phone": normalize_phone(item.get("phone_text", "")),
                            "row_text": address_text,
                        }
                    )
                if results:
                    if wait_timed_out:
                        LOGGER.info(
                            "South Carolina portal recovered %s candidate rows from timed-out DOM for PID=%s using provider_variant=%s",
                            len(results),
                            record.get("PID", ""),
                            variant,
                        )
                    LOGGER.info(
                        "South Carolina portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                LOGGER.info(
                    "South Carolina portal saw rendered results but parsed 0 candidate rows for PID=%s using provider_variant=%s",
                    record.get("PID", ""),
                    variant,
                )
            LOGGER.info("South Carolina portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("SC")
            raise

    def fetch_south_carolina_detail_page(
        self,
        driver: webdriver.Chrome,
        candidate_index: str,
        provider_name: str,
        action_label: str,
    ) -> Dict[str, str]:
        LOGGER.info(
            "Fetching South Carolina detail page via Selenium action=%s candidate_index=%s provider=%s",
            action_label,
            candidate_index,
            provider_name,
        )
        index = int(candidate_index)
        profile_button = driver.execute_script(
            """
const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
const target = normalize(arguments[0]);
const cards = Array.from(document.querySelectorAll('app-program-public-search-result-card'));
let card = cards.find((item) => {
  const title = normalize(item.querySelector('h3 .item, h2.title')?.innerText || item.querySelector('h3 .item, h2.title')?.textContent || '');
  return title === target || (target && title.includes(target));
});
if (!card && Number.isInteger(arguments[1]) && arguments[1] >= 0 && arguments[1] < cards.length) {
  card = cards[arguments[1]];
}
if (!card) return null;
return card.querySelector('button.profile-btn') || Array.from(card.querySelectorAll('a,button')).find((element) => /view profile/i.test((element.innerText || element.textContent || '').trim())) || null;
""",
            provider_name,
            index,
        )
        if profile_button is None:
            raise RuntimeError(
                f"Unable to locate South Carolina View Profile button for provider={provider_name} candidate_index={candidate_index}"
            )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", profile_button)
        try:
            profile_button.click()
        except Exception:
            driver.execute_script("arguments[0].click();", profile_button)

        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
            lambda d: "program address" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower()
            and "contact info" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower()
        )
        driver.execute_script("window.scrollTo(0, Math.max(900, document.body.scrollHeight * 0.35));")
        time.sleep(2.0)
        detail_values = driver.execute_script(
            """
const normalize = (value) => (value || '').replace(/\u00a0/g, ' ').replace(/\s+\n/g, '\n').replace(/\n\s+/g, '\n').replace(/[ \t]+/g, ' ').trim();
const getSectionText = (label) => {
  const matches = Array.from(document.querySelectorAll('*')).filter((node) => normalize(node.innerText || node.textContent || '').toLowerCase() === label.toLowerCase());
  for (let idx = matches.length - 1; idx >= 0; idx -= 1) {
    let current = matches[idx].parentElement;
    while (current) {
      const text = normalize(current.innerText || current.textContent || '');
      if (text && text.toLowerCase().includes(label.toLowerCase()) && text.length > label.length + 5) {
        return text;
      }
      current = current.parentElement;
    }
  }
  return '';
};
const bodyText = normalize(document.body.innerText || document.body.textContent || '');
const addressBlock = getSectionText('Program Address');
const contactBlock = getSectionText('Contact Info');
const onlineBlock = getSectionText('Find Us Online!');
const ageBlock = getSectionText('We currently serve the following age groups:');
const enrollmentMatch = bodyText.match(/Total Enrollment\s*(\d+)/i);
return {
  address_block: addressBlock,
  contact_block: contactBlock,
  online_block: onlineBlock,
  age_block: ageBlock,
  enrollment: enrollmentMatch ? enrollmentMatch[1] : '',
  detail_url: window.location.href || ''
};
"""
        ) or {}
        address_lines = [
            clean_text(line)
            for line in clean_text(detail_values.get("address_block", "")).splitlines()
            if clean_text(line)
            and clean_text(line).lower() not in {"program address", "click for directions"}
            and "county" not in clean_text(line).lower()
        ]
        mailing_address = address_lines[0] if address_lines else ""
        zip_source = address_lines[1] if len(address_lines) > 1 else ""
        phone_value = normalize_phone(detail_values.get("contact_block", ""))
        online_match = re.search(r"https?://\S+", clean_text(detail_values.get("online_block", "")))
        age_lines = [clean_text(line) for line in clean_text(detail_values.get("age_block", "")).splitlines() if clean_text(line)]
        accepted_rows: List[str] = []
        for idx, line in enumerate(age_lines):
            if line.lower() == "accepted" and idx > 0:
                accepted_rows.append(age_lines[idx - 1])

        def extract_age_range_portion(label: str) -> Tuple[str, str]:
            match = re.search(r"\(([^)]+)\)", label)
            if not match:
                return "", ""
            range_text = clean_text(match.group(1))
            pieces = [clean_text(piece) for piece in re.split(r"\s*-\s*", range_text) if clean_text(piece)]
            if len(pieces) < 2:
                return "", ""
            lower = pieces[0]
            upper = pieces[-1]
            if not re.search(r"[A-Za-z]", lower):
                lower = f"{lower} {upper.split()[-1]}"
            return lower, upper

        first_lower, _ = extract_age_range_portion(accepted_rows[0]) if accepted_rows else ("", "")
        _, last_upper = extract_age_range_portion(accepted_rows[-1]) if accepted_rows else ("", "")
        age_value = clean_text(f"{first_lower} - {last_upper}") if first_lower and last_upper else ""
        values = {
            "Mailing_Address": mailing_address,
            "Mailing_Zip": normalize_zip(zip_source),
            "Telephone": phone_value,
            "URL": clean_text(online_match.group(0) if online_match else ""),
            "Capacity (optional)": clean_text(detail_values.get("enrollment", "")),
            "Age Range (optional)": age_value,
            "Detail_URL": clean_text(detail_values.get("detail_url", "")),
        }
        try:
            close_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[5]/button"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", close_button)
            try:
                close_button.click()
            except Exception:
                driver.execute_script("arguments[0].click();", close_button)
            WebDriverWait(driver, 10).until(
                lambda d: not d.find_elements(By.XPATH, "/html/body/div[5]/button")
                or not any(element.is_displayed() for element in d.find_elements(By.XPATH, "/html/body/div[5]/button"))
            )
        except Exception:
            LOGGER.info("South Carolina profile close button was not dismissed cleanly; continuing with reused tab")
        LOGGER.info("South Carolina detail page parsed url=%s values=%s", driver.current_url, values)
        return values

    def enrich_from_south_carolina_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        portal_url = "https://search.sc-ccrr.org/search"
        try:
            candidates = self.search_south_carolina_portal(record)
        except Exception:
            LOGGER.exception("South Carolina portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_city_match = False
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(candidate.get("city", "")).lower()
            if city_match:
                score += 3
            if clean_text(candidate.get("phone", "")):
                score += 1
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants[:4]
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "South Carolina portal candidate scored %s for PID=%s provider=%s overlap=%.3f city_match=%s",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
                city_match,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate:
            return {}, {}
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            LOGGER.warning(
                "South Carolina portal rejected best candidate for PID=%s provider=%s score=%s overlap=%.3f city_match=%s",
                record.get("PID", ""),
                best_candidate.get("provider_name", ""),
                best_score,
                best_overlap,
                best_city_match,
            )
            return {}, {}
        driver = self.get_state_portal_driver("SC")
        try:
            detail_values = self.fetch_south_carolina_detail_page(
                driver=driver,
                candidate_index=best_candidate.get("candidate_index", ""),
                provider_name=best_candidate.get("provider_name", ""),
                action_label=f"south carolina detail page [{record.get('PID', '')}]",
            )
        except Exception:
            LOGGER.exception("South Carolina detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        values = {
            "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")),
            "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "")),
            "URL": clean_text(detail_values.get("URL", "")),
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", "")),
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(best_candidate.get("city", "")),
            candidate_address=values.get("Mailing_Address", "") or best_candidate.get("address", ""),
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
        source_url = clean_text(detail_values.get("Detail_URL", "")) or values.get("URL", "") or portal_url
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes="South Carolina CCR&R portal",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def build_maryland_city_slug(self, city: str) -> str:
        slug = clean_text(city).lower()
        slug = re.sub(r"[^a-z0-9]+", "-", slug)
        return slug.strip("-")

    def search_maryland_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        city_slug = self.build_maryland_city_slug(city)
        portal_url = f"https://locatesearch.marylandfamilynetwork.org/city/{city_slug}-md"
        try:
            for variant in profile.search_name_variants:
                driver = self.open_or_reuse_state_portal_query_tab(
                    "MD",
                    portal_url,
                    ready_locator=(By.XPATH, "/html/body/div[8]/div[3]/div[2]/div[1]/div[1]/div[2]/div/div[42]"),
                )
                if city_slug not in clean_text(driver.current_url).lower():
                    driver.get(portal_url)
                    WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                        EC.presence_of_element_located((By.XPATH, "/html/body/div[8]/div[3]/div[2]/div[1]/div[1]/div[2]/div/div[42]"))
                    )
                search_input = None
                try:
                    search_input = driver.find_element(By.XPATH, '//*[@id="searchBiz1"]')
                except Exception:
                    trigger = driver.find_element(By.XPATH, "/html/body/div[8]/div[3]/div[2]/div[1]/div[1]/div[2]/div/div[42]")
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", trigger)
                    try:
                        trigger.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", trigger)
                    search_input = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="searchBiz1"]'))
                    )
                else:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_input)
                    try:
                        search_input.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", search_input)
                search_input = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="searchBiz1"]'))
                )
                search_input.send_keys(Keys.CONTROL, "a")
                search_input.send_keys(Keys.DELETE)
                search_input.send_keys(variant)
                LOGGER.info(
                    "Maryland portal searching PID=%s with provider_variant=%s city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                )
                time.sleep(1.0)
                candidate_rows = []
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "/html/body/div[29]/div[2]/div[1]/div/div[2]"))
                    )
                    candidate_rows = driver.execute_script(
                    """
const root = document.evaluate('/html/body/div[29]/div[2]/div[1]/div/div[2]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!root) return [];
return Array.from(root.children).map((item, index) => {
  const nameNode = item.querySelector('.bubble-element.Text div') || item.querySelector('.bubble-element.Text');
  return {
    candidate_index: index,
    provider_name: (nameNode && (nameNode.innerText || nameNode.textContent || '')) || ''
  };
}).filter((item) => item.provider_name);
"""
                    )
                except Exception:
                    candidate_rows = []
                if candidate_rows:
                    LOGGER.info(
                        "Maryland portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(candidate_rows),
                        record.get("PID", ""),
                        variant,
                    )
                    return [{"variant": variant, **item} for item in candidate_rows]
            LOGGER.info("Maryland portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("MD")
            raise

    def fetch_maryland_detail_page(
        self,
        driver: webdriver.Chrome,
        candidate_index: str,
        provider_name: str,
        action_label: str,
    ) -> Dict[str, str]:
        LOGGER.info(
            "Fetching Maryland detail page via Selenium action=%s candidate_index=%s provider=%s",
            action_label,
            candidate_index,
            provider_name,
        )
        original_handles = set(driver.window_handles)
        original_handle = driver.current_window_handle
        clicked = driver.execute_script(
            """
const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
const target = normalize(arguments[0]);
const index = arguments[1];
const root = document.evaluate('/html/body/div[29]/div[2]/div[1]/div/div[2]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!root) return false;
const children = Array.from(root.children);
let row = children.find((item) => {
  const nameNode = item.querySelector('.bubble-element.Text div') || item.querySelector('.bubble-element.Text');
  const text = normalize(nameNode ? (nameNode.innerText || nameNode.textContent || '') : '');
  return text === target || (target && text.includes(target));
});
if (!row && index >= 0 && index < children.length) row = children[index];
if (!row) return false;
const clickable = row.querySelector('.clickable-element') || row;
clickable.click();
return true;
""",
            provider_name,
            int(candidate_index),
        )
        if not clicked:
            raise RuntimeError(f"Unable to click Maryland suggestion for provider={provider_name}")
        try:
            WebDriverWait(driver, 5).until(lambda d: len(d.window_handles) != len(original_handles))
        except Exception:
            pass
        current_handles = driver.window_handles
        new_handles = [handle for handle in current_handles if handle not in original_handles]
        if new_handles:
            driver.switch_to.window(new_handles[-1])
        else:
            try:
                driver.switch_to.window(original_handle)
            except Exception:
                if current_handles:
                    driver.switch_to.window(current_handles[-1])
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]"))
        )
        try:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[25]/div"))
            )
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(1.0)
        except Exception:
            pass
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[2]/div")
            )
        )
        phone_href = ""
        website_href = ""
        address_text = ""
        try:
            phone_href = driver.find_element(By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[3]/div/a").get_attribute("href") or ""
        except Exception:
            pass
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(
                    (By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[5]/div/a")
                )
            )
            website_href = driver.find_element(
                By.XPATH,
                "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[5]/div/a",
            ).get_attribute("href") or ""
        except Exception:
            try:
                website_href = driver.execute_script(
                    """
const node = document.evaluate('/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[5]/div/a', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
return node ? (node.href || node.getAttribute('href') || '') : '';
"""
                ) or ""
            except Exception:
                pass
        try:
            address_text = clean_text(driver.find_element(By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[2]/div").text)
        except Exception:
            pass
        address_segments = [clean_text(part) for part in address_text.split(",") if clean_text(part)]
        street_address = address_segments[0] if address_segments else ""
        zip_source = address_segments[-1].split(" ")[-1] if address_segments and address_segments[-1].split(" ") else ""
        values = {
            "Mailing_Address": street_address,
            "Mailing_Zip": normalize_zip(zip_source),
            "Telephone": normalize_phone(phone_href),
            "URL": normalize_url(website_href),
            "Capacity (optional)": "",
            "Age Range (optional)": "",
            "Detail_URL": normalize_url(driver.current_url),
        }
        LOGGER.info("Maryland detail page parsed url=%s values=%s", driver.current_url, values)
        return values

    def enrich_from_maryland_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        portal_url = f"https://locatesearch.marylandfamilynetwork.org/city/{self.build_maryland_city_slug(clean_text(record.get('Mailing_City')))}-md"
        try:
            candidates = self.search_maryland_portal(record)
        except Exception:
            LOGGER.exception("Maryland portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "Maryland portal candidate scored %s for PID=%s provider=%s overlap=%.3f",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_overlap = recall
        if not best_candidate or (best_score < 6 and best_overlap < 0.35):
            return {}, {}
        driver = self.get_state_portal_driver("MD")
        try:
            detail_values = self.fetch_maryland_detail_page(
                driver=driver,
                candidate_index=best_candidate.get("candidate_index", ""),
                provider_name=best_candidate.get("provider_name", ""),
                action_label=f"maryland detail page [{record.get('PID', '')}]",
            )
        except Exception:
            LOGGER.exception("Maryland detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            self.finalize_state_portal_query("MD")
        values = {
            "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")),
            "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "")),
            "URL": clean_text(detail_values.get("URL", "")),
            "Capacity (optional)": "",
            "Age Range (optional)": "",
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(record.get("Mailing_City", "")),
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
        source_url = clean_text(detail_values.get("Detail_URL", "")) or values.get("URL", "") or portal_url
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes="Maryland Family Network locate search",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def search_oklahoma_portal(self, record: Dict[str, str]) -> List[Dict[str, str]]:
        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        home_url = "https://ccl.dhs.ok.gov/providers"
        try:
            for variant in profile.search_name_variants[:4]:
                query_url = f"https://ccl.dhs.ok.gov/providers?provider-name={quote_plus(variant)}"
                driver = self.open_state_portal_query_tab("OK", query_url)
                LOGGER.info(
                    "Loading Oklahoma portal query for PID=%s provider_variant=%s url=%s",
                    record.get("PID", ""),
                    variant,
                    query_url,
                )
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    EC.presence_of_element_located((By.XPATH, "/html/body/div/main/form/div[4]/div[1]/ul"))
                )
                time.sleep(1.5)
                candidate_rows = driver.execute_script(
                    """
const list = document.querySelector('body div main form div:nth-of-type(4) div:nth-of-type(1) ul');
if (!list) return [];
const rows = [];
Array.from(list.querySelectorAll('li')).forEach((item, candidateIndex) => {
  const spans = item.querySelectorAll('span');
  const citySpan = spans[0] || null;
  const actionButton = item.querySelector('span:nth-of-type(2) button, button');
  const buttonText = ((actionButton && actionButton.innerText) || '').trim();
  const cityRaw = ((citySpan && citySpan.innerText) || '').trim();
  const providerName = buttonText;
  const rowText = (item.innerText || '').trim();
  let cityValue = '';
  const rowParts = rowText.split(/\s+/).filter(Boolean);
  if (rowParts.length > 3) {
    cityValue = rowParts[3].replace(/,\s*$/, '').trim();
  } else {
    const cityMatch = rowText.match(/([A-Z][A-Z\\s.'-]+),\\s*OK\\b/i);
    cityValue = cityMatch ? cityMatch[1].trim() : '';
  }
  rows.push({
    candidate_index: candidateIndex,
    provider_name: providerName,
    city_raw: cityRaw,
    city: cityValue,
    row_text: rowText
  });
});
return rows;
"""
                )
                results: List[Dict[str, str]] = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    result_city = clean_text(item.get("city", ""))
                    results.append(
                        {
                            "provider_name": provider_name,
                            "city": result_city,
                            "row_text": clean_text(item.get("row_text", "")),
                            "candidate_index": str(item.get("candidate_index", "")),
                        }
                    )
                if results:
                    LOGGER.info(
                        "Oklahoma portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                self.finalize_state_portal_query("OK")
            LOGGER.info("Oklahoma portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            self.reset_state_portal_driver("OK")
            raise

    def fetch_oklahoma_detail_page(
        self,
        driver: webdriver.Chrome,
        candidate_index: str,
        action_label: str,
        record: Dict[str, str],
    ) -> Dict[str, str]:
        LOGGER.info(
            "Fetching Oklahoma detail page via Selenium action=%s candidate_index=%s",
            action_label,
            candidate_index,
        )
        list_root = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div/main/form/div[4]/div[1]/ul"))
        )
        items = list_root.find_elements(By.XPATH, "./li")
        index = int(candidate_index)
        if index < 0 or index >= len(items):
            raise RuntimeError(f"Oklahoma candidate_index={candidate_index} is out of bounds for {len(items)} rows")
        detail_link_xpath = f"/html/body/div/main/form/div[4]/div[1]/ul/li[{index + 1}]/div/a"
        detail_link = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, detail_link_xpath))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_link)
        time.sleep(0.5)
        try:
            detail_link.click()
        except Exception:
            driver.execute_script("arguments[0].click();", detail_link)

        phone_xpath = "/html/body/div/main/div/div[2]/div/div/div[1]"
        address_xpath = "/html/body/div/main/div/div[2]/div/div/div[3]"
        capacity_xpath = "/html/body/div/main/div/div[1]/section[2]/section[2]"

        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
            EC.presence_of_element_located((By.XPATH, phone_xpath))
        )
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
            EC.presence_of_element_located((By.XPATH, address_xpath))
        )
        time.sleep(1.0)

        phone_text = normalize_phone(driver.find_element(By.XPATH, phone_xpath).text)
        address_text = clean_text(driver.find_element(By.XPATH, address_xpath).text)
        capacity_text = ""
        try:
            capacity_text = clean_text(driver.find_element(By.XPATH, capacity_xpath).text)
            capacity_text = re.sub(r"^\s*Total\s+Capacity\s*", "", capacity_text, flags=re.IGNORECASE).strip(" :-")
        except Exception:
            capacity_text = ""

        detail_values = {
            "Mailing_Address": address_text,
            "Mailing_Zip": normalize_zip(address_text),
            "Telephone": phone_text,
            "Detail_URL": normalize_url(driver.current_url),
            "Capacity (optional)": capacity_text,
            "Age Range (optional)": "",
        }
        LOGGER.info("Oklahoma detail page parsed url=%s values=%s", driver.current_url, detail_values)
        return detail_values

    def enrich_from_oklahoma_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        try:
            candidates = self.search_oklahoma_portal(record)
        except Exception:
            LOGGER.exception("Oklahoma portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_city_match = False
        best_overlap = 0.0
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(candidate.get("city", "")).lower()
            if city_match:
                score += 3
            variant_hit = any(
                clean_text(variant)
                and len(clean_text(variant)) >= 4
                and clean_text(variant).lower() in provider_name.lower()
                for variant in profile.search_name_variants[:4]
            )
            if variant_hit:
                score += 4
            LOGGER.info(
                "Oklahoma portal candidate scored %s for PID=%s provider=%s overlap=%.3f city_match=%s record_city=%s candidate_city=%s row_text=%s",
                score,
                record.get("PID", ""),
                provider_name,
                recall,
                city_match,
                clean_text(record.get("Mailing_City", "")),
                clean_text(candidate.get("city", "")),
                clean_text(candidate.get("row_text", "")),
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate:
            return {}, {}
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            LOGGER.warning(
                "Oklahoma portal rejected best candidate for PID=%s provider=%s score=%s overlap=%.3f city_match=%s",
                record.get("PID", ""),
                best_candidate.get("provider_name", ""),
                best_score,
                best_overlap,
                best_city_match,
            )
            return {}, {}
        driver = self.get_state_portal_driver("OK")
        try:
            detail_values = self.fetch_oklahoma_detail_page(
                driver=driver,
                candidate_index=best_candidate.get("candidate_index", ""),
                action_label=f"oklahoma detail page [{record.get('PID', '')}]",
                record=record,
            )
        except Exception:
            LOGGER.exception("Oklahoma detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            self.finalize_state_portal_query("OK")
        values = {
            "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")),
            "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "") or detail_values.get("Mailing_Address", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "")),
            "URL": "",
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": "",
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(best_candidate.get("city", "")),
            candidate_address=values.get("Mailing_Address", ""),
            candidate_phone=values.get("Telephone", ""),
            candidate_url=clean_text(detail_values.get("Detail_URL", "")),
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = clean_text(detail_values.get("Detail_URL", "")) or "https://ccl.dhs.ok.gov/providers"
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes="Oklahoma official childcare portal",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def wait_for_massachusetts_component(self, driver: webdriver.Chrome, tag_name: str) -> None:
        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
            lambda d: d.execute_script(f"return !!document.querySelector('{tag_name}')")
        )

    def execute_massachusetts_search(
        self,
        driver: webdriver.Chrome,
        provider_name: str,
        city: str,
        zip_code: str,
    ) -> List[PortalSearchResult]:
        LOGGER.info(
            "Executing Massachusetts portal search provider=%s city=%s zip=%s",
            provider_name,
            city,
            zip_code,
        )
        script = """
const root = document.querySelector('c-eec_child-care-search').shadowRoot;
function setValue(selector, value) {
  const el = root.querySelector(selector);
  if (!el) return;
  el.value = value;
  el.dispatchEvent(new Event('input', {bubbles:true, composed:true}));
  el.dispatchEvent(new Event('change', {bubbles:true, composed:true}));
}
root.querySelectorAll('lightning-button')[0].shadowRoot.querySelector('button').click();
setValue('input[name="providerName"]', arguments[0]);
setValue('input[name="selectedCity"]', arguments[1]);
setValue('input[name="selectedZipCode"]', arguments[2]);
root.querySelectorAll('lightning-button')[1].shadowRoot.querySelector('button').click();
return true;
"""
        driver.execute_script(script, provider_name, city, zip_code)
        time.sleep(8)
        shadow_html = driver.execute_script(
            "return document.querySelector('c-eec_child-care-search').shadowRoot.innerHTML;"
        )
        soup = BeautifulSoup(shadow_html, "html.parser")
        results: List[PortalSearchResult] = []
        for block in soup.select(".address-block"):
            name_node = block.select_one("a.school-name")
            if not name_node:
                continue
            detail_path = clean_text(name_node.get("data-name"))
            title = clean_text(name_node.get("aria-label") or name_node.get_text(" ", strip=True))
            spans = block.select(".input-group span[aria-label]")
            address = clean_text(spans[0].get("aria-label")) if len(spans) >= 1 else ""
            program_type = clean_text(spans[1].get("aria-label")) if len(spans) >= 2 else ""
            if detail_path:
                results.append(
                    PortalSearchResult(
                        title=title,
                        detail_url=normalize_url(urljoin("https://childcare.mass.gov", detail_path)),
                        address=address,
                        program_type=program_type,
                    )
                )
        LOGGER.info("Massachusetts portal search produced %s candidates", len(results))
        return results

    def search_massachusetts_portal(self, record: Dict[str, str]) -> List[PortalSearchResult]:
        driver = self.get_search_driver()
        LOGGER.info("Loading Massachusetts official portal for PID=%s", record.get("PID", ""))
        driver.get(STATE_PORTAL_URLS["MA"])
        self.wait_for_massachusetts_component(driver, "c-eec_child-care-search")
        time.sleep(5)

        provider_name = get_record_name_profile(record).search_name_primary
        city = clean_text(record.get("Mailing_City"))
        zip_code = clean_text(record.get("Mailing_Zip"))
        search_variants = [
            (provider_name, city, zip_code),
            (provider_name, city, ""),
            ("", city, zip_code),
            ("", city, ""),
        ]
        for variant_provider, variant_city, variant_zip in search_variants:
            results = self.execute_massachusetts_search(driver, variant_provider, variant_city, variant_zip)
            if results:
                LOGGER.info(
                    "Massachusetts portal returned %s candidate results for PID=%s",
                    len(results),
                    record.get("PID", ""),
                )
                return results
        LOGGER.info("Massachusetts portal returned 0 candidate results for PID=%s", record.get("PID", ""))
        return []

    def parse_massachusetts_detail(self, detail_url: str) -> Dict[str, str]:
        driver = self.get_search_driver()
        LOGGER.info("Opening Massachusetts provider detail page %s", detail_url)
        driver.get(detail_url)
        self.wait_for_massachusetts_component(driver, "c-eec_provider-details")
        time.sleep(5)
        shadow_html = driver.execute_script(
            "return document.querySelector('c-eec_provider-details').shadowRoot.innerHTML;"
        )
        soup = BeautifulSoup(shadow_html, "html.parser")

        result = {
            "Mailing_Address": clean_text((soup.select_one(".account-address") or {}).get_text(" ", strip=True) if soup.select_one(".account-address") else ""),
            "Mailing_Zip": "",
            "Telephone": "",
            "URL": "",
            "Capacity (optional)": "",
            "Age Range (optional)": "",
            "Email": "",
        }

        tel = soup.select_one('a[href^="tel:"]')
        if tel:
            result["Telephone"] = normalize_phone(tel.get_text(" ", strip=True))
        email = soup.select_one('a[href^="mailto:"]')
        if email:
            result["Email"] = clean_text(email.get_text(" ", strip=True))
        result["Mailing_Zip"] = normalize_zip(result["Mailing_Address"])

        all_info = soup.select(".view-only-info")
        for block in all_info:
            label = clean_text((block.select_one("label") or {}).get_text(" ", strip=True) if block.select_one("label") else "")
            value = clean_text((block.select_one(".read-only-info") or {}).get_text(" ", strip=True) if block.select_one(".read-only-info") else "")
            if label.startswith("Capacity"):
                result["Capacity (optional)"] = value

        age_groups = []
        for cell in soup.select("td.slds-cell-wrap"):
            text = clean_text(cell.get_text(" ", strip=True))
            if text in {
                "Infant Age Group",
                "Toddler Age Group",
                "Preschool Age Group",
                "School Age Group",
                "Kindergarten Age Group",
            }:
                age_groups.append(text.replace(" Age Group", ""))
        result["Age Range (optional)"] = age_groups_to_numeric_range(age_groups)

        for link in soup.select('a[href^="http"]'):
            href = normalize_url(link.get("href"))
            if not href:
                continue
            domain = domain_of(href)
            if domain in {"mass.gov", "google.com", "www.google.com"}:
                continue
            result["URL"] = href
            break

        return result

    def enrich_from_massachusetts_portal(
        self, record: Dict[str, str]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        candidates = self.search_massachusetts_portal(record)
        if not candidates:
            return {}, {}

        city = clean_text(record.get("Mailing_City"))
        best = None
        best_score = -999
        for candidate in candidates[:10]:
            combined = f"{candidate.title} {candidate.address} {candidate.program_type}"
            score = token_overlap_score(record.get("Daycare_Name", ""), combined) * 3
            if city and city.lower() in combined.lower():
                score += 2
            if clean_text(record.get("Mailing_State")) == "MA":
                score += 1
            if score > best_score:
                best_score = score
                best = candidate

        if not best or best_score < 7:
            LOGGER.warning(
                "Massachusetts adapter rejected candidates for PID=%s because best score=%s was below threshold",
                record.get("PID", ""),
                best_score,
            )
            return {}, {}

        detail = self.parse_massachusetts_detail(best.detail_url)
        values = {
            "Mailing_Address": detail.get("Mailing_Address", ""),
            "Mailing_Zip": detail.get("Mailing_Zip", ""),
            "Telephone": detail.get("Telephone", ""),
            "URL": detail.get("URL", ""),
            "Capacity (optional)": detail.get("Capacity (optional)", ""),
            "Age Range (optional)": detail.get("Age Range (optional)", ""),
        }
        sources = {
            field: build_source_entry(
                value=value,
                source_url=best.detail_url,
                source_type="official_state_portal",
                notes=(
                    "Massachusetts EEC portal; age range normalized from official age-group categories"
                    if field == "Age Range (optional)"
                    else "Massachusetts EEC portal"
                ),
            )
            for field, value in values.items()
            if clean_text(value)
        }
        if clean_text(detail.get("Email")):
            sources["Email"] = build_source_entry(
                value=detail["Email"],
                source_url=best.detail_url,
                source_type="official_state_portal",
                notes="Massachusetts EEC portal",
            )
        LOGGER.info(
            "Massachusetts adapter selected %s for PID=%s with score=%s",
            best.detail_url,
            record.get("PID", ""),
            best_score,
        )
        return values, sources

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
        if RUN_GOOGLE_ONLY_SAMPLE_MODE or SINGLE_PID_FILTER:
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

        if RUN_GOOGLE_ONLY_SAMPLE_MODE:
            LOGGER.info("PID=%s Google-only sample mode enabled; skipping all API/state connector calls", pid)
            self.enrich_from_google_api_miss_fallback(search_record, base_output, field_sources)
            base_output["Telephone"] = normalize_phone(base_output.get("Telephone"))
            base_output["Mailing_Zip"] = normalize_zip(base_output.get("Mailing_Zip"))
            base_output["URL"] = normalize_url(base_output.get("URL"))
            for field in ("Telephone", "Mailing_Zip", "URL"):
                if field in field_sources:
                    field_sources[field]["value"] = base_output.get(field, "")
            if not row_has_found_data(base_output) and not base_output.get("Match_Status"):
                base_output["Match_Status"] = "not_found"
                base_output["Match_Confidence"] = "0"
                base_output["Matched_Reason"] = "Google-only sample mode did not find any datapoints."
            self.set_checkpoint_row(pid, base_output, field_sources)
            self.set_staging_row(pid, base_output, field_sources)
            current_checkpoint_size = self.checkpoint_size()
            if current_checkpoint_size % 20 == 0:
                self.save_checkpoint()
                self.save_staging()
            return base_output

        try:
            portal_values, portal_sources = self.enrich_from_state_portal(search_record)
        except Exception:
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

        if RUN_API_STATE_TEST_MODE and not row_has_found_data(base_output):
            LOGGER.info("PID=%s had no API data after state adapter; invoking Google API-miss sample fallback", pid)
            self.enrich_from_google_api_miss_fallback(search_record, base_output, field_sources)

        if USE_STATE_PORTAL_ADAPTERS_ONLY:
            base_output["Telephone"] = normalize_phone(base_output.get("Telephone"))
            base_output["Mailing_Zip"] = normalize_zip(base_output.get("Mailing_Zip"))
            base_output["URL"] = normalize_url(base_output.get("URL"))
            for field in ("Telephone", "Mailing_Zip", "URL"):
                if field in field_sources:
                    field_sources[field]["value"] = base_output.get(field, "")
            LOGGER.info(
                "PID=%s completed using state-portal-only mode address=%s zip=%s phone=%s url=%s capacity=%s age=%s",
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
                base_output["Matched_Reason"] = "No datapoints were found from the configured official state adapter."
            self.set_checkpoint_row(pid, base_output, field_sources)
            self.set_staging_row(pid, base_output, field_sources)
            current_checkpoint_size = self.checkpoint_size()
            if current_checkpoint_size % 20 == 0:
                self.save_checkpoint()
                self.save_staging()
            return base_output

        search_name_variants = search_profile.search_name_variants[:4] or [search_profile.search_name_primary]
        official_queries = [
            " ".join(
                part
                for part in [f'"{variant}"', record["Mailing_City"], record["Mailing_State"], "daycare"]
                if clean_text(part)
            )
            for variant in search_name_variants[:3]
        ]
        official_queries = dedupe_preserve_order(official_queries)
        search_results: List[SearchResult] = []
        seen_result_urls = set()
        attempted_search_queries: List[str] = []
        for query in official_queries:
            attempted_search_queries.append(query)
            try:
                variant_results = self.search(query)
            except Exception:
                LOGGER.exception("Search failed for PID=%s query=%s", pid, query)
                continue
            for result in variant_results:
                if result.url in seen_result_urls:
                    continue
                seen_result_urls.add(result.url)
                search_results.append(result)

        if not search_results:
            LOGGER.warning("PID=%s not checkpointed because all official search queries failed or returned no results", pid)
            base_output["Match_Status"] = "not_found"
            base_output["Match_Confidence"] = "0"
            base_output["Matched_Reason"] = "No official-search candidates were returned for the cleaned daycare name variants."
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
    if ADAPTER_ONLY_TEST_STATES:
        output_rows = [
            row for row in rows
            if clean_text(row.get("Mailing_State")) in ADAPTER_ONLY_TEST_STATES and row_has_found_data(row)
        ]
        LOGGER.info(
            "Adapter-only test output filtered from %s rows down to %s rows with found adapter data for states=%s",
            len(rows),
            len(output_rows),
            sorted(ADAPTER_ONLY_TEST_STATES),
        )
    elif RUN_GOOGLE_ONLY_SAMPLE_MODE:
        output_rows = [row for row in rows if row_has_found_data(row)]
        LOGGER.info(
            "Google-only sample output filtered from %s rows down to %s rows with found Google data",
            len(rows),
            len(output_rows),
        )
    elif RUN_API_STATE_TEST_MODE:
        api_states = set(load_active_api_model_states())
        output_rows = [
            row for row in rows
            if clean_text(row.get("Mailing_State")) in api_states and row_has_found_data(row)
        ]
        LOGGER.info(
            "API-state test output filtered from %s rows down to %s rows with found API data",
            len(rows),
            len(output_rows),
        )
    elif RUN_MODEL_STATE_STAGING and not CSV_CLEANING_ONLY_MODE:
        output_rows = [row for row in rows if row_has_found_data(row)]
        LOGGER.info(
            "Model-state staging output filtered from %s rows down to %s rows with found data",
            len(rows),
            len(output_rows),
        )
    LOGGER.info("Writing %s rows to %s", len(output_rows), path)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS, delimiter=";")
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
    if PORTAL_VALIDATION_ALL_ROWS_STATES:
        filtered_rows = [row for row in rows if clean_text(row.get("Mailing_State")) in PORTAL_VALIDATION_ALL_ROWS_STATES]
        LOGGER.info(
            "Running portal validation all-rows mode for states=%s with %s matched CSV rows",
            sorted(PORTAL_VALIDATION_ALL_ROWS_STATES),
            len(filtered_rows),
        )
        return filtered_rows
    if PORTAL_VALIDATION_SAMPLE_ONLY:
        sample_rows = load_portal_validation_sample_rows()
        selected_rows: List[Dict[str, str]] = []
        keyed_rows = {
            (clean_text(row.get("PID")), clean_text(row.get("Mailing_State"))): row
            for row in rows
        }
        for sample in sample_rows:
            matched = keyed_rows.get((clean_text(sample.get("PID")), clean_text(sample.get("Mailing_State"))))
            if matched:
                selected_rows.append(matched)
        LOGGER.info(
            "Running portal validation sample mode for states=%s with %s matched CSV rows",
            sorted(PORTAL_VALIDATION_SAMPLE_STATES),
            len(selected_rows),
        )
        return selected_rows
    if RUN_GOOGLE_ONLY_SAMPLE_MODE:
        sample_size = min(VALIDATION_SAMPLE_SIZE, max(len(rows), 0))
        randomizer = random.Random(VALIDATION_RANDOM_SEED)
        indexed_rows = list(enumerate(rows, start=1))
        sampled = randomizer.sample(indexed_rows, sample_size)
        sampled.sort(key=lambda item: item[0])
        selected_rows = [row for _, row in sampled]
        LOGGER.info(
            "Running Google-only sample mode with %s randomly selected rows using seed=%s",
            sample_size,
            VALIDATION_RANDOM_SEED,
        )
        return selected_rows
    if RUN_API_STATE_TEST_MODE:
        api_states = set(load_active_api_model_states())
        filtered_rows = [row for row in rows if clean_text(row.get("Mailing_State")) in api_states]
        LOGGER.info(
            "Running API-state test mode for states=%s with %s matching rows",
            sorted(api_states),
            len(filtered_rows),
        )
        return filtered_rows
    if RUN_MODEL_STATE_STAGING:
        randomizer = random.Random(VALIDATION_RANDOM_SEED)
        active_states = set(load_active_model_states())
        grouped_rows: Dict[str, List[Tuple[int, Dict[str, str]]]] = {}
        for index, row in enumerate(rows, start=1):
            state = clean_text(row.get("Mailing_State"))
            if state not in active_states:
                continue
            grouped_rows.setdefault(state, []).append((index, row))
        sampled_rows: List[Tuple[int, Dict[str, str]]] = []
        for state in sorted(grouped_rows):
            state_rows = grouped_rows[state]
            target_count = min(len(state_rows), randomizer.randint(MODEL_STATE_SAMPLE_MIN, MODEL_STATE_SAMPLE_MAX))
            sampled_rows.extend(randomizer.sample(state_rows, target_count))
            LOGGER.info(
                "Model-state staging selected %s rows for state=%s from %s available rows",
                target_count,
                state,
                len(state_rows),
            )
        sampled_rows.sort(key=lambda item: item[0])
        selected_rows = [rows[0]] + [row for _, row in sampled_rows]
        LOGGER.info(
            "Running model-state staging sample with %s selected rows across %s active states using seed=%s",
            len(selected_rows),
            len(grouped_rows),
            VALIDATION_RANDOM_SEED,
        )
        return selected_rows
    if VALIDATION_STATE_FILTER:
        filtered_rows = [row for row in rows if clean_text(row.get("Mailing_State")) == VALIDATION_STATE_FILTER]
        LOGGER.info(
            "Running state-filtered validation for state=%s with %s matching rows",
            VALIDATION_STATE_FILTER,
            len(filtered_rows),
        )
        return filtered_rows
    if not RUN_VALIDATION_SAMPLE:
        LOGGER.info("Running full dataset with %s processable rows", len(rows))
        return rows

    sample_size = min(VALIDATION_SAMPLE_SIZE, max(len(rows), 0))
    randomizer = random.Random(VALIDATION_RANDOM_SEED)
    indexed_rows = list(enumerate(rows, start=1))
    sampled = randomizer.sample(indexed_rows, sample_size)
    sampled.sort(key=lambda item: item[0])
    selected_rows = [row for _, row in sampled]
    LOGGER.info(
        "Running validation sample with %s randomly selected rows using seed=%s",
        sample_size,
        VALIDATION_RANDOM_SEED,
    )
    return selected_rows


def get_output_path() -> str:
    if CSV_CLEANING_ONLY_MODE:
        return CLEANED_INPUT_CSV
    if SINGLE_PID_FILTER:
        return os.path.join(OUTPUT_DIR, f"DaycareBuildings_Enriched_PID_{SINGLE_PID_FILTER}.csv")
    if ADAPTER_ONLY_TEST_STATES:
        suffix = "_".join(sorted(ADAPTER_ONLY_TEST_STATES))
        return os.path.join(OUTPUT_DIR, f"DaycareBuildings_Enriched_AdapterOnly_{suffix}.csv")
    if PORTAL_VALIDATION_ALL_ROWS_STATES:
        return os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_IL_VA.csv")
    if PORTAL_VALIDATION_SAMPLE_ONLY:
        return os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_PortalValidation.csv")
    if RUN_GOOGLE_ONLY_SAMPLE_MODE:
        return os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_GoogleOnly_100.csv")
    if RUN_API_STATE_TEST_MODE:
        return os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_API.csv")
    if RUN_MODEL_STATE_STAGING:
        return STAGING_OUTPUT_CSV
    if VALIDATION_STATE_FILTER:
        return os.path.join(OUTPUT_DIR, f"DaycareBuildings_Enriched_{VALIDATION_STATE_FILTER}.csv")
    return SAMPLE_OUTPUT_CSV if RUN_VALIDATION_SAMPLE else OUTPUT_CSV


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
    if RUN_GOOGLE_ONLY_SAMPLE_MODE and GOOGLE_USE_PERSISTENT_PROFILE:
        LOGGER.info(
            "Forcing single-worker mode because Google-only sample mode is using a persistent Chrome profile at %s",
            CHROME_PROFILE_DIR,
        )
        return 1
    if RUN_API_STATE_TEST_MODE:
        return max(1, min(API_TEST_MAX_WORKERS, len(load_active_api_model_states()) or 1))
    if USE_STATE_PORTAL_ADAPTERS_ONLY and ADAPTER_ONLY_TEST_STATES:
        return max(1, len(ADAPTER_ONLY_TEST_STATES))
    return DEFAULT_MAX_WORKERS


def process_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    enricher = DaycareEnricher()
    output_rows: List[Dict[str, str]] = []
    work_rows = rows
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

    def process_state_batch(state_rows: List[Tuple[int, Dict[str, str]]]) -> List[Tuple[int, Dict[str, str]]]:
        state_results: List[Tuple[int, Dict[str, str]]] = []
        for index, row in state_rows:
            try:
                state_results.append((index, enricher.enrich_record(row)))
            except Exception:
                LOGGER.exception("Worker failed for row index=%s PID=%s", index, row.get("PID", ""))
                state_results.append((index, {header: clean_text(row.get(header, "")) for header in OUTPUT_HEADERS}))
        return state_results

    try:
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
        for index in range(1, len(work_rows) + 1):
            output_rows.append(ordered_results[index])
        enricher.save_checkpoint()
        enricher.save_staging()
        LOGGER.info(
            "Completed processing all rows total_facilities=%s processed=%s found_data=%s",
            total_to_process,
            completed,
            found_count,
        )
        return output_rows
    finally:
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
    LOGGER.info("Starting daycare enrichment run")
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

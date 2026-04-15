import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
STAGING_DIR = os.path.join(BASE_DIR, "enrichment_staging")

INPUT_CSV = os.path.join(BASE_DIR, "DaycareBuildings_Input(in).csv")
CLEANED_INPUT_CSV = os.path.join(OUTPUT_DIR, "DaycareBuildings_Cleaned.csv")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched.csv")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "enrichment.json")
GOOGLE_CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "google_enriched_staging.json")
GOOGLE_BAD_PROXY_FILE = os.path.join(OUTPUT_DIR, "google_bad_proxies.json")
GOOGLE_MISS_FILE = os.path.join(OUTPUT_DIR, "google_miss.json")
LOG_FILE = os.path.join(LOG_DIR, "enrichment.log")
STATE_SCRAPER_MODELS_FILE = os.path.join(BASE_DIR, "state_scraper_models.json")
CHROME_BINARY_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

STATE_BATCH_MAX_WORKERS = 6
ADAPTER_ONLY_TEST_STATES = set()
SINGLE_PID_FILTER = ""
FORCE_HEADED = False

USE_STATE_PORTAL_ADAPTERS_ONLY = False
RUN_API_STATE_TEST_MODE = False
CSV_CLEANING_ONLY_MODE = False

CHROME_PROFILE_DIR = os.path.join(BASE_DIR, "chrome_profiles", "google_search")
SAMPLE_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_sample50.csv")
STAGING_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "DaycareBuildings_Enriched_staging.csv")
LEGACY_STAGING_FILE = os.path.join(BASE_DIR, "enrichment_staging.json")
LEGACY_CHECKPOINT_FILES = (
    os.path.join(BASE_DIR, "enrichment_checkpoint.json"),
    os.path.join(OUTPUT_DIR, "enrichment_checkpoint.json"),
)

SEARCH_ENGINE_URL = "https://search.brave.com/search"
GOOGLE_SEARCH_URL = "https://www.google.com/search"
BING_SEARCH_URL = "https://www.bing.com/search"
YAHOO_SEARCH_URL = "https://search.yahoo.com/search"
GOOGLE_HOME_URL = "https://www.google.com/?hl=en"

CHECKPOINT_SCHEMA_VERSION = 3
RUN_VALIDATION_SAMPLE = False
VALIDATION_SAMPLE_SIZE = 100
VALIDATION_RANDOM_SEED = 42
VALIDATION_STATE_FILTER = ""
PORTAL_VALIDATION_SAMPLE_ONLY = False
PORTAL_VALIDATION_SAMPLE_STATES = {"IL", "VA"}
PORTAL_VALIDATION_ALL_ROWS_STATES = set()
RUN_GOOGLE_ONLY_SAMPLE_MODE = False
ENABLE_GOOGLE_FALLBACK_FOR_API_MISSES = False
GOOGLE_API_MISS_SAMPLE_LIMIT = 100
GOOGLE_SEARCH_RETRIES = 1
GOOGLE_FALLBACK_MAX_CONCURRENT = 1
GOOGLE_USE_PERSISTENT_PROFILE = False
GOOGLE_USE_HEADLESS = True
GOOGLE_SEARCH_MIN_DELAY_SECONDS = 0.75
GOOGLE_SEARCH_TOTAL_TIMEOUT_SECONDS = 5.0

OUTPUT_HEADERS = [
    "PID",
    "DayCareType",
    "Daycare_Name",
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
DEFAULT_MAX_WORKERS = STATE_BATCH_MAX_WORKERS
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

ROTATING_BROWSER_PROXY_ENABLED = True
ROTATING_BROWSER_PROXY_SCHEME = "http"
ROTATING_BROWSER_PROXIES = [
]
ROTATING_BROWSER_BAD_PROXY_HOSTS = []

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

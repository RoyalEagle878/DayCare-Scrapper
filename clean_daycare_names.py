import csv
import logging
import os
import re
import unicodedata
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Set, Tuple

from runtime_env import BASE_DIR, CLEANED_INPUT_CSV as OUTPUT_CSV, INPUT_CSV, LOG_DIR, OUTPUT_DIR
LOG_FILE = os.path.join(LOG_DIR, "clean_daycare_names.log")
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 3
MAX_VARIANTS = 15

INPUT_HEADERS = [
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
]

CLEANING_HEADERS = [
    "Original_Name",
    "Normalized_Name",
    "Normalized_Name_Core",
    "Search_Name_Primary",
    "Search_Name_Variants",
    "Cleaning_Rules_Applied",
]

OUTPUT_HEADERS = INPUT_HEADERS + CLEANING_HEADERS

DECORATIVE_TOKENS = {
    "THE",
    "INC",
    "LLC",
    "LTD",
    "CORP",
    "CORPORATION",
    "COMPANY",
    "CO",
}

GENERIC_BACKUP_REMOVE_TOKENS = {
    "DAYCARE",
    "DAY",
    "CARE",
    "CHILDCARE",
    "CHILD",
    "CHILDREN",
    "CHILDRENS",
    "CENTER",
    "CENTRE",
    "CTR",
    "CC",
    "CCC",
    "CDC",
    "DCC",
    "DC",
    "ECC",
    "SCHOOL",
    "SCH",
    "PRESCHOOL",
    "PRESCH",
    "PRESCHOOLS",
    "PRESCHOOLERS",
    "NURSERY",
    "NRSRY",
    "PRE",
    "PREK",
    "KINDERGARTEN",
    "KDGN",
    "DEV",
    "DEVELOPMENT",
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

CONNECTOR_TOKENS = {"AND", "OF", "FOR", "AT", "IN"}

TOKEN_EXPANSIONS = {
    "CTR": ["CENTER", "CENTRE"],
    "CDC": ["CHILD DEVELOPMENT CENTER"],
    "CC": ["CHILD CARE", "COMMUNITY CENTER"],
    "CCC": ["CHILD CARE CENTER"],
    "DCC": ["DAY CARE CENTER", "CHILD CARE CENTER"],
    "DC": ["DAY CARE", "DEVELOPMENT CENTER"],
    "ECC": ["EARLY CHILDHOOD CENTER", "EARLY CHILDHOOD EDUCATION CENTER"],
    "CH": ["CHURCH"],
    "CHLD": ["CHILD"],
    "CHRN": ["CHILDREN"],
    "CHLDHD": ["CHILDHOOD"],
    "DEV": ["DEVELOPMENT"],
    "DVLP": ["DEVELOPMENT"],
    "EDUC": ["EDUCATION"],
    "SCH": ["SCHOOL"],
    "KDGN": ["KINDERGARTEN"],
    "PRE": ["PRESCHOOL", "PRE SCHOOL"],
    "PRESCH": ["PRESCHOOL", "PRE SCHOOL"],
    "PREK": ["PRE KINDERGARTEN"],
    "PRE-K": ["PRE KINDERGARTEN"],
    "NRSRY": ["NURSERY"],
    "CLG": ["COLLEGE"],
    "LRNG": ["LEARNING"],
    "CMTY": ["COMMUNITY"],
    "LUTH": ["LUTHERAN"],
    "METH": ["METHODIST"],
    "PRESBY": ["PRESBYTERIAN"],
    "ACAD": ["ACADEMY"],
    "UNIV": ["UNIVERSITY"],
    "CRCH": ["CHURCH"],
    "SACC": ["SCHOOL AGE CHILD CARE"],
    "MNTSSRI": ["MONTESSORI"],
    "ACDMY": ["ACADEMY"],
    "MONSGNR": ["MONSIGNOR"],
    "ST": ["SAINT", "ST"],
    "MT": ["MOUNT", "MT"],
    "COOP": ["COOPERATIVE"],
    "CO-OP": ["COOPERATIVE"]
}

PHRASE_EXPANSIONS = [
    (("DEV", "CTR"), "DEVELOPMENT CENTER"),
    (("DVLP", "CTR"), "DEVELOPMENT CENTER"),
    (("CHLD", "DEV", "CTR"), "CHILD DEVELOPMENT CENTER"),
    (("CHLD", "DVLP", "CTR"), "CHILD DEVELOPMENT CENTER"),
    (("EARLY", "CHLD", "CTR"), "EARLY CHILD CENTER"),
    (("CH", "SCH"), "CHURCH SCHOOL"),
    (("DAY", "NRSRY"), "DAY NURSERY"),
    (("PRE", "SCH"), "PRESCHOOL"),
    (("PRE", "SCHOOL"), "PRESCHOOL"),
    (("CHLD", "LRNG", "CTR"), "CHILD LEARNING CENTER"),
    (("CHLD", "CARE", "CTR"), "CHILD CARE CENTER"),
    (("CO", "OP"), "COOPERATIVE"),
]

CITY_EXACT_EXPANSIONS = {
    "W SAINT PAUL": "WEST SAINT PAUL",
    "FRANKLIN LKS": "FRANKLIN LAKES",
    "VALLEY VLG": "VALLEY VILLAGE",
    "COLORADO SPGS": "COLORADO SPRINGS",
    "FT LAUDERDALE": "FORT LAUDERDALE",
    "CLARENDON HLS": "CLARENDON HILLS",
    "PEORIA HTS": "PEORIA HEIGHTS",
    "PRAIRIE VLG": "PRAIRIE VILLAGE",
    "BLOOMFLD TWP": "BLOOMFIELD TOWNSHIP",
    "LOGAN TWP": "LOGAN TOWNSHIP",
    "JEFFERSON CTY": "JEFFERSON CITY",
    "SALT LAKE CTY": "SALT LAKE CITY",
}

CITY_TOKEN_EXPANSIONS = {
    "N": "NORTH",
    "E": "EAST",
    "W": "WEST",
    "S": "SOUTH",
    "FT": "FORT",
    "HTS": "HEIGHTS",
    "TWP": "TOWNSHIP",
    "VLG": "VILLAGE",
    "SPGS": "SPRINGS",
    "LKS": "LAKES",
}

CITY_SUFFIX_STRIP_TOKENS = {"TOWNSHIP", "TWP", "VILLAGE", "VLG", "BOROUGH"}


def configure_logging() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger = logging.getLogger("daycare_name_cleaner")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


LOGGER = configure_logging()


def clean_text(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_name_text(name: str) -> str:
    text = clean_text(unicodedata.normalize("NFKD", name or ""))
    if not text:
        return ""
    text = text.replace("&", " AND ")
    text = text.replace("/", " ")
    text = text.replace("\\", " ")
    text = re.sub(r"\bCO[\s\-]*OP\b", " COOP ", text, flags=re.IGNORECASE)
    text = text.replace("-", " ")
    text = text.replace("’", "'").replace("`", "'")
    text = re.sub(r"\bPRE[\s\-]*K\b", " PREK ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bPRE[\s\-]*SCHOOL\b", " PRESCHOOL ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b([A-Za-z]+)'S\b", r"\1S", text)
    text = re.sub(r"[^A-Za-z0-9\s']", " ", text)
    text = text.replace("'", "")
    text = re.sub(r"\s+", " ", text).strip().upper()
    return text


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for value in values:
        normalized = clean_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def tokenize_name(name: str) -> List[str]:
    normalized = normalize_name_text(name)
    return [token for token in normalized.split() if token]


def normalize_city_name(city: str) -> str:
    normalized_city = normalize_name_text(city)
    if not normalized_city:
        return ""
    if normalized_city in CITY_EXACT_EXPANSIONS:
        normalized_city = CITY_EXACT_EXPANSIONS[normalized_city]
    tokens = normalized_city.split()
    expanded_tokens = [CITY_TOKEN_EXPANSIONS.get(token, token) for token in tokens]
    expanded_tokens = [token for token in expanded_tokens if token not in CITY_SUFFIX_STRIP_TOKENS]
    return " ".join(expanded_tokens)


def apply_phrase_expansions(tokens: List[str]) -> Tuple[List[str], List[str]]:
    variants: List[str] = []
    applied_rules: List[str] = []
    for phrase_tokens, replacement in PHRASE_EXPANSIONS:
        phrase_len = len(phrase_tokens)
        for index in range(len(tokens) - phrase_len + 1):
            window = tuple(tokens[index:index + phrase_len])
            if window != phrase_tokens:
                continue
            replaced = list(tokens[:index]) + normalize_name_text(replacement).split() + list(tokens[index + phrase_len:])
            variants.append(" ".join(replaced))
            applied_rules.append(f"{' '.join(phrase_tokens)}->{replacement}")
    return variants, applied_rules


def apply_token_expansions(tokens: List[str]) -> Tuple[List[str], List[str]]:
    variants: List[str] = []
    applied_rules: List[str] = []
    for index, token in enumerate(tokens):
        for expansion in TOKEN_EXPANSIONS.get(token, []):
            expanded_tokens = list(tokens)
            expanded_tokens[index:index + 1] = normalize_name_text(expansion).split()
            variants.append(" ".join(expanded_tokens))
            applied_rules.append(f"{token}->{expansion}")
    return variants, applied_rules


def apply_city_aware_expansions(tokens: List[str], city: str) -> Tuple[List[str], List[str]]:
    city_tokens = [token for token in tokenize_name(normalize_city_name(city)) if token]
    if not city_tokens:
        return [], []
    variants: List[str] = []
    applied_rules: List[str] = []
    for index, token in enumerate(tokens):
        if len(token) < 4:
            continue
        for city_token in city_tokens:
            if token == city_token:
                continue
            if city_token.startswith(token):
                expanded_tokens = list(tokens)
                expanded_tokens[index] = city_token
                variants.append(" ".join(expanded_tokens))
                applied_rules.append(f"{token}->{city_token} (from city)")
    return variants, applied_rules


def apply_compound_city_variants(tokens: List[str], city: str) -> Tuple[List[str], List[str]]:
    city_tokens = [token for token in tokenize_name(normalize_city_name(city)) if token]
    if len(city_tokens) < 2:
        return [], []
    compact_city = "".join(city_tokens)
    spaced_city = " ".join(city_tokens)
    token_set = set(tokens)
    variants: List[str] = []
    applied_rules: List[str] = []

    if compact_city in token_set:
        expanded_tokens = [spaced_city if token == compact_city else token for token in tokens]
        variants.append(" ".join(expanded_tokens))
        applied_rules.append(f"{compact_city}->{spaced_city} (compound city)")

    for index in range(len(tokens) - len(city_tokens) + 1):
        window = tokens[index:index + len(city_tokens)]
        if window == city_tokens:
            compact_tokens = list(tokens[:index]) + [compact_city] + list(tokens[index + len(city_tokens):])
            variants.append(" ".join(compact_tokens))
            applied_rules.append(f"{spaced_city}->{compact_city} (compound city)")
            break

    return variants, applied_rules


def build_core_identity(tokens: List[str]) -> str:
    return " ".join(
        token for token in tokens
        if token not in DECORATIVE_TOKENS
        and token not in CONNECTOR_TOKENS
        and (token not in GENERIC_BACKUP_REMOVE_TOKENS or token in RELIGIOUS_COMMUNITY_TOKENS)
    )


def remove_city_tokens(tokens: List[str], city: str) -> Tuple[List[str], List[str]]:
    city_tokens = tokenize_name(normalize_city_name(city))
    city_token_set = set(city_tokens)
    if not city_tokens:
        return list(tokens), []
    filtered: List[str] = []
    removed: List[str] = []
    for token in tokens:
        matches_city = token in city_token_set
        if not matches_city and len(token) >= 4:
            matches_city = any(city_token.startswith(token) for city_token in city_tokens)
        if matches_city:
            removed.append(token)
        else:
            filtered.append(token)
    if filtered:
        return filtered, removed
    return list(tokens), []


def build_normalized_core(tokens: List[str], city: str) -> Tuple[str, List[str]]:
    tokens_without_city, removed_city_tokens = remove_city_tokens(tokens, city)
    core_tokens: List[str] = []
    for token in tokens_without_city:
        if token in DECORATIVE_TOKENS:
            continue
        if token in CONNECTOR_TOKENS:
            continue
        if token in GENERIC_BACKUP_REMOVE_TOKENS and token not in RELIGIOUS_COMMUNITY_TOKENS:
            continue
        core_tokens.append(token)
    if not core_tokens:
        core_tokens = [token for token in tokens_without_city if token not in DECORATIVE_TOKENS]
    if not core_tokens:
        core_tokens = [token for token in tokens if token not in DECORATIVE_TOKENS]
    return " ".join(core_tokens), removed_city_tokens


def build_short_search(tokens: List[str]) -> str:
    distinctive = [
        token for token in tokens
        if token not in DECORATIVE_TOKENS and token not in GENERIC_BACKUP_REMOVE_TOKENS
    ]
    if not distinctive:
        distinctive = [token for token in tokens if token not in DECORATIVE_TOKENS]
    return " ".join(distinctive[:5])


def build_religious_community_form(tokens: List[str]) -> str:
    return " ".join(
        token for token in tokens
        if token not in DECORATIVE_TOKENS
        and (token in RELIGIOUS_COMMUNITY_TOKENS or token not in GENERIC_BACKUP_REMOVE_TOKENS)
    )


def build_cleaning_profile(daycare_name: str, city: str) -> Dict[str, str]:
    original_name = clean_text(daycare_name)
    tokens = [token for token in tokenize_name(original_name) if token not in DECORATIVE_TOKENS]
    normalized_name = " ".join(tokens)
    normalized_city = normalize_city_name(city)
    normalized_core, removed_city_tokens = build_normalized_core(tokens, normalized_city)

    variants: List[str] = [normalized_name]
    rules_applied: List[str] = []

    token_variants, token_rules = apply_token_expansions(tokens)
    phrase_variants, phrase_rules = apply_phrase_expansions(tokens)
    city_variants, city_rules = apply_city_aware_expansions(tokens, normalized_city)
    compound_city_variants, compound_city_rules = apply_compound_city_variants(tokens, normalized_city)

    variants.extend(token_variants)
    variants.extend(phrase_variants)
    variants.extend(city_variants)
    variants.extend(compound_city_variants)
    rules_applied.extend(token_rules)
    rules_applied.extend(phrase_rules)
    rules_applied.extend(city_rules)
    rules_applied.extend(compound_city_rules)
    if removed_city_tokens:
        rules_applied.append(f"removed city tokens from core: {' '.join(dedupe_preserve_order(removed_city_tokens))}")

    variants.append(normalized_core)
    variants.append(build_core_identity(tokens))
    variants.append(build_short_search(tokens))
    variants.append(build_religious_community_form(tokens))

    if normalized_name:
        variants.append(re.sub(r"\bST\b", "SAINT", normalized_name))
        variants.append(re.sub(r"\bMT\b", "MOUNT", normalized_name))

    variants = dedupe_preserve_order(variants)
    variants = sorted(
        variants,
        key=lambda value: (-len(clean_text(value)), variants.index(value)),
    )[:MAX_VARIANTS]
    primary = variants[0] if variants else normalized_name

    return {
        "Original_Name": original_name,
        "Normalized_Name": normalized_name,
        "Normalized_Name_Core": normalized_core,
        "Search_Name_Primary": primary,
        "Search_Name_Variants": " || ".join(variants),
        "Cleaning_Rules_Applied": " || ".join(dedupe_preserve_order(rules_applied)),
    }


def read_rows(path: str) -> List[Dict[str, str]]:
    LOGGER.info("Reading input rows from %s", path)
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        rows = list(reader)
    LOGGER.info("Loaded %s raw rows", len(rows))
    normalized_rows: List[Dict[str, str]] = []
    for row in rows:
        clean_row = {
            "PID": clean_text(row.get("PID")),
            "DayCareType": clean_text(row.get("DayCareType")),
            "Daycare_Name": clean_text(row.get("Daycare_Name")),
            "Mailing_City": clean_text(row.get("Mailing_City")),
            "Mailing_State": clean_text(row.get("Mailing_State")),
            "Mailing_Address": clean_text(row.get("Mailing_Address")),
            "Mailing_Zip": clean_text(row.get("Mailing_Zip")),
            "Telephone": clean_text(row.get("Telephone")),
            "URL": clean_text(row.get("URL")),
            "Capacity (optional)": clean_text(row.get("Capacity (optional)")),
            "Age Range (optional)": clean_text(row.get("Age Range (optional),,")),
        }
        normalized_rows.append(clean_row)
    return normalized_rows


def clean_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    cleaned_rows: List[Dict[str, str]] = []
    for row in rows:
        normalized_city = normalize_city_name(row.get("Mailing_City", ""))
        profile = build_cleaning_profile(row.get("Daycare_Name", ""), normalized_city)
        cleaned_row = dict(row)
        if normalized_city:
            cleaned_row["Mailing_City"] = normalized_city.title()
        cleaned_row.update(profile)
        cleaned_rows.append(cleaned_row)
        LOGGER.info(
            "PID=%s original=%s normalized=%s primary=%s rules=%s",
            cleaned_row.get("PID", ""),
            cleaned_row.get("Original_Name", ""),
            cleaned_row.get("Normalized_Name", ""),
            cleaned_row.get("Search_Name_Primary", ""),
            cleaned_row.get("Cleaning_Rules_Applied", ""),
        )
    return cleaned_rows


def write_rows(path: str, rows: List[Dict[str, str]]) -> None:
    LOGGER.info("Writing %s cleaned rows to %s", len(rows), path)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADERS, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    LOGGER.info("Completed writing cleaned CSV to %s", path)


def main() -> None:
    LOGGER.info("Starting daycare name cleaning run")
    rows = read_rows(INPUT_CSV)
    cleaned_rows = clean_rows(rows)
    write_rows(OUTPUT_CSV, cleaned_rows)
    LOGGER.info("Completed daycare name cleaning run output=%s rows=%s", OUTPUT_CSV, len(cleaned_rows))
    print(f"Wrote {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

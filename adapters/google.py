class GoogleSearchAdapter:
    name = "GOOGLE"

    BAD_FALLBACK_DOMAINS = {
        "accounts.google.com",
        "google.com",
        "www.google.com",
        "maps.google.com",
        "winnie.com",
        "www.winnie.com",
        "dnb.com",
        "www.dnb.com",
        "usnews.com",
        "www.usnews.com",
        "easyshiksha.com",
        "www.easyshiksha.com",
        "kidambi.com",
        "www.kidambi.com",
        "indeed.com",
        "www.indeed.com",
        "hommati.com",
        "www.hommati.com",
    }
    BAD_PANEL_TITLES = {
        "loading",
        "more locations",
        "web results",
        "results",
        "overview",
        "locations",
        "direction",
        "directions",
        "website",
        "call",
        "photos",
        "updates",
        "services",
        "products",
        "reviews",
        "hours",
    }
    BAD_PANEL_TITLE_FRAGMENTS = (
        "web result",
        "site links",
        "more locations",
        "faq",
        "agenda",
        "secretary of",
    )

    def handle_google_consent(self, driver):
        import time

        from selenium.webdriver.common.by import By

        try:
            accept_label = driver.find_elements(By.XPATH, "//div[contains(@class,'QS5gu') and normalize-space()='Accept all']")
            if not accept_label:
                return False
            accept_button = driver.find_elements(By.XPATH, '//*[@id="L2AGLb"]/div')
            if not accept_button:
                return False
            driver.execute_script("arguments[0].click();", accept_button[0])
            time.sleep(2.0)
            return True
        except Exception:
            return False

    def _human_pause(self, driver):
        import random
        import time

        from selenium.webdriver.common.action_chains import ActionChains
        from selenium.webdriver.common.by import By

        total_sleep = random.uniform(3.0, 5.0)
        end_time = time.time() + total_sleep
        while time.time() < end_time:
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                width = driver.execute_script("return Math.max(window.innerWidth || 0, 300);") or 300
                height = driver.execute_script("return Math.max(window.innerHeight || 0, 300);") or 300
                offset_x = random.randint(20, max(int(width) - 20, 21))
                offset_y = random.randint(20, max(int(height) - 20, 21))
                ActionChains(driver).move_to_element_with_offset(body, offset_x, offset_y).perform()
            except Exception:
                pass
            time.sleep(random.uniform(0.4, 0.9))

    def _human_pre_search_interaction(self, driver, search_box):
        import random
        import time

        from selenium.webdriver.common.action_chains import ActionChains

        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_box)
        except Exception:
            pass
        try:
            ActionChains(driver).move_to_element(search_box).pause(random.uniform(0.15, 0.45)).click(search_box).perform()
        except Exception:
            try:
                search_box.click()
            except Exception:
                pass
        try:
            driver.execute_script("window.scrollBy(0, arguments[0]);", random.randint(60, 180))
            time.sleep(random.uniform(0.15, 0.35))
            driver.execute_script("window.scrollBy(0, arguments[0]);", -random.randint(20, 90))
        except Exception:
            pass
        time.sleep(random.uniform(0.2, 0.6))

    def _type_like_human(self, element, text):
        import random
        import time

        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.03, 0.11))
            if random.random() < 0.08:
                time.sleep(random.uniform(0.12, 0.28))

    def _root_url(self, url):
        from urllib.parse import urlparse

        from enrich_daycare_data import clean_text

        parsed = urlparse(clean_text(url))
        if not parsed.netloc:
            return ""
        scheme = parsed.scheme or "https"
        return f"{scheme}://{parsed.netloc}"

    def _root_domain(self, url):
        from urllib.parse import urlparse

        from enrich_daycare_data import clean_text

        return clean_text(urlparse(clean_text(url)).netloc).lower()

    def _looks_like_bad_fallback_url(self, url):
        domain = self._root_domain(url)
        return not domain or domain in self.BAD_FALLBACK_DOMAINS

    def _extract_panel_title(self, panel_root):
        import re

        from enrich_daycare_data import clean_text

        def normalize_candidate(value):
            cleaned = clean_text(value)
            if not cleaned:
                return ""
            cleaned = re.sub(r"\s*\(@[^)]*\)\s*$", "", cleaned).strip()
            lowered = cleaned.lower()
            if lowered in self.BAD_PANEL_TITLES:
                return ""
            if any(fragment in lowered for fragment in self.BAD_PANEL_TITLE_FRAGMENTS):
                return ""
            return cleaned

        selectors = (
            "[role='heading'][data-attrid='title']",
            "div[data-attrid='title'] [role='heading']",
            "div[data-attrid='title'] span",
            "h2[data-attrid='title']",
            "div.kno-ecr-pt span",
            "div.kp-header h2 span",
            "div[data-attrid='kc:/location/location:entity_title'] span",
            "div[data-attrid='kc:/organization/organization:entity_title'] span",
            "div[data-attrid='kc:/education/school:entity_title'] span",
            "div.SPZz6b span",
        )
        for selector in selectors:
            node = panel_root.select_one(selector)
            if not node:
                continue
            value = normalize_candidate(node.get_text(" ", strip=True))
            if value:
                return value
        for node in panel_root.select("h2, h3"):
            value = normalize_candidate(node.get_text(" ", strip=True))
            if value and len(value) >= 4:
                return value
        return ""

    def _panel_title_matches_record(self, record, panel_name):
        from enrich_daycare_data import clean_text, get_record_name_profile, token_overlap_metrics

        panel_name = clean_text(panel_name)
        if not panel_name:
            return False
        profile = get_record_name_profile(record)
        best_shared = 0
        best_recall = 0.0
        best_precision = 0.0
        for variant in profile.search_name_variants:
            variant = clean_text(variant)
            if not variant:
                continue
            shared, recall, precision = token_overlap_metrics(variant, panel_name)
            if (shared, recall + precision) > (best_shared, best_recall + best_precision):
                best_shared = shared
                best_recall = recall
                best_precision = precision
        return (
            best_shared >= 2
            and best_recall >= 0.5
            and (best_recall + best_precision) >= 1.1
        )

    def _has_strong_panel_signals(self, values):
        from enrich_daycare_data import clean_text

        has_name = bool(clean_text(values.get("Matched_Provider_Name", "")))
        other_signals = 0
        if clean_text(values.get("Mailing_Address", "")):
            other_signals += 1
        if clean_text(values.get("Mailing_Zip", "")):
            other_signals += 1
        if clean_text(values.get("Telephone", "")):
            other_signals += 1
        if clean_text(values.get("URL", "")):
            other_signals += 1
        return has_name and other_signals >= 1

    def _build_query(self, record):
        from enrich_daycare_data import clean_text, get_record_name_profile

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City", ""))
        state = clean_text(record.get("Mailing_State", ""))
        variants = [clean_text(v) for v in profile.search_name_variants if clean_text(v)]
        if not variants:
            variants = [clean_text(record.get("Daycare_Name", ""))]
        variant_blob = " OR ".join(variants)
        return f"({variant_blob}) \"{city},{state}\""

    def _is_blocked(self, driver):
        try:
            return bool(
                driver.execute_script(
                    """
const bodyText = (document.body ? (document.body.innerText || document.body.textContent || '') : '').toLowerCase();
const title = (document.title || '').toLowerCase();
if (document.querySelector('form#captcha-form')) return true;
if (document.querySelector('iframe[src*="recaptcha"], div.g-recaptcha, #recaptcha')) return true;
return (
  bodyText.includes('our systems have detected unusual traffic') ||
  bodyText.includes('detected unusual traffic') ||
  bodyText.includes('enablejs') ||
  bodyText.includes('sorry') ||
  title.includes('unusual traffic') ||
  title.includes('sorry')
);
"""
                )
            )
        except Exception:
            return False

    def _enforce_backoff(self, enricher, state, query):
        import time

        from enrich_daycare_data import LOGGER

        try:
            enricher.queue_active_google_proxies_as_bad()
        except Exception:
            LOGGER.exception("Failed to move active Google proxies into bad queue")
        wait_seconds = enricher.get_next_google_backoff_seconds(state)
        LOGGER.info(
            "Google anti-robot detected for state=%s query=%s; resetting search browser and backing off for %.0f seconds",
            state,
            query,
            wait_seconds,
        )
        try:
            enricher.reset_search_driver()
        except Exception:
            pass
        time.sleep(wait_seconds)

    def _retry_backoff_seconds(self, attempt_index):
        import random

        from enrich_daycare_data import RETRY_BACKOFF_SECONDS

        base_seconds = max(float(RETRY_BACKOFF_SECONDS), 1.0)
        return (base_seconds * (2 ** max(attempt_index, 0))) + random.uniform(0.25, 1.0)

    def _ensure_search_box(self, driver):
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import GOOGLE_HOME_URL, SELENIUM_WAIT_TIMEOUT

        try:
            search_boxes = driver.find_elements(By.NAME, "q")
            visible_box = next((box for box in search_boxes if box.is_displayed()), None)
            if visible_box:
                return visible_box
        except Exception:
            pass
        driver.get(GOOGLE_HOME_URL)
        return WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )

    def _submit_query(self, driver, query):
        import random
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT

        search_box = self._ensure_search_box(driver)
        self._human_pre_search_interaction(driver, search_box)
        search_box.send_keys(Keys.CONTROL, "a")
        time.sleep(random.uniform(0.08, 0.2))
        search_box.send_keys(Keys.DELETE)
        time.sleep(random.uniform(0.12, 0.25))
        self._type_like_human(search_box, query)
        time.sleep(random.uniform(0.15, 0.45))
        search_box.send_keys(Keys.ENTER)
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(random.uniform(1.0, 1.8))
        LOGGER.info("Google adapter submitted query=%s", query)

    def _wait_for_results_surface(self, driver):
        import time

        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import SELENIUM_WAIT_TIMEOUT

        wait_timeout = max(SELENIUM_WAIT_TIMEOUT, 20)
        try:
            WebDriverWait(driver, wait_timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except Exception:
            pass

        try:
            WebDriverWait(driver, wait_timeout).until(
                lambda d: d.execute_script(
                    """
return !!(
  document.querySelector('#rhs [role="heading"][data-attrid="title"]') ||
  document.querySelector('#rhs [data-attrid="title"]') ||
  document.querySelector('#rhs [aria-label^="Call phone number "]') ||
  document.querySelector('#rhs a[href][ping]') ||
  document.querySelector('#search a[href] h3') ||
  document.querySelector('#center_col a[href][ping]') ||
  document.querySelector('form [name="q"]')
);
"""
                )
            )
        except Exception:
            pass

        time.sleep(0.8)

    def _parse_results_from_soup(self, soup):
        from enrich_daycare_data import SEARCH_RESULTS_LIMIT, SearchResult, clean_text, extract_google_target_url, is_blacklisted_official, is_usable_search_result

        results = []
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
                continue
            if href in seen:
                continue
            if is_blacklisted_official(href):
                continue
            results.append(SearchResult(rank=len(results) + 1, title=title, url=href, snippet=snippet, provider="google"))
            seen.add(href)
            if len(results) >= SEARCH_RESULTS_LIMIT:
                break
        return results

    def _extract_panel_from_soup(self, enricher, soup, record):
        import re

        from enrich_daycare_data import LOGGER
        from enrich_daycare_data import clean_text, extract_google_target_url, normalize_phone, normalize_url

        panel_root = soup.select_one("#rhs")
        if not panel_root:
            return {}

        panel_text = clean_text(panel_root.get_text("\n", strip=True))
        address_candidates = []
        for node in panel_root.select("span[class]"):
            class_name = clean_text(node.get("class", [""])[0] if isinstance(node.get("class"), list) else node.get("class", ""))
            text_value = clean_text(node.get_text(" ", strip=True))
            raw_class = clean_text(" ".join(node.get("class", [])) if isinstance(node.get("class"), list) else node.get("class", ""))
            if not text_value or not raw_class or len(raw_class.split()) != 1:
                continue
            if "united states" not in text_value.lower():
                continue
            address_candidates.append(text_value)
        address_text = address_candidates[0] if address_candidates else panel_text
        address_match = re.search(
            r"(\d[\w#&.,'/ -]{4,}?)(?:,\s*|\s+)(?:.*?,\s*)?([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b",
            address_text,
        )
        mailing_address = clean_text(address_match.group(1)) if address_match else ""
        mailing_zip = clean_text(address_match.group(3)) if address_match else ""

        phone_value = ""
        for phone_node in panel_root.select("span[aria-label]"):
            aria = clean_text(phone_node.get("aria-label", ""))
            text_value = clean_text(phone_node.get_text(" ", strip=True))
            if "call phone number" not in aria.lower():
                continue
            raw_phone = text_value
            if not raw_phone:
                match = re.search(r"Call phone number\s+(.+)$", aria, re.I)
                raw_phone = clean_text(match.group(1)) if match else ""
            digits = re.sub(r"\D", "", raw_phone)
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            phone_value = normalize_phone(digits)
            if phone_value:
                break

        website_url = ""
        for node in panel_root.select("a[href][ping]"):
            text_value = clean_text(node.get_text(" ", strip=True)).lower()
            if text_value != "website":
                continue
            href = extract_google_target_url(node.get("href", "")) or normalize_url(node.get("href", ""))
            if href and "google." not in href and not self._looks_like_bad_fallback_url(href):
                website_url = self._root_url(href)
                break

        for selector in (
            'a[data-attrid*="visit_website"]',
            'a[data-attrid*="authority"]',
            'a[data-attrid*="website"]',
            'a[aria-label="Website"]',
        ):
            if website_url:
                break
            node = panel_root.select_one(selector)
            if not node:
                continue
            href = extract_google_target_url(node.get("href", "")) or normalize_url(node.get("href", ""))
            if href and "google." not in href and not self._looks_like_bad_fallback_url(href):
                website_url = self._root_url(href)
                break

        panel_name = self._extract_panel_title(panel_root)

        has_gkp = bool(
            panel_name
            or panel_root.select_one("[role='heading'][data-attrid='title']")
            or panel_root.select_one("div[data-attrid='title']")
            or panel_root.select_one('[aria-label^="Call phone number "]')
            or panel_root.select_one('a[data-attrid*="visit_website"]')
        )

        values = {
            "Matched_Provider_Name": panel_name,
            "Mailing_Address": mailing_address,
            "Mailing_Zip": mailing_zip,
            "Telephone": phone_value,
            "URL": website_url,
        }
        strong_panel = self._has_strong_panel_signals(values)
        if not has_gkp or not strong_panel:
            return {}
        LOGGER.info(
            "Google adapter accepted soup panel via panel-presence rule for PID=%s panel_name=%s values=%s",
            record.get("PID", ""),
            panel_name,
            values,
        )

        return values

    def _extract_panel_from_dom(self, driver, record):
        import re

        from enrich_daycare_data import LOGGER
        from enrich_daycare_data import clean_text, normalize_phone

        try:
            panel_data = driver.execute_script(
                """
const root = document.querySelector('#rhs');
if (!root) return null;

const textOf = (node) => node ? ((node.innerText || node.textContent || '').trim()) : '';

const titleNode =
  root.querySelector("[role='heading'][data-attrid='title']") ||
  root.querySelector("div[data-attrid='title'] [role='heading']") ||
  root.querySelector("div[data-attrid='title'] span") ||
  root.querySelector("h2[data-attrid='title']") ||
  root.querySelector("div.kno-ecr-pt span") ||
  root.querySelector("div.kp-header h2 span") ||
  root.querySelector("div[data-attrid='kc:/location/location:entity_title'] span") ||
  root.querySelector("div[data-attrid='kc:/organization/organization:entity_title'] span") ||
  root.querySelector("div[data-attrid='kc:/education/school:entity_title'] span") ||
  root.querySelector("div.SPZz6b span");

const phoneCandidates = Array.from(root.querySelectorAll("span[aria-label]"))
  .map(el => ({
    text: textOf(el),
    aria: el.getAttribute('aria-label') || '',
  }))
  .filter(item => item.text);

const phoneNode = phoneCandidates.find(
  item => /call phone number/i.test(item.aria)
) || phoneCandidates.find(
  item => /\+?1?[\s().-]*\d{3}[\s().-]*\d{3}[\s().-]*\d{4}/.test(item.text)
);

const websiteCandidates = Array.from(root.querySelectorAll("a[href][ping]"))
  .map(el => ({
    href: el.href || '',
    text: textOf(el),
  }))
  .filter(item => item.href && !/google\\./i.test(item.href));

const websiteNode = websiteCandidates.find(
  item => item.text.toLowerCase() === 'website'
) || websiteCandidates[0] || null;

const addressCandidates = Array.from(root.querySelectorAll("span[class]"))
  .map(el => ({
    text: textOf(el),
    className: (el.className || '').toString().trim()
  }))
  .filter(item => item.text && item.className && item.className.split(/\\s+/).length === 1);

const addressNode = addressCandidates.find(item => item.text.toLowerCase().includes('united states'));

const resultLinks = Array.from(document.querySelectorAll('#center_col a[href][ping]'))
  .map(el => ({
    href: el.href || '',
    text: textOf(el),
  }))
  .filter(item => item.href);

return {
  panelText: textOf(root),
  title: textOf(titleNode),
  phoneText: phoneNode ? phoneNode.text : '',
  phoneAria: phoneNode ? phoneNode.aria : '',
  websiteHref: websiteNode ? websiteNode.href : '',
  addressText: addressNode ? addressNode.text : '',
  resultLinks
};
"""
            )
        except Exception:
            return {}

        if not isinstance(panel_data, dict):
            return {}

        panel_name = clean_text(panel_data.get("title", ""))
        lowered_panel_name = panel_name.lower()
        if lowered_panel_name in self.BAD_PANEL_TITLES:
            panel_name = ""
        if any(fragment in lowered_panel_name for fragment in self.BAD_PANEL_TITLE_FRAGMENTS):
            panel_name = ""

        address_text = clean_text(panel_data.get("addressText", "")) or clean_text(panel_data.get("panelText", ""))
        address_match = re.search(
            r"(\d[\w#&.,'/ -]{4,}?)(?:,\s*|\s+)(?:.*?,\s*)?([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b",
            address_text,
        )
        mailing_address = clean_text(address_match.group(1)) if address_match else ""
        mailing_zip = clean_text(address_match.group(3)) if address_match else ""

        raw_phone = clean_text(panel_data.get("phoneText", ""))
        if not raw_phone:
            aria = clean_text(panel_data.get("phoneAria", ""))
            phone_match = re.search(r"Call phone number\s+(.+)$", aria, re.I)
            raw_phone = clean_text(phone_match.group(1)) if phone_match else ""
        digits = re.sub(r"\D", "", raw_phone)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        phone_value = normalize_phone(digits)

        website_url = ""
        href = clean_text(panel_data.get("websiteHref", ""))
        if href and not self._looks_like_bad_fallback_url(href):
            website_url = self._root_url(href)

        if not website_url:
            result_links = panel_data.get("resultLinks", []) or []
            best_result = None
            best_score = -999
            for result in result_links[:5]:
                result_href = clean_text(result.get("href", ""))
                if self._looks_like_bad_fallback_url(result_href):
                    continue
                score = self._score_result_url(
                    record,
                    type(
                        "DomSearchResult",
                        (),
                        {
                            "title": clean_text(result.get("text", "")),
                            "snippet": "",
                            "url": result_href,
                            "rank": len(result_links),
                        },
                    )(),
                )
                if score > best_score:
                    best_score = score
                    best_result = result_href
            if best_result:
                website_url = self._root_url(best_result)

        values = {
            "Matched_Provider_Name": panel_name,
            "Mailing_Address": mailing_address,
            "Mailing_Zip": mailing_zip,
            "Telephone": phone_value,
            "URL": website_url,
        }
        strong_panel = self._has_strong_panel_signals(values)
        if not strong_panel:
            return {}
        LOGGER.info(
            "Google adapter accepted DOM panel via panel-presence rule for PID=%s panel_name=%s values=%s",
            record.get("PID", ""),
            panel_name,
            values,
        )
        return values

    def _score_result_url(self, record, result):
        from enrich_daycare_data import clean_text, domain_of, get_record_name_profile, token_overlap_score

        profile = get_record_name_profile(record)
        haystack = f"{result.title} {result.snippet} {domain_of(result.url)}"
        score = token_overlap_score(record.get("Daycare_Name", ""), haystack) * 4
        if clean_text(record.get("Mailing_City", "")).lower() in haystack.lower():
            score += 2
        if clean_text(record.get("Mailing_State", "")).lower() in haystack.lower():
            score += 1
        if result.rank <= 5:
            score += max(0, 6 - result.rank)
        variant_hit = any(
            clean_text(v) and len(clean_text(v)) >= 4 and clean_text(v).lower() in haystack.lower()
            for v in profile.search_name_variants
        )
        if variant_hit:
            score += 4
        return score

    def _fetch_values(self, enricher, record, query):
        import random
        import time

        from bs4 import BeautifulSoup

        from enrich_daycare_data import LOGGER, clean_text

        state = clean_text(record.get("Mailing_State", "")).upper()
        driver = enricher.get_search_driver()
        self._submit_query(driver, query)
        if self._is_blocked(driver):
            self._enforce_backoff(enricher, state, query)
            return None
        self._wait_for_results_surface(driver)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        panel_values = self._extract_panel_from_soup(enricher, soup, record)
        if not panel_values:
            panel_values = self._extract_panel_from_dom(driver, record)
        if not panel_values:
            time.sleep(2.5)
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            panel_values = self._extract_panel_from_soup(enricher, soup, record)
            if not panel_values:
                panel_values = self._extract_panel_from_dom(driver, record)
        if not panel_values:
            LOGGER.info(
                "Google adapter did not find a real knowledge panel for PID=%s query=%s",
                record.get("PID", ""),
                query,
            )
            LOGGER.info(
                "Google miss diagnostics for PID=%s: has_rhs=%s page_title=%s",
                record.get("PID", ""),
                "#rhs" in html,
                clean_text(getattr(driver, "title", "")),
            )
            self._human_pause(driver)
            return {}
        results = self._parse_results_from_soup(soup)[:5]
        if not clean_text(panel_values.get("URL", "")) and results:
            best_result = None
            best_score = -999
            for result in results:
                if self._looks_like_bad_fallback_url(result.url):
                    continue
                score = self._score_result_url(record, result)
                if score > best_score:
                    best_score = score
                    best_result = result
            if best_result:
                panel_values["URL"] = self._root_url(best_result.url)
        LOGGER.info(
            "Google adapter parsed PID=%s query=%s panel_values=%s result_count=%s",
            record.get("PID", ""),
            query,
            panel_values,
            len(results),
        )
        self._human_pause(driver)
        enricher.reset_google_backoff_seconds(state)
        return panel_values

    def run(self, enricher, record):
        import time

        from selenium.common.exceptions import InvalidSessionIdException, NoSuchWindowException, WebDriverException

        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, normalize_phone, normalize_url, normalize_zip

        query = self._build_query(record)
        last_error = None
        values = None
        for attempt in range(2):
            try:
                values = self._fetch_values(enricher, record, query)
                if values is None:
                    enricher.queue_google_antibot_retry(record)
                    return {}, {}
                if not values:
                    if attempt < 1:
                        sleep_seconds = self._retry_backoff_seconds(attempt)
                        LOGGER.info(
                            "Google adapter found no panel for PID=%s attempt=%s; backing off for %.2f seconds before retrying query=%s",
                            record.get("PID", ""),
                            attempt + 1,
                            sleep_seconds,
                            query,
                        )
                        time.sleep(sleep_seconds)
                        continue
                    return {}, {}
                break
            except (InvalidSessionIdException, NoSuchWindowException, WebDriverException) as exc:
                last_error = exc
                LOGGER.warning(
                    "Google adapter search driver failed for PID=%s attempt=%s; restarting browser and retrying query=%s",
                    record.get("PID", ""),
                    attempt + 1,
                    query,
                )
                try:
                    enricher.reset_search_driver()
                except Exception:
                    pass
                if attempt < 1:
                    sleep_seconds = self._retry_backoff_seconds(attempt)
                    LOGGER.info(
                        "Google adapter retry backoff for PID=%s attempt=%s sleeping %.2f seconds",
                        record.get("PID", ""),
                        attempt + 1,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)
        if values is None:
            if last_error:
                LOGGER.error(
                    "Google adapter failed for PID=%s query=%s last_error=%s",
                    record.get("PID", ""),
                    query,
                    last_error,
                )
            return {}, {}

        normalized_values = {
            "Mailing_Address": clean_text(values.get("Mailing_Address", "")),
            "Mailing_Zip": normalize_zip(values.get("Mailing_Zip", "")),
            "Telephone": normalize_phone(values.get("Telephone", "")),
            "URL": normalize_url(values.get("URL", "")),
        }
        matched_provider_name = clean_text(values.get("Matched_Provider_Name", "")) or clean_text(record.get("Daycare_Name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city="",
            candidate_address=normalized_values.get("Mailing_Address", ""),
            candidate_phone=normalized_values.get("Telephone", ""),
            candidate_url=normalized_values.get("URL", ""),
        )
        normalized_values.update(
            {
                "Capacity (optional)": "",
                "Age Range (optional)": "",
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = normalized_values.get("URL", "") or "https://www.google.com/"
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="google_search",
                notes="Google knowledge panel / search result fallback",
            )
            for field, value in normalized_values.items()
            if clean_text(value)
        }
        return normalized_values, sources

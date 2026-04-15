from adapters.base import StateAdapter


class WinnieFallbackAdapter(StateAdapter):
    state = "WINNIE"

    def _human_pause(self):
        import random
        import time

        time.sleep(random.uniform(2.0, 3.0))

    def _human_mouse_move(self, driver):
        import random

        from selenium.webdriver.common.action_chains import ActionChains
        from selenium.webdriver.common.by import By

        try:
            body = driver.find_element(By.TAG_NAME, "body")
            width = driver.execute_script("return Math.max(window.innerWidth || 0, 300);") or 300
            height = driver.execute_script("return Math.max(window.innerHeight || 0, 300);") or 300
            offset_x = random.randint(40, max(int(width) - 40, 41))
            offset_y = random.randint(40, max(int(height) - 40, 41))
            ActionChains(driver).move_to_element_with_offset(body, offset_x, offset_y).perform()
        except Exception:
            pass

    def _captcha_present(self, driver):
        try:
            return bool(
                driver.execute_script(
                    """
const hardClassNode = document.querySelector('.captcha__ddv1');
if (hardClassNode) return true;
const node = document.querySelector('#captcha__frame__bottom');
if (!node) return false;
const text = (node.innerText || node.textContent || '').toLowerCase();
return node.classList.contains('toggled') || text.includes('slide right to secure your access');
"""
                )
            )
        except Exception:
            return False

    def _enforce_backoff(self, enricher, state, city=""):
        import time

        from enrich_daycare_data import LOGGER

        wait_seconds = enricher.get_next_winnie_backoff_seconds(state)
        LOGGER.info(
            "Winnie captcha detected for state=%s city=%s; resetting browser and backing off for %.0f seconds",
            state,
            city,
            wait_seconds,
        )
        try:
            enricher.reset_state_portal_driver(f"WINNIE_{state}")
        except Exception:
            pass
        time.sleep(wait_seconds)

    def _normalize_city_key(self, value):
        from enrich_daycare_data import clean_text, simplify_name

        return simplify_name(clean_text(value))

    def _slugify(self, value):
        import re

        from enrich_daycare_data import clean_text

        text = clean_text(value).lower()
        text = re.sub(r"[^a-z0-9]+", "-", text)
        return text.strip("-")

    def _score_candidate(self, record, provider_name):
        from enrich_daycare_data import clean_text, get_record_name_profile, token_overlap_metrics

        profile = get_record_name_profile(record)
        shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
        score = shared * 4
        variant_hit = any(
            clean_text(variant) and len(clean_text(variant)) >= 4 and clean_text(variant).lower() in provider_name.lower()
            for variant in profile.search_name_variants
        )
        if variant_hit:
            score += 4
        return score, recall

    def _fetch_detail(self, enricher, driver, detail_url):
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import SELENIUM_WAIT_TIMEOUT, clean_text, normalize_zip

        self._human_mouse_move(driver)
        self._human_pause()
        existing_handles = set(driver.window_handles)
        driver.execute_script("window.open(arguments[0], '_blank');", detail_url)
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
        new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
        if new_handles:
            driver.switch_to.window(new_handles[-1])
        self._human_mouse_move(driver)
        self._human_pause()
        if self._captcha_present(driver):
            self._enforce_backoff(enricher, "WINNIE", detail_url)
            raise RuntimeError("Winnie slider captcha detected on detail page")
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        detail_values = driver.execute_script(
            """
const normalize = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/\\s+/g, ' ').trim();
const addressParagraphs = Array.from(document.querySelectorAll('div.address address p')).map((node) => normalize(node.innerText || node.textContent || '')).filter(Boolean);
const ageBadges = Array.from(document.querySelectorAll('.badge.ages')).map((node) => normalize(node.innerText || node.textContent || '')).filter(Boolean);
const capacityNode = document.querySelector('div.capacity p');
return {
  address_lines: addressParagraphs,
  age_badges: ageBadges,
  capacity: normalize(capacityNode ? (capacityNode.innerText || capacityNode.textContent || '') : ''),
  detail_url: window.location.href || ''
};
""",
        ) or {}
        address_lines = detail_values.get("address_lines") or []
        age_badges = detail_values.get("age_badges") or []
        first_badge = clean_text(age_badges[0]) if age_badges else ""
        last_badge = clean_text(age_badges[-1]) if age_badges else ""
        lower = clean_text(first_badge.split("-")[0]) if first_badge else ""
        upper = clean_text(last_badge.split("-")[-1]) if last_badge else ""
        values = {
            "Mailing_Address": clean_text(address_lines[0]).rstrip(",") if len(address_lines) > 1 else "",
            "Mailing_Zip": normalize_zip(clean_text(address_lines[1]).split(" ")[-1] if len(address_lines) > 1 and clean_text(address_lines[1]).split(" ") else ""),
            "Telephone": "",
            "URL": "",
            "Capacity (optional)": clean_text(detail_values.get("capacity", "")),
            "Age Range (optional)": clean_text(f"{lower} - {upper}") if lower and upper else "",
            "Detail_URL": clean_text(detail_values.get("detail_url", "")),
        }
        try:
            driver.close()
        except Exception:
            pass
        remaining_handles = driver.window_handles
        if remaining_handles:
            driver.switch_to.window(remaining_handles[0])
        return values

    def run_city_retry(self, enricher, state, city, records_by_pid):
        from selenium.common.exceptions import TimeoutException
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import (
            LOGGER,
            SELENIUM_WAIT_TIMEOUT,
            STATE_NAMES,
            build_source_entry,
            classify_match_status,
            clean_text,
            normalize_age_groups_text_to_numeric_range,
            normalize_zip,
        )

        if not records_by_pid:
            return {}
        state_name = clean_text(STATE_NAMES.get(state, ""))
        if not state_name:
            return {}
        state_slug = state_name.lower().replace(" ", "-")
        state_abbrev_slug = clean_text(state).lower()
        state_key = f"WINNIE_{state}"
        state_url = f"https://winnie.com/browse/{state_slug}"
        resolved = {}
        try:
            driver = enricher.open_or_reuse_state_portal_query_tab(
                state_key,
                state_url,
                ready_locator=(By.TAG_NAME, "body"),
            )
            LOGGER.info("Winnie fallback loaded state browse page for state=%s url=%s", state, state_url)
            if self._captcha_present(driver):
                self._enforce_backoff(enricher, state, city)
                return {}
            enricher.reset_winnie_backoff_seconds(state)
            try:
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                    lambda d: len(
                        d.find_elements(By.CSS_SELECTOR, "a[href*='/browse/']")
                    ) > 0
                )
            except Exception:
                LOGGER.info("Winnie fallback did not see browse links yet for state=%s url=%s", state, state_url)
            city_links = driver.execute_script(
                """
return Array.from(document.querySelectorAll('a[href]')).map((node) => ({
  name: (node.innerText || node.textContent || '').trim(),
  href: node.href || node.getAttribute('href') || ''
})).filter((item) => item.name && item.href && item.href.includes('/browse/'));
"""
            ) or []
            if not city_links:
                LOGGER.info("Winnie fallback treating missing browse links as a captcha/block signal for state=%s city=%s", state, city)
                self._enforce_backoff(enricher, state, city)
                return {}
            city_key = self._normalize_city_key(city)
            expected_city_slug = f"/browse/{state_slug}/{self._slugify(city)}-{state_abbrev_slug}"
            city_match = next(
                (
                    item
                    for item in city_links
                    if clean_text(item.get("href", "")).lower().rstrip("/") == f"https://winnie.com{expected_city_slug}".rstrip("/")
                ),
                None,
            ) or next(
                (
                    item
                    for item in city_links
                    if expected_city_slug in clean_text(item.get("href", "")).lower()
                ),
                None,
            ) or next(
                (
                    item
                    for item in city_links
                    if self._normalize_city_key(item.get("name", "")) == city_key
                    and f"/browse/{state_slug}/" in clean_text(item.get("href", "")).lower()
                ),
                None,
            )
            if not city_match:
                LOGGER.info("Winnie fallback did not find city match for state=%s city=%s expected_slug=%s", state, city, expected_city_slug)
                return {}
            city_url = clean_text(city_match.get("href", ""))
            LOGGER.info("Winnie fallback matched city for state=%s city=%s city_url=%s", state, city, city_url)
            self._human_mouse_move(driver)
            self._human_pause()
            existing_handles = set(driver.window_handles)
            LOGGER.info("Winnie fallback opening city page in new tab for state=%s city=%s", state, city)
            driver.execute_script("window.open(arguments[0], '_blank');", city_url)
            WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
            new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
            if new_handles:
                driver.switch_to.window(new_handles[-1])
            self._human_mouse_move(driver)
            self._human_pause()
            if self._captcha_present(driver):
                self._enforce_backoff(enricher, state, city)
                return {}
            try:
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/place/']"))
                )
            except TimeoutException:
                LOGGER.info("Winnie fallback timed out waiting for place links; treating as captcha/block for state=%s city=%s", state, city)
                self._enforce_backoff(enricher, state, city)
                return {}
            candidates = driver.execute_script(
                """
return Array.from(document.querySelectorAll('a[href*="/place/"]')).map((node) => ({
  provider_name: (node.innerText || node.textContent || '').trim(),
  detail_url: node.href || node.getAttribute('href') || ''
})).filter((item) => item.provider_name && item.detail_url);
"""
            ) or []
            for pid, record in records_by_pid.items():
                best_candidate = None
                best_score = -999
                for candidate in candidates:
                    score, _recall = self._score_candidate(record, clean_text(candidate.get("provider_name", "")))
                    if score > best_score:
                        best_score = score
                        best_candidate = candidate
                if not best_candidate:
                    continue
                LOGGER.info(
                    "Winnie fallback selected candidate for PID=%s state=%s city=%s provider=%s score=%s detail_url=%s",
                    pid,
                    state,
                    city,
                    clean_text(best_candidate.get("provider_name", "")),
                    best_score,
                    clean_text(best_candidate.get("detail_url", "")),
                )
                detail_values = self._fetch_detail(enricher, driver, clean_text(best_candidate.get("detail_url", "")))
                values = {
                    "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")),
                    "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "")),
                    "Telephone": "",
                    "URL": "",
                    "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
                    "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", "")),
                }
                matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
                match_status, match_confidence, match_reason = classify_match_status(
                    record,
                    candidate_name=matched_provider_name,
                    candidate_city=city,
                    candidate_address=values.get("Mailing_Address", ""),
                    candidate_phone="",
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
                confidence = int(clean_text(values.get("Match_Confidence", "0")) or "0")
                if clean_text(values.get("Match_Status", "")) == "not_found" and confidence <= 60:
                    continue
                source_url = clean_text(detail_values.get("Detail_URL", "")) or clean_text(best_candidate.get("detail_url", "")) or state_url
                LOGGER.info(
                    "Winnie fallback success values for PID=%s state=%s city=%s values=%s",
                    pid,
                    state,
                    city,
                    values,
                )
                sources = {
                    field: build_source_entry(
                        value=value,
                        source_url=source_url,
                        source_type="trusted_public_listing",
                        notes="Winnie fallback listing",
                    )
                    for field, value in values.items()
                    if clean_text(value)
                }
                resolved[pid] = (values, sources)
                LOGGER.info("Winnie fallback matched PID=%s state=%s city=%s", pid, state, city)
            return resolved
        finally:
            enricher.finalize_state_portal_query(state_key)

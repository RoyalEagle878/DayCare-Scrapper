from adapters.base import StateAdapter


class SouthCarolinaAdapter(StateAdapter):
    state = "SC"

    def search(self, enricher, record):
        import time
        from selenium.common.exceptions import TimeoutException
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from enrich_daycare_data import SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile, normalize_phone, normalize_zip

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        home_url = "https://search.sc-ccrr.org/search"
        try:
            session_flags = enricher.state_portal_session_flags.setdefault("SC", {})
            for variant in profile.search_name_variants[:4]:
                driver = enricher.open_or_reuse_state_portal_query_tab("SC", home_url, ready_locator=(By.ID, "formly_3_input_name_2"))
                previous_signature = driver.execute_script("return Array.from(document.querySelectorAll('app-program-public-search-result-card h3 .item, app-program-public-search-result-card h2.title')).map((node) => (node.innerText || node.textContent || '').trim()).filter(Boolean).join(' || ');") or ""
                name_input = driver.find_element(By.ID, "formly_3_input_name_2")
                location_input = driver.find_element(By.ID, "mat-input-36")
                driver.execute_script("arguments[0].click();", name_input)
                name_input.send_keys(Keys.CONTROL, "a")
                name_input.send_keys(Keys.DELETE)
                name_input.send_keys(variant)
                driver.execute_script("arguments[0].click();", location_input)
                location_input.send_keys(Keys.CONTROL, "a")
                location_input.send_keys(Keys.DELETE)
                location_input.send_keys(city)
                try:
                    suggestion = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(lambda d: next((element for element in d.find_elements(By.CSS_SELECTOR, ".pac-item") if element.is_displayed() and city.lower() in clean_text(element.text).lower() and "sc" in clean_text(element.text).lower()), None))
                    try:
                        suggestion.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", suggestion)
                except Exception:
                    pass
                if not session_flags.get("centers_checked"):
                    checkbox = driver.find_element(By.ID, "formly_4_checkboxes_publicProgramType_0_0-input")
                    if not checkbox.is_selected():
                        driver.execute_script("arguments[0].click();", checkbox)
                    session_flags["centers_checked"] = True
                search_button = driver.find_element(By.CSS_SELECTOR, "button.search-btn")
                try:
                    search_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", search_button)
                time.sleep(2.0)
                wait_timed_out = False
                try:
                    WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(lambda d: ("showing 0 programs that match your search" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower() or "no results found" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower() or len(d.find_elements(By.CSS_SELECTOR, "app-program-public-search-result-card")) > 0) and ("showing 0 programs that match your search" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower() or "no results found" in clean_text(d.find_element(By.TAG_NAME, "body").text).lower() or ((d.execute_script("return Array.from(document.querySelectorAll('app-program-public-search-result-card h3 .item, app-program-public-search-result-card h2.title')).map((node) => (node.innerText || node.textContent || '').trim()).filter(Boolean).join(' || ');") or "") != previous_signature or not previous_signature)))
                except TimeoutException:
                    wait_timed_out = True
                body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text).lower()
                if "showing 0 programs that match your search" in body_text or "no results found" in body_text:
                    continue
                candidate_rows = driver.execute_script("""const rows = []; Array.from(document.querySelectorAll('app-program-public-search-result-card')).forEach((card, index) => { const titleNode = card.querySelector('h3 .item, h2.title'); const streetNode = card.querySelector('app-view-address-block .street-number'); const cityNode = card.querySelector('app-view-address-block .city'); const stateNode = card.querySelector('app-view-address-block .state'); const zipNode = card.querySelector('app-view-address-block .zip'); const phoneNode = card.querySelector('app-view-contact-block .phone-display a, app-view-contact-block a[href^=\"tel:\"]'); const addressText = [ (streetNode && (streetNode.innerText || streetNode.textContent || '')) || '', [cityNode && (cityNode.innerText || cityNode.textContent || ''), stateNode && (stateNode.innerText || stateNode.textContent || ''), zipNode && (zipNode.innerText || zipNode.textContent || '')].filter(Boolean).join(' ') ].filter(Boolean).join('\\n'); const profileButton = Array.from(card.querySelectorAll('a,button')).find((element) => /view profile/i.test((element.innerText || element.textContent || '').trim())); rows.push({ candidate_index:index, provider_name:titleNode ? (titleNode.innerText || titleNode.textContent || '') : '', address_text:addressText, phone_text:phoneNode ? (phoneNode.innerText || phoneNode.textContent || '') : '', has_profile_button:!!profileButton }); }); return rows;""")
                results = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    address_text = clean_text(item.get("address_text", ""))
                    if not provider_name or not bool(item.get("has_profile_button")):
                        continue
                    results.append({"candidate_index": str(item.get("candidate_index", "")), "provider_name": provider_name, "address": address_text, "city": city if city and city.lower() in address_text.lower() else "", "zip": normalize_zip(address_text), "phone": normalize_phone(item.get("phone_text", "")), "row_text": address_text})
                if results:
                    return results
            return []
        except Exception:
            enricher.reset_state_portal_driver("SC")
            raise

    def fetch_detail(self, enricher, driver, candidate_index, provider_name, action_label):
        import time
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from enrich_daycare_data import SELENIUM_WAIT_TIMEOUT, clean_text, domain_of, normalize_phone, normalize_url, normalize_zip, re

        index = int(candidate_index)
        profile_button = driver.execute_script("""const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase(); const target = normalize(arguments[0]); const cards = Array.from(document.querySelectorAll('app-program-public-search-result-card')); let card = cards.find((item) => { const title = normalize(item.querySelector('h3 .item, h2.title')?.innerText || item.querySelector('h3 .item, h2.title')?.textContent || ''); return title === target || (target && title.includes(target)); }); if (!card && Number.isInteger(arguments[1]) && arguments[1] >= 0 && arguments[1] < cards.length) card = cards[arguments[1]]; if (!card) return null; return card.querySelector('button.profile-btn') || Array.from(card.querySelectorAll('a,button')).find((element) => /view profile/i.test((element.innerText || element.textContent || '').trim())) || null;""", provider_name, index)
        if profile_button is None:
            raise RuntimeError("Unable to locate South Carolina View Profile button")
        try:
            profile_button.click()
        except Exception:
            driver.execute_script("arguments[0].click();", profile_button)
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "app-program-public-search-result-slide app-public-profile"))
        )
        time.sleep(2.0)
        detail_values = driver.execute_script(
            """
const normalize = (value) => (value || '').replace(/\\u00a0/g, ' ').replace(/[ \\t]+/g, ' ').trim();
const root = document.querySelector('app-program-public-search-result-slide app-public-profile');
if (!root) return {};
const streetNode = root.querySelector('app-view-section:nth-of-type(1) app-view-group:nth-of-type(3) app-view-address-block .street-number');
const zipNode = root.querySelector('app-view-section:nth-of-type(1) app-view-group:nth-of-type(3) app-view-address-block .zip');
const websiteNode = root.querySelector('app-view-section:nth-of-type(1) app-view-group:nth-of-type(4) app-view-link-field a');
const phoneNode = root.querySelector('app-view-section:nth-of-type(1) app-view-group:nth-of-type(2) app-view-contact-block address div.contact-links div.phone-display a, app-view-section:nth-of-type(1) app-view-group:nth-of-type(2) app-view-contact-block a[href^="tel:"]');
const capacityNode = root.querySelector('app-view-section:nth-of-type(3) app-view-group:nth-of-type(2) app-view-number-field-block app-view-number-field span, app-view-section:nth-of-type(3) app-view-group:nth-of-type(2) app-view-number-field-block .item');
const ageRoot = root.querySelector('app-view-section:nth-of-type(3) app-view-group:nth-of-type(2) app-view-multi-selection-field-block:nth-of-type(1) app-view-multi-selection-field');
const ageItems = ageRoot ? Array.from(ageRoot.querySelectorAll('.item, li, div, span')).map((node) => ({
  text: normalize(node.innerText || node.textContent || ''),
  checked: (node.className || '').toString().toLowerCase().includes('checked')
})).filter((item) => item.text) : [];
return {
  street: normalize(streetNode ? (streetNode.innerText || streetNode.textContent || '') : ''),
  zip: normalize(zipNode ? (zipNode.innerText || zipNode.textContent || '') : ''),
  website: websiteNode ? (websiteNode.href || websiteNode.getAttribute('href') || '') : '',
  phone: normalize(phoneNode ? (phoneNode.getAttribute('href') || phoneNode.href || phoneNode.innerText || phoneNode.textContent || '') : ''),
  capacity: normalize(capacityNode ? (capacityNode.innerText || capacityNode.textContent || '') : ''),
  age_items: ageItems,
  detail_url: window.location.href || ''
};
"""
        ) or {}
        mailing_address = clean_text(detail_values.get("street", ""))
        zip_source = clean_text(detail_values.get("zip", ""))
        age_items = detail_values.get("age_items") or []
        accepted_rows = [
            clean_text(item.get("text", ""))
            for item in age_items
            if item.get("checked") and re.search(r"\([^)]+\)", clean_text(item.get("text", "")))
        ]
        def extract_age_range_portion(label):
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
        values = {
            "Mailing_Address": mailing_address,
            "Mailing_Zip": normalize_zip(zip_source),
            "Telephone": normalize_phone(detail_values.get("phone", "")),
            "URL": clean_text(detail_values.get("website", "")),
            "Capacity (optional)": clean_text(detail_values.get("capacity", "")),
            "Age Range (optional)": clean_text(f"{first_lower} - {last_upper}") if first_lower and last_upper else "",
            "Detail_URL": clean_text(detail_values.get("detail_url", "")),
        }
        website_url = normalize_url(values.get("URL", ""))
        if domain_of(website_url) in {"search.sc-ccrr.org", "www.search.sc-ccrr.org", "sc-ccrr.org", "www.sc-ccrr.org"}:
            values["URL"] = ""
        try:
            close_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[5]/button")))
            try:
                close_button.click()
            except Exception:
                driver.execute_script("arguments[0].click();", close_button)
        except Exception:
            pass
        return values

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, get_record_name_profile, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_zip, token_overlap_metrics
        portal_url = "https://search.sc-ccrr.org/search"
        try:
            candidates = self.search(enricher, record)
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
            variant_hit = any(clean_text(v) and len(clean_text(v)) >= 4 and clean_text(v).lower() in provider_name.lower() for v in profile.search_name_variants[:4])
            if variant_hit:
                score += 4
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate or (best_score < 6 and not (best_city_match and best_overlap >= 0.35)):
            return {}, {}
        driver = enricher.get_state_portal_driver("SC")
        try:
            detail_values = self.fetch_detail(enricher, driver=driver, candidate_index=best_candidate.get("candidate_index", ""), provider_name=best_candidate.get("provider_name", ""), action_label=f"south carolina detail page [{record.get('PID', '')}]")
        except Exception:
            LOGGER.exception("South Carolina detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        values = {"Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")), "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "")), "Telephone": normalize_phone(detail_values.get("Telephone", "")), "URL": clean_text(detail_values.get("URL", "")), "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")), "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", ""))}
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(best_candidate.get("city", "")), candidate_address=values.get("Mailing_Address", "") or best_candidate.get("address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=values.get("URL", ""))
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = clean_text(detail_values.get("Detail_URL", "")) or values.get("URL", "") or portal_url
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="South Carolina CCR&R search") for field, value in values.items() if clean_text(value)}
        return values, sources

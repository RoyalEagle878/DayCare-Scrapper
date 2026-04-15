from adapters.base import StateAdapter


class OklahomaAdapter(StateAdapter):
    state = "OK"

    def search(self, enricher, record):
        from urllib.parse import quote_plus
        import time
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile

        profile = get_record_name_profile(record)
        try:
            for variant in profile.search_name_variants[:4]:
                query_url = f"https://ccl.dhs.ok.gov/providers?provider-name={quote_plus(variant)}"
                driver = enricher.open_state_portal_query_tab("OK", query_url)
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
  const actionButton = item.querySelector('span:nth-of-type(2) button, button');
  const buttonText = ((actionButton && actionButton.innerText) || '').trim();
  const rowText = (item.innerText || '').trim();
  let cityValue = '';
  const rowParts = rowText.split(/\\s+/).filter(Boolean);
  if (rowParts.length > 3) cityValue = rowParts[3].replace(/,\\s*$/, '').trim();
  rows.push({ candidate_index: candidateIndex, provider_name: buttonText, city: cityValue, row_text: rowText });
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
                            "provider_name": provider_name,
                            "city": clean_text(item.get("city", "")),
                            "row_text": clean_text(item.get("row_text", "")),
                            "candidate_index": str(item.get("candidate_index", "")),
                        }
                    )
                if results:
                    return results
                enricher.finalize_state_portal_query("OK")
            return []
        except Exception:
            enricher.reset_state_portal_driver("OK")
            raise

    def fetch_detail(self, enricher, driver, candidate_index, action_label):
        import time
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, normalize_phone, normalize_url, normalize_zip, re

        LOGGER.info("Fetching Oklahoma detail page via Selenium action=%s candidate_index=%s", action_label, candidate_index)
        list_root = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div/main/form/div[4]/div[1]/ul"))
        )
        items = list_root.find_elements(By.XPATH, "./li")
        index = int(candidate_index)
        if index < 0 or index >= len(items):
            raise RuntimeError(f"Oklahoma candidate_index={candidate_index} is out of bounds for {len(items)} rows")
        detail_link = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"/html/body/div/main/form/div[4]/div[1]/ul/li[{index + 1}]/div/a"))
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
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(EC.presence_of_element_located((By.XPATH, phone_xpath)))
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(EC.presence_of_element_located((By.XPATH, address_xpath)))
        time.sleep(1.0)
        phone_text = normalize_phone(driver.find_element(By.XPATH, phone_xpath).text)
        address_text = clean_text(driver.find_element(By.XPATH, address_xpath).text)
        capacity_text = ""
        try:
            capacity_text = clean_text(driver.find_element(By.XPATH, capacity_xpath).text)
            capacity_text = re.sub(r"^\s*Total\s+Capacity\s*", "", capacity_text, flags=re.IGNORECASE).strip(" :-")
        except Exception:
            pass
        return {
            "Mailing_Address": address_text,
            "Mailing_Zip": normalize_zip(address_text),
            "Telephone": phone_text,
            "Detail_URL": normalize_url(driver.current_url),
            "Capacity (optional)": capacity_text,
            "Age Range (optional)": "",
        }

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, get_record_name_profile, normalize_phone, normalize_zip, token_overlap_metrics
        try:
            candidates = self.search(enricher, record)
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
        driver = enricher.get_state_portal_driver("OK")
        try:
            detail_values = self.fetch_detail(enricher, driver=driver, candidate_index=best_candidate.get("candidate_index", ""), action_label=f"oklahoma detail page [{record.get('PID', '')}]")
        except Exception:
            LOGGER.exception("Oklahoma detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            enricher.finalize_state_portal_query("OK")
        values = {
            "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")),
            "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "") or detail_values.get("Mailing_Address", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "")),
            "URL": "",
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": "",
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(best_candidate.get("city", "")), candidate_address=values.get("Mailing_Address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=clean_text(detail_values.get("Detail_URL", "")))
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = clean_text(detail_values.get("Detail_URL", "")) or "https://ccl.dhs.ok.gov/providers"
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Oklahoma childcare locator") for field, value in values.items() if clean_text(value)}
        return values, sources

from adapters.base import StateAdapter


class MarylandAdapter(StateAdapter):
    state = "MD"

    def build_city_slug(self, city, clean_text, re_module):
        slug = clean_text(city).lower()
        slug = re_module.sub(r"[^a-z0-9]+", "-", slug)
        return slug.strip("-")

    def search(self, enricher, record):
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile, re

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        city_slug = self.build_city_slug(city, clean_text, re)
        portal_url = f"https://locatesearch.marylandfamilynetwork.org/city/{city_slug}-md"
        try:
            for variant in profile.search_name_variants:
                driver = enricher.open_or_reuse_state_portal_query_tab("MD", portal_url, ready_locator=(By.XPATH, "/html/body/div[8]/div[3]/div[2]/div[1]/div[1]/div[2]/div/div[42]"))
                if city_slug not in clean_text(driver.current_url).lower():
                    driver.get(portal_url)
                    WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[8]/div[3]/div[2]/div[1]/div[1]/div[2]/div/div[42]")))
                try:
                    search_input = driver.find_element(By.XPATH, '//*[@id="searchBiz1"]')
                except Exception:
                    trigger = driver.find_element(By.XPATH, "/html/body/div[8]/div[3]/div[2]/div[1]/div[1]/div[2]/div/div[42]")
                    try:
                        trigger.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", trigger)
                    search_input = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, '//*[@id="searchBiz1"]')))
                search_input.send_keys(Keys.CONTROL, "a")
                search_input.send_keys(Keys.DELETE)
                search_input.send_keys(variant)
                time.sleep(1.0)
                candidate_rows = []
                try:
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[29]/div[2]/div[1]/div/div[2]")))
                    candidate_rows = driver.execute_script(
                        """
const root = document.evaluate('/html/body/div[29]/div[2]/div[1]/div/div[2]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!root) return [];
return Array.from(root.children).map((item, index) => {
  const nameNode = item.querySelector('.bubble-element.Text div') || item.querySelector('.bubble-element.Text');
  return { candidate_index:index, provider_name:(nameNode && (nameNode.innerText || nameNode.textContent || '')) || '' };
}).filter((item) => item.provider_name);
"""
                    )
                except Exception:
                    candidate_rows = []
                if candidate_rows:
                    return [{"variant": variant, **item} for item in candidate_rows]
            return []
        except Exception:
            enricher.reset_state_portal_driver("MD")
            raise

    def fetch_detail(self, enricher, driver, candidate_index, provider_name, action_label):
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, normalize_phone, normalize_url, normalize_zip

        LOGGER.info("Fetching Maryland detail page via Selenium action=%s candidate_index=%s provider=%s", action_label, candidate_index, provider_name)
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
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]")))
        try:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[25]/div")))
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(1.0)
        except Exception:
            pass
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[2]/div")))
        phone_href = ""
        website_href = ""
        address_text = ""
        try:
            phone_href = driver.find_element(By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[3]/div/a").get_attribute("href") or ""
        except Exception:
            pass
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[5]/div/a")))
            website_href = driver.find_element(By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[5]/div/a").get_attribute("href") or ""
        except Exception:
            try:
                website_href = driver.execute_script("const node = document.evaluate('/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[5]/div/a', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue; return node ? (node.href || node.getAttribute('href') || '') : '';") or ""
            except Exception:
                pass
        try:
            address_text = clean_text(driver.find_element(By.XPATH, "/html/body/div[8]/div[2]/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[2]/div").text)
        except Exception:
            pass
        address_segments = [clean_text(part) for part in address_text.split(",") if clean_text(part)]
        street_address = address_segments[0] if address_segments else ""
        zip_source = address_segments[-1].split(" ")[-1] if address_segments and address_segments[-1].split(" ") else ""
        return {"Mailing_Address": street_address, "Mailing_Zip": normalize_zip(zip_source), "Telephone": normalize_phone(phone_href), "URL": normalize_url(website_href), "Capacity (optional)": "", "Age Range (optional)": "", "Detail_URL": normalize_url(driver.current_url)}

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, get_record_name_profile, normalize_phone, normalize_zip, token_overlap_metrics, re
        portal_url = f"https://locatesearch.marylandfamilynetwork.org/city/{self.build_city_slug(clean_text(record.get('Mailing_City')), clean_text, re)}-md"
        try:
            candidates = self.search(enricher, record)
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
            variant_hit = any(clean_text(v) and len(clean_text(v)) >= 4 and clean_text(v).lower() in provider_name.lower() for v in profile.search_name_variants)
            if variant_hit:
                score += 4
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_overlap = recall
        if not best_candidate or (best_score < 6 and best_overlap < 0.35):
            return {}, {}
        driver = enricher.get_state_portal_driver("MD")
        try:
            detail_values = self.fetch_detail(enricher, driver=driver, candidate_index=best_candidate.get("candidate_index", ""), provider_name=best_candidate.get("provider_name", ""), action_label=f"maryland detail page [{record.get('PID', '')}]")
        except Exception:
            LOGGER.exception("Maryland detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            enricher.finalize_state_portal_query("MD")
        values = {"Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")), "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "")), "Telephone": normalize_phone(detail_values.get("Telephone", "")), "URL": clean_text(detail_values.get("URL", "")), "Capacity (optional)": "", "Age Range (optional)": ""}
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(record.get("Mailing_City", "")), candidate_address=values.get("Mailing_Address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=values.get("URL", ""))
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = clean_text(detail_values.get("Detail_URL", "")) or values.get("URL", "") or portal_url
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Maryland Family Network locate search") for field, value in values.items() if clean_text(value)}
        return values, sources

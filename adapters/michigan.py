from adapters.base import StateAdapter


class MichiganAdapter(StateAdapter):
    state = "MI"

    def search(self, enricher, record):
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile, normalize_phone, re

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://greatstarttoquality.org/find-programs/"
        no_results_text = "Your search returned no matches, please check your search criteria and try again."
        try:
            for variant in profile.search_name_variants[:4]:
                driver = enricher.open_state_portal_query_tab("MI", portal_url)
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div/div/div/main/article/div[1]/div/div[3]/div/form/div[3]/div[2]/div/div[2]/div[2]/div/div/input"))
                )
                provider_input = driver.find_element(By.XPATH, "/html/body/div[2]/div/div/div/main/article/div[1]/div/div[3]/div/form/div[3]/div[2]/div/div[2]/div[2]/div/div/input")
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
                    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="submitAgency4ReferralForm"]')))
                    submit_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", submit_button)
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    lambda d: len(set(d.window_handles) - existing_handles) >= 1 or "UpdateReferral" in clean_text(d.current_url)
                )
                new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
                if new_handles:
                    driver.switch_to.window(new_handles[-1])
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    lambda d: d.execute_script(
                        """
const bodyText = ((document.body && (document.body.innerText || document.body.textContent)) || '').trim().toLowerCase();
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
                    enricher.finalize_state_portal_query("MI")
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
  const capacityNode = infoDivs.find(node => ((node.querySelector('b')?.innerText || '').trim().toLowerCase()) === 'capacity');
  const ageNode = infoDivs.find(node => ((node.querySelector('b')?.innerText || '').trim().toLowerCase()) === 'ages served');
  const capacityText = ((capacityNode && (capacityNode.innerText || capacityNode.textContent)) || '').trim();
  const ageText = ageTextAttr || ((ageNode && (ageNode.innerText || ageNode.textContent)) || '').trim();
  results.push({ candidate_index:index, provider_name:name, detail_url:href, address_text:addressText, city_text:cityText, zip_text:zipText, phone:phoneText, capacity:capacityText, age:ageText, row_text:(card.innerText || '').trim() });
});
return results;
"""
                )
                results = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    address_text = clean_text(item.get("address_text", ""))
                    parsed_city = clean_text(item.get("city_text", "")) or city
                    parsed_zip = clean_text(item.get("zip_text", ""))
                    row_text = clean_text(item.get("row_text", ""))
                    if not address_text or any(label in address_text for label in ["Type:", "Capacity:", "Ages Served:", "In Operation:", "Licensing Inspection Report", "Program Quality Guide", "Program Details"]):
                        address_text = row_text
                    if city and parsed_city and city.lower() != parsed_city.lower() and city.lower() not in address_text.lower():
                        continue
                    address_match = re.search(r"(\d{1,6}\s+.+?\b" + re.escape(parsed_city or city) + r"\b\s+MI\s+\d{5}(?:-\d{4})?)", address_text, re.IGNORECASE)
                    if not address_match:
                        address_match = re.search(r"(\d{1,6}\s+.+?\bMI\b\s+\d{5}(?:-\d{4})?)", address_text, re.IGNORECASE)
                    address_block = clean_text(address_match.group(1)) if address_match else ""
                    address_line = clean_text(address_block or address_text)
                    if parsed_city and parsed_city.lower() in address_line.lower():
                        address_line = re.split(r"\b" + re.escape(parsed_city) + r"\b", address_line, maxsplit=1, flags=re.IGNORECASE)[0].strip(" ,")
                    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", parsed_zip or address_block or address_text or row_text)
                    raw_phone_text = clean_text(item.get("phone", "")) or row_text
                    capacity_text = clean_text(item.get("capacity", ""))
                    if not capacity_text:
                        capacity_match = re.search(r"Capacity:\s*([0-9]+)", row_text, re.IGNORECASE)
                        capacity_text = capacity_match.group(1) if capacity_match else ""
                    age_text = clean_text(item.get("age", ""))
                    if not age_text:
                        age_match = re.search(r"Ages Served:\s*(.+?)(?=\s+(?:Monday\s*-\s*Friday|In Operation:|Licensing Inspection Report|Program Quality Guide|Free PreK|Message to Families:|Program Details|$))", row_text, re.IGNORECASE)
                        age_text = clean_text(age_match.group(1)) if age_match else ""
                    results.append({"provider_name":provider_name,"address":address_line,"city":parsed_city or city,"zip":parsed_zip or (zip_match.group(0) if zip_match else ""),"phone":normalize_phone(raw_phone_text),"capacity":re.sub(r"^\s*Capacity\s*:?\s*", "", capacity_text, flags=re.IGNORECASE).strip(),"age":re.sub(r"^\s*Ages?\s*:?\s*", "", age_text, flags=re.IGNORECASE).strip(),"detail_url":"" if clean_text(item.get("detail_url", "")).startswith("javascript:") else clean_text(item.get("detail_url", "")),"row_text":row_text})
                if results:
                    return results
                enricher.finalize_state_portal_query("MI")
            return []
        except Exception:
            enricher.reset_state_portal_driver("MI")
            raise

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, get_record_name_profile, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_zip, token_overlap_metrics
        try:
            candidates = self.search(enricher, record)
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
            variant_hit = any(clean_text(v) and len(clean_text(v)) >= 4 and clean_text(v).lower() in provider_name.lower() for v in profile.search_name_variants[:4])
            if variant_hit:
                score += 4
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_city_match = city_match
                best_overlap = recall
        if not best_candidate or (best_score < 6 and not (best_city_match and best_overlap >= 0.35)):
            enricher.finalize_state_portal_query("MI")
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
            match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(best_candidate.get("city", "")), candidate_address=values.get("Mailing_Address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=values.get("URL", ""))
            values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
            source_url = values.get("URL", "") or "https://greatstarttoquality.org/find-programs/"
            sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Michigan Great Start to Quality") for field, value in values.items() if clean_text(value)}
            return values, sources
        finally:
            enricher.finalize_state_portal_query("MI")

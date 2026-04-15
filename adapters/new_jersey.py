from adapters.base import StateAdapter


class NewJerseyAdapter(StateAdapter):
    state = "NJ"

    def search(self, enricher, record):
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile, normalize_zip

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://childcareexplorer.njccis.com/portal/"
        try:
            for variant in profile.search_name_variants[:4]:
                driver = enricher.open_state_portal_query_tab("NJ", portal_url)
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.NAME, "facilityName")))
                time.sleep(1.0)
                provider_input = driver.find_element(By.NAME, "facilityName")
                city_input = driver.find_element(By.NAME, "city")
                provider_input.clear()
                provider_input.send_keys(variant)
                city_input.clear()
                city_input.send_keys(city)
                driver.find_element(By.ID, "submit").click()
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    lambda d: d.execute_script(
                        """
const bodyText = ((document.body && (document.body.innerText || document.body.textContent)) || '').toLowerCase();
if (bodyText.includes('no records found')) return true;
const grid = document.getElementById('grdUsers');
if (!grid) return false;
const style = window.getComputedStyle(grid);
if (grid.hasAttribute('hidden') || style.display === 'none' || style.visibility === 'hidden') return false;
const rows = grid.querySelectorAll('tbody.ui-datatable-data tr');
return rows.length > 0;
"""
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
  const selectNode = cells[0].querySelector('button.btn-sm, a.btn-sm, button, a, input[type="button"], input[type="submit"]');
  const cellValue = (cellIndex) => {
    const cell = cells[cellIndex];
    if (!cell) return '';
    const dataNode = cell.querySelector('.ui-cell-data');
    return ((dataNode && dataNode.innerText) || cell.innerText || '').trim();
  };
  rows.push({ row_index:rowIndex, has_select:!!selectNode, provider_name:cellValue(1), address:cellValue(2), city:cellValue(3), zip:cellValue(4), county:cellValue(5), text:(row.innerText || '').trim() });
});
return rows;
"""
                )
                results = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name or clean_text(str(item.get("has_select", ""))).lower() not in {"true", "1"}:
                        continue
                    address_line = clean_text(item.get("address", ""))
                    result_city = clean_text(item.get("city", "")) or city
                    zip_code = normalize_zip(item.get("zip", ""))
                    address_value = ", ".join(part for part in [address_line, result_city or city, "NJ", zip_code] if clean_text(part))
                    results.append({"provider_name": provider_name, "address": address_value, "city": result_city or city, "zip": zip_code, "phone": "", "row_index": str(item.get("row_index", ""))})
                if results:
                    return results
                enricher.finalize_state_portal_query("NJ", portal_url)
            return []
        except Exception:
            enricher.reset_state_portal_driver("NJ")
            raise

    def fetch_detail(self, enricher, driver, row_index, action_label):
        import time

        from bs4 import BeautifulSoup
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_url, re

        rows = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(lambda d: d.find_elements(By.CSS_SELECTOR, "#grdUsers tbody.ui-datatable-data tr"))
        index = int(row_index)
        row = rows[index]
        button = row.find_element(By.CSS_SELECTOR, "button.btn-sm, a.btn-sm, button, a, input[type='button'], input[type='submit']")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        time.sleep(0.5)
        try:
            button.click()
        except Exception:
            driver.execute_script("arguments[0].click();", button)
        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(lambda d: "/provider-details/" in clean_text(d.current_url) or d.execute_script("return !!Array.from(document.querySelectorAll('.labelIt')).find(el => ((el.innerText || '').toLowerCase().includes('phone')) && el.querySelector('a[href^=\"tel:\"]')) || !!Array.from(document.querySelectorAll('.panel-footer h3, .panel-footer, h3')).find(el => (el.innerText || '').toLowerCase().includes('ages served'));"))
        time.sleep(1.0)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        provider_panel = soup.select_one("portal-provider-header .panel.panel-info") or soup.select_one("portal-provider-header")
        panel_soup = provider_panel if provider_panel is not None else soup
        labeled_values = {}
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
        phone_block = next((block for block in panel_soup.select(".labelIt") if clean_text((block.find("strong").get_text(" ", strip=True) if block.find("strong") else "")).lower() == "phone"), None)
        detail_phone = ""
        if phone_block is not None:
            phone_link = phone_block.select_one("a[href^='tel:']")
            if phone_link:
                detail_phone = normalize_phone(phone_link.get_text(" ", strip=True) or phone_link.get("href", ""))
        if not detail_phone:
            try:
                phone_node = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "/html/body/app-root/div/main/div/portal-provider-details/div/div[2]/div/p-accordion/div/p-accordiontab[1]/div[2]/div/div/div/portal-provider-header/div/div[2]/div/div/div/div[3]/span/a")))
                detail_phone = normalize_phone(phone_node.text or phone_node.get_attribute("href") or "")
            except Exception:
                detail_phone = ""
        detail_age = ""
        for node in panel_soup.select(".panel-footer h3, .panel-footer, h3"):
            node_text = clean_text(node.get_text(" ", strip=True))
            if node_text and "ages served" in node_text.lower():
                detail_age = re.sub(r"^\s*Ages Served\s*", "", node_text, flags=re.IGNORECASE).strip(" :-")
                if detail_age:
                    break
        if not detail_age:
            try:
                age_node = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "/html/body/app-root/div/main/div/portal-provider-details/div/div[2]/div/p-accordion/div/p-accordiontab[1]/div[2]/div/div/div/portal-provider-header/div/div[3]/h3")))
                detail_age = re.sub(r"^\s*Ages Served\s*", "", clean_text(age_node.text), flags=re.IGNORECASE).strip(" :-")
            except Exception:
                detail_age = ""
        return {"Mailing_Address": "", "Mailing_Zip": "", "Telephone": detail_phone, "Detail_URL": normalize_url(driver.current_url), "Capacity (optional)": clean_text(labeled_values.get("capacity", "")), "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_age or clean_text(labeled_values.get("ages served", "")))}

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, get_record_name_profile, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_zip, token_overlap_metrics
        try:
            candidates = self.search(enricher, record)
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
        driver = enricher.get_state_portal_driver("NJ")
        try:
            detail_values = self.fetch_detail(enricher, driver=driver, row_index=best_candidate.get("row_index", ""), action_label=f"new jersey detail page [{record.get('PID', '')}]")
        except Exception:
            LOGGER.exception("New Jersey detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            enricher.finalize_state_portal_query("NJ", "https://childcareexplorer.njccis.com/portal/")
        values = {"Mailing_Address": clean_text(best_candidate.get("address", "")), "Mailing_Zip": normalize_zip(best_candidate.get("zip", "")), "Telephone": normalize_phone(detail_values.get("Telephone", "")), "URL": "", "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")), "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", ""))}
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(best_candidate.get("city", "")), candidate_address=values.get("Mailing_Address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=clean_text(detail_values.get("Detail_URL", "")))
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = clean_text(detail_values.get("Detail_URL", "")) or "https://childcareexplorer.njccis.com/portal/"
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="New Jersey official childcare portal") for field, value in values.items() if clean_text(value)}
        return values, sources

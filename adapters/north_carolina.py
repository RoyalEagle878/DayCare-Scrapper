from adapters.base import StateAdapter


class NorthCarolinaAdapter(StateAdapter):
    state = "NC"

    def try_select_city(self, driver, city):
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import clean_text

        city = clean_text(city)
        if not city:
            return False
        city_input = driver.find_element(By.XPATH, '//*[@id="dnn_ctr1464_View_cboCity_Input"]')
        city_input.send_keys(Keys.CONTROL, "a")
        city_input.send_keys(Keys.DELETE)
        city_input.send_keys(city)
        time.sleep(1.0)
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
        dropdown_candidates = driver.find_elements(By.XPATH, "//*[contains(@id,'dnn_ctr1464_View_cboCity_DropDown')]//*[self::li or self::div or self::td or self::span]")
        for candidate in dropdown_candidates:
            candidate_text = clean_text(candidate.text)
            if candidate_text.lower() != city.lower():
                continue
            try:
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

    def search(self, enricher, record):
        import time

        from selenium.common.exceptions import TimeoutException
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://ncchildcare.ncdhhs.gov/childcaresearch"
        table_id = "dnn_ctr1464_View_rgSearchResults_ctl00"
        no_results_xpath = "/html/body/form/div[6]/div[3]/div[3]/div/div[1]/div/div[2]/div[2]/div[1]/div[2]/div"
        nc_wait_timeout = max(SELENIUM_WAIT_TIMEOUT, 45)
        try:
            for variant in profile.search_name_variants[:4]:
                driver = enricher.open_state_portal_query_tab("NC", portal_url)
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, '//*[@id="dnn_ctr1464_View_txtFacilityName"]')))
                time.sleep(1.0)
                provider_input = driver.find_element(By.XPATH, '//*[@id="dnn_ctr1464_View_txtFacilityName"]')
                provider_input.clear()
                provider_input.send_keys(variant)
                self.try_select_city(driver, city)
                driver.find_element(By.XPATH, '//*[@id="dnn_ctr1464_View_btnSearch"]').click()
                WebDriverWait(driver, nc_wait_timeout).until(lambda d: d.execute_script("""const table = document.getElementById(arguments[0]); const noResultsNode = document.evaluate(arguments[1], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue; if (noResultsNode) { const msg = (noResultsNode.innerText || '').trim().toLowerCase(); if (msg.includes('the search did not return any results')) return true; } if (!table) return false; const visible = table.offsetParent !== null; const resultLink = table.querySelector('tbody tr td a, tr td a'); return visible && !!resultLink;""", table_id, no_results_xpath))
                no_results_text = ""
                try:
                    no_results_text = clean_text(driver.find_element(By.XPATH, no_results_xpath).text)
                except Exception:
                    pass
                if "the search did not return any results" in no_results_text.lower():
                    enricher.finalize_state_portal_query("NC", portal_url)
                    continue
                time.sleep(2.0)
                candidate_rows = driver.execute_script("""const table = document.getElementById(arguments[0]); if (!table) return []; const rows = []; Array.from(table.querySelectorAll('tbody tr, tr')).forEach((row, rowIndex) => { const cells = Array.from(row.querySelectorAll('td')); const rowText = (row.innerText || '').trim(); if (!rowText || rowText.toLowerCase().includes('no records')) return; const cellTexts = cells.map(cell => ((cell.innerText || '').trim())); const links = Array.from(row.querySelectorAll('a')); const meaningfulLinks = links.map(link => ((link.innerText || '').trim())).filter(text => text && !/^\\d+$/.test(text)); let providerName = ''; if (meaningfulLinks.length) { providerName = meaningfulLinks.sort((a, b) => b.length - a.length)[0]; } else if (cellTexts.length > 1) { providerName = cellTexts[1]; } else if (cellTexts.length) { providerName = cellTexts[0]; } if (!providerName) return; rows.push({ row_index: rows.length, provider_name: providerName, row_text: rowText, cell_texts: cellTexts }); }); return rows;""", table_id)
                results = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if provider_name:
                        row_text = clean_text(item.get("row_text", ""))
                        results.append({"provider_name": provider_name, "row_index": str(item.get("row_index", "")), "row_text": row_text, "city": city if city.lower() in row_text.lower() else "", "cell_texts": " || ".join([clean_text(v) for v in (item.get("cell_texts") or []) if clean_text(v)])})
                if results:
                    return results
                enricher.finalize_state_portal_query("NC", portal_url)
            return []
        except Exception:
            enricher.reset_state_portal_driver("NC")
            raise

    def fetch_detail(self, enricher, driver, row_index, action_label, record):
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_url, normalize_zip, re

        rows = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(lambda d: d.find_elements(By.CSS_SELECTOR, "#dnn_ctr1464_View_rgSearchResults_ctl00 tr"))
        candidate_rows = [row for row in rows if row.find_elements(By.TAG_NAME, "a")]
        row = candidate_rows[int(row_index)]
        detail_link = row.find_element(By.TAG_NAME, "a")
        try:
            detail_link.click()
        except Exception:
            driver.execute_script("arguments[0].click();", detail_link)
        address_xpath = "/html/body/form/div[6]/div[3]/div[3]/div/div[1]/div/div[2]/div[2]/div[3]/div/div[2]/div/div/div[3]/div[2]"
        phone_xpath = "/html/body/form/div[6]/div[3]/div[3]/div/div[1]/div/div[2]/div[2]/div[3]/div/div[2]/div/div/div[7]/div[2]"
        license_tab_xpath = "/html/body/form/div[6]/div[3]/div[3]/div/div[1]/div/div[2]/div[2]/div[3]/div/div[3]"
        age_id = "dnn_ctr1464_View_FacilityDetail_rptLicenseInfo_lblAgeRange_0"
        capacity_id = "dnn_ctr1464_View_FacilityDetail_rptLicenseInfo_lblFirstShiftCapacity_0"
        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, address_xpath)))
        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.XPATH, phone_xpath)))
        time.sleep(1.0)
        address_text = clean_text(driver.find_element(By.XPATH, address_xpath).text)
        phone_text = normalize_phone(driver.find_element(By.XPATH, phone_xpath).text)
        try:
            license_tab = driver.find_element(By.XPATH, license_tab_xpath)
            license_tab.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", driver.find_element(By.XPATH, license_tab_xpath))
            except Exception:
                pass
        age_text = ""
        capacity_text = ""
        try:
            age_node = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, age_id)))
            age_text = clean_text(age_node.text or age_node.get_attribute("innerText") or age_node.get_attribute("textContent") or age_node.get_attribute("innerHTML"))
        except Exception:
            pass
        try:
            capacity_node = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, capacity_id)))
            capacity_text = clean_text(capacity_node.text or capacity_node.get_attribute("innerText") or capacity_node.get_attribute("textContent"))
        except Exception:
            pass
        normalized_age_text = normalize_age_groups_text_to_numeric_range(age_text)
        if not normalized_age_text and age_text:
            fallback_age = clean_text(age_text)
            fallback_age = re.sub(r"\bthrough\b", " - ", fallback_age, flags=re.IGNORECASE)
            fallback_age = re.sub(r"\bto\b", " - ", fallback_age, flags=re.IGNORECASE)
            fallback_age = re.sub(r"\s*-\s*", " - ", fallback_age)
            normalized_age_text = fallback_age
        return {"Mailing_Address": address_text, "Mailing_Zip": normalize_zip(address_text), "Telephone": phone_text, "Detail_URL": normalize_url(driver.current_url), "Capacity (optional)": capacity_text, "Age Range (optional)": normalized_age_text}

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, format_numeric_age_range, get_record_name_profile, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_zip, re, token_overlap_metrics
        try:
            candidates = self.search(enricher, record)
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
        driver = enricher.get_state_portal_driver("NC")
        try:
            detail_values = self.fetch_detail(enricher, driver=driver, row_index=best_candidate.get("row_index", ""), action_label=f"north carolina detail page [{record.get('PID', '')}]", record=record)
        except Exception:
            LOGGER.exception("North Carolina detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            enricher.finalize_state_portal_query("NC", "https://ncchildcare.ncdhhs.gov/childcaresearch")
        age_raw = clean_text(detail_values.get("Age Range (optional)", ""))
        mailing_address = clean_text(detail_values.get("Mailing_Address", ""))
        if "," in mailing_address:
            mailing_address = clean_text(mailing_address.split(",", 1)[0])
        values = {"Mailing_Address": mailing_address, "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "") or detail_values.get("Mailing_Address", "")), "Telephone": normalize_phone(detail_values.get("Telephone", "")), "URL": "", "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")), "Age Range (optional)": (format_numeric_age_range(*[part.strip() for part in age_raw.split("-", 1)], unit="years") if re.fullmatch(r"\s*\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*", age_raw) else normalize_age_groups_text_to_numeric_range(age_raw))}
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(record.get("Mailing_City", "")) if best_city_match else "", candidate_address=values.get("Mailing_Address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=clean_text(detail_values.get("Detail_URL", "")))
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = clean_text(detail_values.get("Detail_URL", "")) or "https://ncchildcare.ncdhhs.gov/childcaresearch"
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="North Carolina official childcare portal") for field, value in values.items() if clean_text(value)}
        return values, sources

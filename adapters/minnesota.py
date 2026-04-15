from adapters.base import StateAdapter


class MinnesotaAdapter(StateAdapter):
    state = "MN"

    def search(self, enricher, record):
        import time
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile, normalize_phone, re

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://www.parentaware.org/search/#/"
        no_results_text = "Showing 0 programs that match your search"
        try:
            for variant in profile.search_name_variants[:4]:
                driver = enricher.open_state_portal_query_tab("MN", portal_url)
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(EC.presence_of_element_located((By.XPATH, '//*[@id="name-type"]')))
                try:
                    by_name_label = driver.find_element(By.XPATH, "//label[@for='name-type']")
                    try:
                        by_name_label.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", by_name_label)
                except Exception:
                    pass
                name_input = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(EC.presence_of_element_located((By.XPATH, '//*[@id="name"]')))
                name_input.send_keys(Keys.CONTROL, "a")
                name_input.send_keys(Keys.DELETE)
                name_input.send_keys(variant)
                search_button = driver.find_element(By.XPATH, "/html/body/main/div/div/div/div[2]/form/button")
                try:
                    search_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", search_button)
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(lambda d: d.execute_script("const bodyText = ((document.body && (document.body.innerText || document.body.textContent)) || '').trim(); if (bodyText.includes(arguments[0])) return true; const container = document.querySelector('.search-results-list-container'); if (!container) return false; return container.querySelectorAll('article.result-item').length > 0;", no_results_text))
                body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text)
                if no_results_text.lower() in body_text.lower():
                    enricher.finalize_state_portal_query("MN")
                    continue
                while True:
                    try:
                        load_more_buttons = driver.find_elements(By.XPATH, "//button[contains(., 'Load More')]")
                        visible_buttons = [button for button in load_more_buttons if button.is_displayed()]
                        if not visible_buttons:
                            break
                        button = visible_buttons[0]
                        existing_count = len(driver.find_elements(By.CSS_SELECTOR, "article.result-item"))
                        try:
                            button.click()
                        except Exception:
                            driver.execute_script("arguments[0].click();", button)
                        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "article.result-item")) > existing_count or not any(btn.is_displayed() for btn in d.find_elements(By.XPATH, "//button[contains(., 'Load More')]")))
                        time.sleep(1.0)
                    except Exception:
                        break
                candidate_rows = driver.execute_script("""const container = document.querySelector('.search-results-list-container'); if (!container) return []; const cards = Array.from(container.querySelectorAll('article.result-item')); return cards.map((card, index) => { const titleNode = card.querySelector('h2.title'); const titleText = ((titleNode && (titleNode.innerText || titleNode.textContent)) || '').trim(); const linkNode = titleNode ? titleNode.closest('a') : card.querySelector('a[href*=\"#/detail/\"]'); const detailHref = ((linkNode && linkNode.getAttribute('href')) || '').trim(); const detailUrl = detailHref ? new URL(detailHref, window.location.href).href : ''; const addressNode = card.querySelector('.address'); const addressText = ((addressNode && (addressNode.innerText || addressNode.textContent)) || '').trim(); return { candidate_index:index, provider_name:titleText, detail_url:detailUrl, address_text:addressText, row_text:(card.innerText || '').trim() }; });""")
                results = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    address_text = clean_text(item.get("address_text", ""))
                    if city and city.lower() not in address_text.lower():
                        continue
                    address_lines = [clean_text(line) for line in address_text.splitlines() if clean_text(line)]
                    street_line = address_lines[0] if address_lines else ""
                    city_state_zip_line = address_lines[1] if len(address_lines) > 1 else ""
                    phone_line = address_lines[2] if len(address_lines) > 2 else ""
                    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", city_state_zip_line)
                    results.append({"candidate_index": str(item.get("candidate_index", "")), "provider_name": provider_name, "address": street_line, "city": city, "zip": zip_match.group(0) if zip_match else "", "phone": normalize_phone(phone_line), "detail_url": clean_text(item.get("detail_url", "")), "row_text": clean_text(item.get("row_text", ""))})
                if results:
                    return results
                enricher.finalize_state_portal_query("MN")
            return []
        except Exception:
            enricher.reset_state_portal_driver("MN")
            raise

    def fetch_detail(self, enricher, driver, detail_url, action_label):
        import time
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from enrich_daycare_data import SELENIUM_WAIT_TIMEOUT, clean_text, normalize_phone, re

        existing_handles = set(driver.window_handles)
        driver.execute_script("window.open(arguments[0], '_blank');", detail_url)
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
        new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
        if new_handles:
            driver.switch_to.window(new_handles[-1])
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(lambda d: d.find_elements(By.XPATH, "/html/body/main/div/div/div/div[2]/div[2]/div[1]/div") or d.find_elements(By.XPATH, "/html/body/main/div/div/div/div[2]/div[3]/dl[1]/dd[1]"))
        time.sleep(1.0)
        detail_values = driver.execute_script("""const addressNode = document.evaluate('/html/body/main/div/div/div/div[2]/div[2]/div[1]/div', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue; const phoneAnchor = document.querySelector('a[href^=\"tel:\"]'); const websiteNode = document.evaluate('/html/body/main/div/div/div/div[2]/div[1]/div[1]/div[3]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue; const capacityNode = document.evaluate('/html/body/main/div/div/div/div[2]/div[3]/dl[1]/dd[1]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue; const ageNode = document.evaluate('/html/body/main/div/div/div/div[2]/div[3]/dl[1]/dd[2]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue; const addressLines = (() => { if (!addressNode) return []; const lines = []; let current = ''; for (const node of addressNode.childNodes) { if (node.nodeType === Node.TEXT_NODE) { current += node.textContent || ''; continue; } if (node.nodeType === Node.ELEMENT_NODE && node.tagName === 'BR') { if ((current || '').trim()) lines.push(current.trim()); current = ''; continue; } current += node.textContent || ''; } if ((current || '').trim()) lines.push(current.trim()); return lines.filter(Boolean); })(); return { address_lines: addressLines, address_text: addressNode ? (addressNode.innerText || addressNode.textContent || '') : '', phone_text: phoneAnchor ? (phoneAnchor.innerText || phoneAnchor.textContent || '') : '', website_text: websiteNode ? (websiteNode.innerText || websiteNode.textContent || '') : '', website_href: (() => { if (!websiteNode) return ''; const anchor = websiteNode.querySelector('a'); return anchor ? (anchor.href || anchor.getAttribute('href') || '') : ''; })(), capacity_text: capacityNode ? (capacityNode.innerText || capacityNode.textContent || '') : '', age_text: ageNode ? (ageNode.innerText || ageNode.textContent || '') : '' };""") or {}
        address_lines = [clean_text(line) for line in (detail_values.get("address_lines") or []) if clean_text(str(line))]
        if not address_lines:
            address_lines = [clean_text(line) for line in clean_text(detail_values.get("address_text", "")).splitlines() if clean_text(line)]
        street_line = address_lines[0] if address_lines else ""
        city_state_zip_line = address_lines[1] if len(address_lines) > 1 else ""
        city_state_zip_tokens = city_state_zip_line.split()
        zip_candidate = city_state_zip_tokens[-1] if city_state_zip_tokens else ""
        zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", zip_candidate) or re.search(r"\b\d{5}(?:-\d{4})?\b", city_state_zip_line)
        values = {"Mailing_Address": street_line, "Mailing_Zip": zip_match.group(0) if zip_match else "", "Telephone": normalize_phone(detail_values.get("phone_text", "")), "URL": clean_text(detail_values.get("website_href", "") or detail_values.get("website_text", "")), "Capacity (optional)": clean_text(clean_text(detail_values.get("capacity_text", "")).split(" ", 1)[0]), "Age Range (optional)": clean_text(detail_values.get("age_text", ""))}
        try:
            driver.close()
        except Exception:
            pass
        if driver.window_handles:
            driver.switch_to.window(driver.window_handles[0])
        return values

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, get_record_name_profile, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_zip, token_overlap_metrics

        portal_url = "https://www.parentaware.org/search/#/"
        try:
            candidates = self.search(enricher, record)
        except Exception:
            LOGGER.exception("Minnesota portal search failed for PID=%s", record.get("PID", ""))
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
            enricher.finalize_state_portal_query("MN")
            return {}, {}
        driver = enricher.get_state_portal_driver("MN")
        try:
            detail_values = self.fetch_detail(enricher, driver=driver, detail_url=best_candidate.get("detail_url", ""), action_label=f"minnesota detail page [{record.get('PID', '')}]")
        except Exception:
            LOGGER.exception("Minnesota detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            enricher.finalize_state_portal_query("MN")
        values = {"Mailing_Address": clean_text(detail_values.get("Mailing_Address", "") or best_candidate.get("address", "")), "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "") or best_candidate.get("zip", "")), "Telephone": normalize_phone(detail_values.get("Telephone", "") or best_candidate.get("phone", "")), "URL": clean_text(detail_values.get("URL", "")), "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")), "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", ""))}
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(best_candidate.get("city", "")), candidate_address=values.get("Mailing_Address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=values.get("URL", ""))
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = values.get("URL", "") or best_candidate.get("detail_url", "") or portal_url
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Minnesota Parent Aware") for field, value in values.items() if clean_text(value)}
        return values, sources

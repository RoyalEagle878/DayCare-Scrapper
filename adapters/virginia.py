from adapters.base import StateAdapter


class VirginiaAdapter(StateAdapter):
    state = "VA"

    def search(self, enricher, record):
        import time
        from urllib.parse import urljoin
        from bs4 import BeautifulSoup
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from enrich_daycare_data import SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile, normalize_phone, normalize_zip, re

        profile = get_record_name_profile(record)
        home_url = "https://www.dss.virginia.gov/facility/search/cc2.cgi?rm=Search"
        try:
            driver = enricher.open_or_reuse_state_portal_query_tab("VA", home_url, ready_locator=(By.NAME, "search_keywords_name"))
            for variant in profile.search_name_variants[:4]:
                submitted = False
                for attempt in range(2):
                    try:
                        WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.NAME, "search_keywords_name")))
                        name_input = driver.find_element(By.NAME, "search_keywords_name")
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", name_input)
                        name_input.clear()
                        name_input.send_keys(Keys.CONTROL, "a")
                        name_input.send_keys(Keys.DELETE)
                        name_input.send_keys(variant)
                        try:
                            name_input.send_keys(Keys.ENTER)
                        except Exception:
                            driver.execute_script("arguments[0].form && arguments[0].form.submit();", name_input)
                        submitted = True
                        break
                    except Exception:
                        if attempt == 0:
                            driver.get(home_url)
                            continue
                        raise
                if not submitted:
                    continue
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(1.0)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                results = []
                seen_urls = set()
                for link in soup.select("a[href*='cc2.cgi?rm=Details;ID=']"):
                    href = clean_text(link.get("href", ""))
                    href = urljoin(home_url, href)
                    name = clean_text(link.get_text(" ", strip=True))
                    if not href or href in seen_urls:
                        continue
                    if not name:
                        parent_text = clean_text(link.parent.get_text(" ", strip=True) if link.parent else "")
                        name = parent_text.split("  ")[0].strip() if parent_text else ""
                    if not name:
                        continue
                    container = link.find_parent("tr") or link.find_parent("td") or link.parent
                    container_text = clean_text(container.get_text("\n", strip=True) if container else "")
                    container_lines = [clean_text(item) for item in (container.stripped_strings if container else []) if clean_text(item)]
                    address_line = ""
                    city = ""
                    zip_code = ""
                    for line in container_lines:
                        if line == name:
                            continue
                        if not address_line and re.search(r"\b\d{1,6}\s+[A-Za-z0-9#.\- ]+\b", line):
                            address_line = line
                        city_state_zip_match = re.search(r"\b([A-Z][A-Z .'-]+),\s*VA\s+(\d{5}(?:-\d{4})?)\b", line, re.IGNORECASE)
                        if city_state_zip_match:
                            city = clean_text(city_state_zip_match.group(1))
                            zip_code = normalize_zip(city_state_zip_match.group(2))
                    address_value = ", ".join(part for part in [address_line, city, "VA", zip_code] if clean_text(part))
                    seen_urls.add(href)
                    results.append({"provider_name": name, "detail_url": href, "address": address_value, "city": city, "zip": zip_code, "phone": normalize_phone(container_text)})
                if results:
                    return results
                enricher.finalize_state_portal_query("VA", home_url)
            return []
        except Exception:
            enricher.reset_state_portal_driver("VA")
            raise

    def fetch_detail(self, enricher, detail_url, action_label, driver=None):
        import time
        from bs4 import BeautifulSoup
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from enrich_daycare_data import SELENIUM_WAIT_TIMEOUT, clean_text, normalize_phone, re

        driver = driver or enricher.get_state_portal_driver("VA")
        try:
            driver.get(detail_url)
            WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1.0)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            text = soup.get_text("\n", strip=True)
        except Exception:
            enricher.reset_state_portal_driver("VA")
            raise
        labeled_values = {}
        for row in soup.select("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = clean_text(cells[0].get_text(" ", strip=True)).rstrip(":")
            value = clean_text(" ".join(cell.get_text(" ", strip=True) for cell in cells[1:]))
            if label and value:
                labeled_values[label.lower()] = value
        for dt in soup.select("dt"):
            label = clean_text(dt.get_text(" ", strip=True)).rstrip(":").lower()
            dd = dt.find_next_sibling("dd")
            value = clean_text(dd.get_text(" ", strip=True) if dd else "")
            if label and value:
                labeled_values[label] = value
        def extract_labeled_value(label):
            direct = labeled_values.get(label.lower(), "")
            if direct:
                return direct
            for key, value in labeled_values.items():
                if label.lower() in key:
                    return value
            for pattern in [re.compile(rf"{re.escape(label)}\s*[:\t]\s*(.+)", re.IGNORECASE), re.compile(rf"{re.escape(label)}\s*\n\s*(.+)", re.IGNORECASE)]:
                match = pattern.search(text)
                if match:
                    return clean_text(match.group(1))
            return ""
        def extract_structured_address():
            for label in ("Address", "Facility Address", "Street Address", "Location", "Physical Address"):
                value = extract_labeled_value(label)
                if value:
                    return value
            lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]
            for index, line in enumerate(lines):
                if re.search(r"\b\d{1,6}\s+[A-Za-z0-9#.\-]+\b", line):
                    if index + 1 < len(lines) and re.search(r"\b[A-Z][A-Za-z.\- ]+,\s*[A-Z]{2}\s+\d{5}", lines[index + 1]):
                        return f"{line}, {lines[index + 1]}"
                    return line
            return enricher.extract_address_from_text(text, {})
        def extract_structured_phone():
            for label in ("Phone", "Telephone", "Phone Number", "Business Phone"):
                value = normalize_phone(extract_labeled_value(label))
                if value:
                    return value
            for key, value in labeled_values.items():
                if "phone" in key or "telephone" in key:
                    normalized = normalize_phone(value)
                    if normalized:
                        return normalized
            return normalize_phone(enricher.extract_phone_from_text(text))
        address_value = extract_structured_address()
        return {"Mailing_Address": address_value, "Mailing_Zip": enricher.extract_zip_from_text(address_value or text, {}), "Telephone": extract_structured_phone(), "URL": "", "Capacity (optional)": extract_labeled_value("Capacity"), "Age Range (optional)": extract_labeled_value("Ages"), "Business_Hours": extract_labeled_value("Business Hours"), "Facility_Type": extract_labeled_value("Facility Type"), "License_Type": extract_labeled_value("License Type"), "Administrator": extract_labeled_value("Administrator"), "Inspector": extract_labeled_value("Inspector"), "Facility_ID": extract_labeled_value("License/Facility ID#")}

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, get_record_name_profile, looks_like_street_address, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_zip, token_overlap_score

        try:
            candidates = self.search(enricher, record)
        except Exception:
            LOGGER.exception("Virginia portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        profile = get_record_name_profile(record)
        best_candidate = None
        best_score = -999
        best_variant_hit = False
        for candidate in candidates:
            provider_name = clean_text(candidate.get("provider_name", ""))
            score = token_overlap_score(record.get("Daycare_Name", ""), provider_name) * 4
            variant_hit = any(clean_text(v) and len(clean_text(v)) >= 4 and clean_text(v).lower() in provider_name.lower() for v in profile.search_name_variants[:4])
            if variant_hit:
                score += 4
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_variant_hit = variant_hit
        if not best_candidate or (best_score < 6 and not (best_variant_hit and len(candidates) == 1)):
            return {}, {}
        detail_url = best_candidate.get("detail_url", "")
        shared_driver = enricher.get_state_portal_driver("VA")
        try:
            detail_values = self.fetch_detail(enricher, detail_url=detail_url, action_label=f"virginia detail page [{record.get('PID', '')}]", driver=shared_driver)
        except Exception:
            LOGGER.exception("Virginia detail page fetch failed for PID=%s url=%s", record.get("PID", ""), detail_url)
            detail_values = {}
        finally:
            enricher.finalize_state_portal_query("VA", "https://www.dss.virginia.gov/facility/search/cc2.cgi?rm=Search")
        detail_address = clean_text(detail_values.get("Mailing_Address", ""))
        candidate_address = clean_text(best_candidate.get("address", ""))
        selected_address = detail_address if looks_like_street_address(detail_address) else candidate_address
        if "," in selected_address:
            selected_address = clean_text(selected_address.split(",", 1)[0])
        detail_zip = normalize_zip(detail_values.get("Mailing_Zip", ""))
        candidate_zip = normalize_zip(best_candidate.get("zip", ""))
        selected_zip = detail_zip if detail_zip else candidate_zip
        values = {"Mailing_Address": selected_address, "Mailing_Zip": selected_zip, "Telephone": normalize_phone(detail_values.get("Telephone", "")) or normalize_phone(best_candidate.get("phone", "")) or normalize_phone(detail_values.get("Inspector", "")), "URL": "", "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")), "Age Range (optional)": normalize_age_groups_text_to_numeric_range(detail_values.get("Age Range (optional)", ""))}
        matched_provider_name = best_candidate.get("provider_name", "")
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(record.get("Mailing_City")), candidate_address=values.get("Mailing_Address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=detail_url)
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = clean_text(detail_values.get("Detail_URL", "")) or detail_url or "https://www.dss.virginia.gov/facility/search/cc2.cgi?rm=Search"
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Virginia DSS facility search") for field, value in values.items() if clean_text(value)}
        return values, sources

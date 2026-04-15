from adapters.base import StateAdapter


class IllinoisAdapter(StateAdapter):
    state = "IL"

    def search(self, enricher, record):
        import time

        from bs4 import BeautifulSoup
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        home_url = "https://sunshine.dcfs.illinois.gov/Content/Licensing/Daycare/ProviderLookup.aspx"
        try:
            for variant in profile.search_name_variants:
                driver = enricher.open_state_portal_query_tab("IL", home_url)
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolderContent_ASPxProviderName_I"))
                )
                provider_input = driver.find_element(By.ID, "ctl00_ContentPlaceHolderContent_ASPxProviderName_I")
                city_input = driver.find_element(By.ID, "ctl00_ContentPlaceHolderContent_ASPxCity_I")
                provider_input.clear()
                provider_input.send_keys(variant)
                city_input.clear()
                city_input.send_keys(city)
                LOGGER.info(
                    "Illinois portal searching PID=%s with provider_variant=%s city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                )
                driver.execute_script(
                    """
                    if (typeof dcfssearch === 'function') {
                        dcfssearch();
                    } else {
                        const btn = document.getElementById('ctl00_ContentPlaceHolderContent_ASPxSearch_I');
                        if (btn) { btn.click(); }
                    }
                    """
                )
                WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolderContent_ASPxGridView1"))
                )
                time.sleep(1.0)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.select_one("#ctl00_ContentPlaceHolderContent_ASPxGridView1_DXMainTable")
                if not table:
                    enricher.finalize_state_portal_query("IL", home_url)
                    continue
                rows = table.select("tr.dxgvDataRow, tr.dxgvDataRowAlt")
                if not rows:
                    rows = [row for row in table.select("tr") if row.select("td.dxgv, td.dxgvFixedColumn")]
                results = []
                for row in rows:
                    cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.select("td.dxgv, td.dxgvFixedColumn")]
                    if len(cells) < 12:
                        continue
                    if cells[0] in {"Doing Business as", "Street", "City", "County", "Zip", "Phone"}:
                        continue
                    results.append(
                        {
                            "provider_name": cells[0],
                            "address": cells[1],
                            "city": cells[2],
                            "zip": cells[4],
                            "phone": cells[5],
                            "age_range": cells[8],
                            "capacity": cells[10],
                        }
                    )
                if results:
                    return results
                enricher.finalize_state_portal_query("IL", home_url)
            return []
        except Exception:
            enricher.reset_state_portal_driver("IL")
            raise

    def run(self, enricher, record):
        from enrich_daycare_data import (
            LOGGER,
            build_source_entry,
            classify_match_status,
            clean_text,
            normalize_age_groups_text_to_numeric_range,
            normalize_phone,
            normalize_zip,
            token_overlap_score,
        )

        try:
            candidates = self.search(enricher, record)
        except Exception:
            LOGGER.exception("Illinois portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        best_candidate = None
        best_score = -999
        for candidate in candidates:
            overlap = token_overlap_score(record.get("Daycare_Name", ""), candidate.get("provider_name", ""))
            city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(candidate.get("city")).lower()
            score = overlap * 4
            if city_match:
                score += 3
            if candidate.get("capacity"):
                score += 1
            if score > best_score:
                best_score = score
                best_candidate = candidate
        if not best_candidate:
            return {}, {}
        best_city_match = clean_text(record.get("Mailing_City")).lower() == clean_text(best_candidate.get("city")).lower()
        best_overlap = token_overlap_score(record.get("Daycare_Name", ""), best_candidate.get("provider_name", ""))
        if best_score < 6 and not (best_city_match and best_overlap >= 0.35):
            return {}, {}
        address_value = ", ".join(part for part in [best_candidate.get("address", ""), best_candidate.get("city", ""), "IL", best_candidate.get("zip", "")] if clean_text(part))
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(best_candidate.get("zip", "")),
            "Telephone": normalize_phone(best_candidate.get("phone", "")),
            "URL": "",
            "Capacity (optional)": best_candidate.get("capacity", ""),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(best_candidate.get("age_range", "")),
        }
        matched_provider_name = best_candidate.get("provider_name", "")
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=best_candidate.get("city", ""),
            candidate_address=address_value,
            candidate_phone=values["Telephone"],
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
        source_url = "https://sunshine.dcfs.illinois.gov/Content/Licensing/Daycare/ProviderLookup.aspx"
        sources = {
            field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Illinois official childcare portal")
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

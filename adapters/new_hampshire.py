from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from adapters.base import StateAdapter


class NewHampshireAdapter(StateAdapter):
    state = "NH"

    def search(self, enricher, record):
        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile
        import time

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch"
        no_results_text = "We're sorry we could not find any results based on this criteria. Please consider refining your search criteria and try again"
        try:
            for variant in profile.search_name_variants:
                driver = enricher.open_or_reuse_state_portal_query_tab(
                    "NH",
                    portal_url,
                    ready_locator=(By.XPATH, '//*[@id="j_id0:j_id3:j_id96:accountName"]'),
                )
                name_input = WebDriverWait(driver, SELENIUM_WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="j_id0:j_id3:j_id96:accountName"]'))
                )
                city_select = driver.find_element(By.XPATH, '//*[@id="j_id0:j_id3:j_id96:city"]')
                name_input.send_keys(Keys.CONTROL, "a")
                name_input.send_keys(Keys.DELETE)
                name_input.send_keys(variant)

                selected_city = ""
                try:
                    options = city_select.find_elements(By.TAG_NAME, "option")
                    for option in options:
                        option_text = clean_text(option.text)
                        if option_text and option_text.lower() == city.lower():
                            option.click()
                            selected_city = option_text
                            break
                    if not selected_city:
                        for option in options:
                            if clean_text(option.get_attribute("value")) == "":
                                option.click()
                                break
                except Exception:
                    selected_city = ""

                LOGGER.info(
                    "New Hampshire portal searching PID=%s with provider_variant=%s city=%s selected_city=%s",
                    record.get("PID", ""),
                    variant,
                    city,
                    selected_city,
                )
                search_button = driver.find_element(
                    By.XPATH,
                    '//*[@id="j_id0:j_id3:j_id96"]/div[2]/section/div/div[2]/div/div/div[1]/div[5]/button[1]',
                )
                try:
                    search_button.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", search_button)
                time.sleep(1.0)
                no_result = no_results_text.lower() in clean_text(driver.find_element(By.TAG_NAME, "body").text).lower()
                if no_result:
                    enricher.finalize_state_portal_query("NH")
                    continue
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="accountTable"]'))
                )
                candidate_rows = driver.execute_script(
                    """
const rows = [];
document.querySelectorAll('#dtbody article > div > div').forEach((card, index) => {
  const link = card.querySelector('div p a');
  if (!link) return;
  rows.push({
    candidate_index: index,
    provider_name: (link.innerText || link.textContent || '').trim(),
    detail_url: link.href || link.getAttribute('href') || '',
    row_text: (card.innerText || '').trim()
  });
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
                            "candidate_index": str(item.get("candidate_index", "")),
                            "provider_name": provider_name,
                            "detail_url": clean_text(item.get("detail_url", "")),
                            "row_text": clean_text(item.get("row_text", "")),
                        }
                    )
                if results:
                    LOGGER.info(
                        "New Hampshire portal returned %s candidate rows for PID=%s using provider_variant=%s",
                        len(results),
                        record.get("PID", ""),
                        variant,
                    )
                    return results
                enricher.finalize_state_portal_query("NH")
            LOGGER.info("New Hampshire portal returned 0 candidate rows for PID=%s", record.get("PID", ""))
            return []
        except Exception:
            enricher.reset_state_portal_driver("NH")
            raise

    def fetch_detail(self, enricher, driver, detail_url, action_label):
        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, normalize_phone, normalize_url, normalize_zip

        LOGGER.info("Fetching New Hampshire detail page via Selenium action=%s url=%s", action_label, detail_url)
        existing_handles = set(driver.window_handles)
        driver.execute_script("window.open(arguments[0], '_blank');", detail_url)
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > len(existing_handles))
        new_handles = [handle for handle in driver.window_handles if handle not in existing_handles]
        if new_handles:
            driver.switch_to.window(new_handles[-1])
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="j_id0:j_id7:j_id98"]/div[1]/div/div/div/div[1]/div[1]/div/div/div[1]/div/p'))
        )
        address_lines = driver.execute_script(
            """
const node = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!node) return [];
const lines = [];
let current = '';
Array.from(node.childNodes).forEach((child) => {
  if (child.nodeName && child.nodeName.toLowerCase() === 'br') {
    if (current.trim()) lines.push(current.trim());
    current = '';
    return;
  }
  current += child.textContent || '';
});
if (current.trim()) lines.push(current.trim());
return lines;
""",
            '//*[@id="j_id0:j_id7:j_id98"]/div[1]/div/div/div/div[1]/div[1]/div/div/div[1]/div/p',
        ) or []
        website_href = ""
        try:
            website_href = driver.find_element(By.XPATH, '//*[@id="j_id0:j_id7:j_id98:j_id120"]/a').get_attribute("href") or ""
        except Exception:
            pass
        phone_text = clean_text(driver.find_element(By.XPATH, '//*[@id="j_id0:j_id7:j_id98"]/div[1]/div/div/div/div[1]/div[2]/div[1]/span[2]').text)
        capacity_text = ""
        try:
            capacity_text = clean_text(driver.find_element(By.XPATH, '//*[@id="j_id0:j_id7:j_id98"]/div[2]/div[1]/div[3]/div[1]/div/div[6]').text)
        except Exception:
            pass
        lines = [clean_text(str(line)) for line in address_lines if clean_text(str(line))]
        first_line = lines[0] if lines else ""
        second_line = lines[1] if len(lines) > 1 else ""
        values = {
            "Mailing_Address": first_line.replace(",", "").strip(),
            "Mailing_Zip": normalize_zip(second_line.split(" ")[-1] if second_line.split(" ") else ""),
            "Telephone": normalize_phone(phone_text),
            "URL": normalize_url(website_href),
            "Capacity (optional)": capacity_text.lstrip("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ :"),
            "Age Range (optional)": "",
            "Detail_URL": normalize_url(driver.current_url),
        }
        try:
            driver.close()
        except Exception:
            pass
        remaining_handles = driver.window_handles
        if remaining_handles:
            driver.switch_to.window(remaining_handles[0])
        LOGGER.info("New Hampshire detail page parsed url=%s values=%s", detail_url, values)
        return values

    def run(self, enricher, record):
        from enrich_daycare_data import (
            LOGGER,
            OUTPUT_HEADERS,
            build_source_entry,
            classify_match_status,
            clean_text,
            get_record_name_profile,
            normalize_phone,
            normalize_zip,
            token_overlap_metrics,
        )

        portal_url = "https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch"
        try:
            candidates = self.search(enricher, record)
        except Exception:
            LOGGER.exception("New Hampshire portal search failed for PID=%s", record.get("PID", ""))
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
            variant_hit = any(clean_text(variant) and len(clean_text(variant)) >= 4 and clean_text(variant).lower() in provider_name.lower() for variant in profile.search_name_variants)
            if variant_hit:
                score += 4
            LOGGER.info("New Hampshire portal candidate scored %s for PID=%s provider=%s overlap=%.3f", score, record.get("PID", ""), provider_name, recall)
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_overlap = recall
        if not best_candidate or (best_score < 6 and best_overlap < 0.35):
            return {}, {}
        driver = enricher.get_state_portal_driver("NH")
        try:
            detail_values = self.fetch_detail(
                enricher,
                driver=driver,
                detail_url=best_candidate.get("detail_url", ""),
                action_label=f"new hampshire detail page [{record.get('PID', '')}]",
            )
        except Exception:
            LOGGER.exception("New Hampshire detail page fetch failed for PID=%s", record.get("PID", ""))
            detail_values = {}
        finally:
            enricher.finalize_state_portal_query("NH")
        values = {
            "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "")),
            "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "")),
            "Telephone": normalize_phone(detail_values.get("Telephone", "")),
            "URL": clean_text(detail_values.get("URL", "")),
            "Capacity (optional)": clean_text(detail_values.get("Capacity (optional)", "")),
            "Age Range (optional)": "",
        }
        matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(record.get("Mailing_City", "")),
            candidate_address=values.get("Mailing_Address", ""),
            candidate_phone=values.get("Telephone", ""),
            candidate_url=values.get("URL", ""),
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = clean_text(detail_values.get("Detail_URL", "")) or values.get("URL", "") or portal_url
        sources = {
            field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal")
            for field, value in values.items()
            if field in OUTPUT_HEADERS and clean_text(value)
        }
        return values, sources

from adapters.base import StateAdapter


class ArizonaAdapter(StateAdapter):
    state = "AZ"

    def search(self, enricher, record):
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, get_record_name_profile, normalize_phone, re

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        portal_url = "https://azchildcaresearch.azdes.gov/s/providersearch?language=en_US"
        toast_xpath = "/html/body/div[4]/div[1]/div/div"
        results_xpath = "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/section/div[1]/lightning-layout"
        try:
            for variant in profile.search_name_variants[:4]:
                driver = enricher.open_state_portal_query_tab("AZ", portal_url)
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/c-pvm-header-component-application/header/div[1]/nav/div/div[3]/input"))
                )
                city_input = driver.find_element(By.XPATH, "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/c-pvm-header-component-application/header/div[1]/nav/div/div[1]/div/input")
                provider_input = driver.find_element(By.XPATH, "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/c-pvm-header-component-application/header/div[1]/nav/div/div[3]/input")
                city_input.send_keys(Keys.CONTROL, "a")
                city_input.send_keys(Keys.DELETE)
                if city:
                    city_input.send_keys(city)
                provider_input.send_keys(Keys.CONTROL, "a")
                provider_input.send_keys(Keys.DELETE)
                provider_input.send_keys(variant)
                baseline_signature = driver.execute_script(
                    """
const root = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!root) return '';
const names = Array.from(root.querySelectorAll('lightning-layout-item a')).map(node => ((node.innerText || node.textContent || '').trim())).filter(Boolean).slice(0, 10);
return `${names.length}::${names.join('|')}`;
""",
                    results_xpath,
                )
                driver.find_element(By.XPATH, "/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/c-pvm-header-component-application/header/div[1]/nav/div/button").click()
                WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 30)).until(
                    lambda d: d.execute_script(
                        """
const toast = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (toast && (toast.innerText || '').trim()) return true;
const results = document.evaluate(arguments[1], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!results) return false;
const names = Array.from(results.querySelectorAll('lightning-layout-item a')).map(node => ((node.innerText || node.textContent || '').trim())).filter(Boolean).slice(0, 10);
const signature = `${names.length}::${names.join('|')}`;
return !!names.length && signature !== arguments[2];
""",
                        toast_xpath,
                        results_xpath,
                        baseline_signature,
                    )
                )
                toast_text = ""
                try:
                    toast_text = clean_text(driver.find_element(By.XPATH, toast_xpath).text)
                except Exception:
                    pass
                if toast_text and "no data" in toast_text.lower():
                    enricher.finalize_state_portal_query("AZ")
                    continue
                time.sleep(2.0)
                candidate_rows = driver.execute_script(
                    """
const root = document.evaluate(arguments[0], document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
if (!root) return [];
const cards = Array.from(root.querySelectorAll('lightning-layout-item'));
const results = [];
cards.forEach((card, index) => {
  const nameLink = card.querySelector('div a');
  const addressLink = card.querySelector('a.multiline.provider-address-link');
  const addressLine1Node = addressLink ? addressLink.querySelector('.addr-line1') : null;
  const addressLine2Node = addressLink ? addressLink.querySelector('.addr-line2') : null;
  const phoneNode = card.querySelector('div div:nth-of-type(2) div:nth-of-type(1) p:nth-of-type(2) a');
  const cardText = (card.innerText || '').trim();
  const providerName = ((nameLink && nameLink.innerText) || '').trim();
  if (!providerName) return;
  const href = ((nameLink && nameLink.href) || '').trim();
  let capacity = '';
  const paragraphs = Array.from(card.querySelectorAll('p'));
  for (const paragraph of paragraphs) {
    const paragraphText = ((paragraph.innerText || '') + ' ' + (paragraph.textContent || '')).trim();
    if (!/capacity/i.test(paragraphText)) continue;
    const spans = Array.from(paragraph.querySelectorAll('span')).map(span => ((span.innerText || span.textContent || '').trim())).filter(Boolean);
    if (spans.length >= 2) {
      capacity = spans[spans.length - 1];
      break;
    }
    const match = paragraphText.match(/capacity\s*:?\s*([0-9]+(?:\.[0-9]+)?)/i);
    if (match) {
      capacity = match[1];
      break;
    }
  }
  results.push({
    candidate_index: index,
    provider_name: providerName,
    address: ((addressLine1Node && addressLine1Node.innerText) || '').trim(),
    address_line2: ((addressLine2Node && addressLine2Node.innerText) || '').trim(),
    city: arguments[1],
    phone: ((phoneNode && phoneNode.innerText) || '').trim(),
    capacity: capacity,
    detail_url: href,
    row_text: cardText
  });
});
return results;
""",
                    results_xpath,
                    city,
                )
                results = []
                for item in candidate_rows or []:
                    provider_name = clean_text(item.get("provider_name", ""))
                    if not provider_name:
                        continue
                    address_line2_value = clean_text(item.get("address_line2", ""))
                    zip_source = address_line2_value.split(",")[-1] if "," in address_line2_value else address_line2_value
                    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", zip_source or "")
                    if not zip_match:
                        zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", clean_text(item.get("row_text", "")) or "")
                    capacity_match = re.search(r"\b\d+(?:\.\d+)?\b", clean_text(item.get("capacity", "")))
                    results.append(
                        {
                            "candidate_index": str(item.get("candidate_index", "")),
                            "provider_name": provider_name,
                            "address": clean_text(item.get("address", "")),
                            "city": city,
                            "zip": zip_match.group(0) if zip_match else "",
                            "phone": normalize_phone(item.get("phone", "")),
                            "capacity": capacity_match.group(0) if capacity_match else "",
                            "detail_url": clean_text(item.get("detail_url", "")),
                            "row_text": clean_text(item.get("row_text", "")),
                        }
                    )
                if results:
                    return results
                enricher.finalize_state_portal_query("AZ")
            return []
        except Exception:
            enricher.reset_state_portal_driver("AZ")
            raise

    def fetch_detail(self, enricher, driver, candidate_index, action_label):
        import time

        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        from enrich_daycare_data import LOGGER, SELENIUM_WAIT_TIMEOUT, clean_text, normalize_zip

        index_value = int(clean_text(candidate_index))
        LOGGER.info("Fetching Arizona detail popup via Selenium action=%s candidate_index=%s", action_label, index_value)
        popup_trigger_xpath = f"/html/body/div[3]/div[2]/div/div/div/div[1]/c-pvm-hero-section-provider-search-new-view/section/div[1]/lightning-layout/slot/lightning-layout-item/slot/lightning-layout/slot/lightning-layout-item[{index_value + 1}]/slot/div/div[1]"
        popup_container_xpath = "//*[contains(@class,'popup-container') and contains(@class,'slide-in')]"
        contact_info_xpath = "//*[contains(@class,'popup-container') and contains(@class,'slide-in')]//*[contains(@class,'contact-info')]"
        trigger = WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(EC.presence_of_element_located((By.XPATH, popup_trigger_xpath)))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", trigger)
        time.sleep(0.5)
        try:
            trigger.click()
        except Exception:
            driver.execute_script("arguments[0].click();", trigger)
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(EC.presence_of_element_located((By.XPATH, popup_container_xpath)))
        WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(EC.presence_of_element_located((By.XPATH, contact_info_xpath)))
        time.sleep(1.0)
        contact_details = driver.execute_script(
            """
const popup = document.querySelector('.popup-container.slide-in');
if (!popup) return {};
const contactInfo = popup.querySelector('.contact-info');
if (!contactInfo) return {};
const addressLink = contactInfo.querySelector('p a');
const addressText = ((addressLink && (addressLink.textContent || addressLink.innerText)) || '').trim();
return { address_text: addressText };
"""
        ) or {}
        address_text = clean_text(contact_details.get("address_text", ""))
        address_parts = [clean_text(part) for part in address_text.split(",") if clean_text(part)]
        return {
            "Mailing_Address": address_parts[0] if address_parts else "",
            "Mailing_Zip": normalize_zip(address_parts[-1] if address_parts else ""),
        }

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, get_record_name_profile, normalize_phone, normalize_zip, token_overlap_metrics
        try:
            candidates = self.search(enricher, record)
        except Exception:
            LOGGER.exception("Arizona portal search failed for PID=%s", record.get("PID", ""))
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
            enricher.finalize_state_portal_query("AZ")
            return {}, {}
        try:
            driver = enricher.get_state_portal_driver("AZ")
            try:
                detail_values = self.fetch_detail(enricher, driver=driver, candidate_index=best_candidate.get("candidate_index", ""), action_label=f"arizona detail popup [{record.get('PID', '')}]")
            except Exception:
                LOGGER.exception("Arizona detail popup fetch failed for PID=%s", record.get("PID", ""))
                detail_values = {}
            values = {
                "Mailing_Address": clean_text(detail_values.get("Mailing_Address", "") or best_candidate.get("address", "")),
                "Mailing_Zip": normalize_zip(detail_values.get("Mailing_Zip", "") or best_candidate.get("zip", "")),
                "Telephone": normalize_phone(best_candidate.get("phone", "")),
                "URL": clean_text(best_candidate.get("detail_url", "")),
                "Capacity (optional)": clean_text(best_candidate.get("capacity", "")),
                "Age Range (optional)": "",
            }
            matched_provider_name = clean_text(best_candidate.get("provider_name", ""))
            match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=clean_text(best_candidate.get("city", "")), candidate_address=values.get("Mailing_Address", ""), candidate_phone=values.get("Telephone", ""), candidate_url=values.get("URL", ""))
            values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
            source_url = values.get("URL", "") or "https://azchildcaresearch.azdes.gov/s/providersearch?language=en_US"
            sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Arizona official childcare portal") for field, value in values.items() if clean_text(value)}
            return values, sources
        finally:
            enricher.finalize_state_portal_query("AZ")

from adapters.base import StateAdapter


class MassachusettsAdapter(StateAdapter):
    state = "MA"

    def wait_for_component(self, driver, tag_name, timeout):
        from selenium.webdriver.support.ui import WebDriverWait

        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script(
                """
const host = document.querySelector(arguments[0]);
if (!host) return false;
return !!host.shadowRoot || !!(host.innerHTML || '').trim();
""",
                tag_name,
            )
        )

    def execute_search(self, enricher, driver, provider_name, city, zip_code):
        import time

        from enrich_daycare_data import LOGGER, PortalSearchResult, SELENIUM_WAIT_TIMEOUT, clean_text, normalize_url, normalize_zip
        from selenium.webdriver.support.ui import WebDriverWait

        self.wait_for_component(driver, "c-eec_child-care-search", max(SELENIUM_WAIT_TIMEOUT, 30))
        driver.execute_script(
            """
const host = document.querySelector('c-eec_child-care-search');
const root = (host && host.shadowRoot) || host;
if (!root) return false;
const allInputs = Array.from(root.querySelectorAll('input, textarea'));
const pickInput = (tokens) => allInputs.find((node) => {
  const text = [
    node.name,
    node.id,
    node.placeholder,
    node.getAttribute('aria-label'),
    node.closest('label')?.innerText,
    node.parentElement?.innerText
  ].filter(Boolean).join(' ').toLowerCase();
  return tokens.some((token) => text.includes(token));
});
const assign = (node, value) => {
  if (!node) return;
  node.focus();
  node.value = value || '';
  node.dispatchEvent(new Event('input', { bubbles: true }));
  node.dispatchEvent(new Event('change', { bubbles: true }));
  node.blur();
};
assign(pickInput(['provider', 'program', 'name']), arguments[0]);
assign(pickInput(['city', 'town']), arguments[1]);
assign(pickInput(['zip', 'postal']), arguments[2]);
let searchButton = root.querySelector('#tab-1 slot div:nth-of-type(2) div:nth-of-type(2) lightning-button');
if (!searchButton) {
  const buttons = Array.from(root.querySelectorAll('button, lightning-button'));
  searchButton = buttons.find((btn) => ((btn.innerText || btn.textContent || '').toLowerCase().includes('search')));
}
if (!searchButton) return false;
searchButton.scrollIntoView({ block: 'center' });
searchButton.click();
return true;
""",
            provider_name,
            city,
            zip_code,
        )
        time.sleep(2.0)
        try:
            WebDriverWait(driver, max(SELENIUM_WAIT_TIMEOUT, 20)).until(
                lambda d: d.execute_script(
                    """
const walk = (node, acc) => {
  if (!node) return;
  acc.push(node);
  const children = node.children ? Array.from(node.children) : [];
  children.forEach((child) => {
    walk(child, acc);
    if (child.shadowRoot) walk(child.shadowRoot, acc);
  });
};
const host = document.querySelector('c-eec_child-care-search');
const nodes = [];
walk((host && host.shadowRoot) || host || document.body, nodes);
const text = nodes.map((node) => (node.innerText || node.textContent || '')).join(' ').toLowerCase();
if (text.includes('no results') || text.includes('0 results')) return true;
return nodes.some((node) => node.querySelectorAll && Array.from(node.querySelectorAll('a[href]')).some((a) => {
  const href = (a.getAttribute('href') || '').toLowerCase();
  return href.includes('provider') || href.includes('detail') || href.includes('/child-care/');
}));
""",
                )
            )
        except Exception:
            LOGGER.info(
                "Massachusetts portal search wait timed out for provider=%s city=%s zip=%s; inspecting current DOM anyway",
                provider_name,
                city,
                zip_code,
            )
        result_rows = driver.execute_script(
            """
const walk = (node, acc) => {
  if (!node) return;
  acc.push(node);
  const children = node.children ? Array.from(node.children) : [];
  children.forEach((child) => {
    walk(child, acc);
    if (child.shadowRoot) walk(child.shadowRoot, acc);
  });
};
const host = document.querySelector('c-eec_child-care-search');
const nodes = [];
walk((host && host.shadowRoot) || host || document.body, nodes);
const links = [];
nodes.forEach((node) => {
  if (!node.querySelectorAll) return;
  node.querySelectorAll('a[href]').forEach((a) => links.push(a));
});
const rows = [];
links.forEach((link) => {
  const href = link.href || link.getAttribute('href') || '';
  const title = (link.innerText || link.textContent || '').trim();
  const container = link.closest('article, tr, li, div');
  const text = ((container && (container.innerText || container.textContent)) || '').trim();
  if (!title || !href) return;
  const normalizedHref = href.toLowerCase();
  if (
    normalizedHref.startsWith('#') ||
    normalizedHref.startsWith('javascript:') ||
    (!normalizedHref.includes('provider') && !normalizedHref.includes('detail') && !normalizedHref.includes('/child-care/'))
  ) return;
  rows.push({
    title,
    detail_url: href,
    address: text,
    program_type: text,
  });
});
return rows;
"""
        ) or []
        deduped = []
        seen_urls = set()
        for item in result_rows:
            detail_url = normalize_url(item.get("detail_url", ""))
            title = clean_text(item.get("title", ""))
            if not detail_url or not title or detail_url in seen_urls:
                continue
            seen_urls.add(detail_url)
            deduped.append(
                PortalSearchResult(
                    title=title,
                    detail_url=detail_url,
                    address=clean_text(item.get("address", "")),
                    program_type=clean_text(item.get("program_type", "")),
                )
            )
        return deduped

    def search(self, enricher, record):
        import time

        from enrich_daycare_data import LOGGER, STATE_PORTAL_URLS, clean_text, get_record_name_profile

        driver = enricher.get_search_driver()
        LOGGER.info("Loading Massachusetts official portal for PID=%s", record.get("PID", ""))
        driver.get(STATE_PORTAL_URLS["MA"])
        self.wait_for_component(driver, "c-eec_child-care-search", 30)
        time.sleep(5)
        provider_name = get_record_name_profile(record).search_name_primary
        city = clean_text(record.get("Mailing_City"))
        zip_code = clean_text(record.get("Mailing_Zip"))
        search_variants = [
            (provider_name, city, zip_code),
            (provider_name, city, ""),
            ("", city, zip_code),
            ("", city, ""),
        ]
        for variant_provider, variant_city, variant_zip in search_variants:
            results = self.execute_search(enricher, driver, variant_provider, variant_city, variant_zip)
            if results:
                LOGGER.info(
                    "Massachusetts portal returned %s candidate results for PID=%s",
                    len(results),
                    record.get("PID", ""),
                )
                return results
        LOGGER.info("Massachusetts portal returned 0 candidate results for PID=%s", record.get("PID", ""))
        return []

    def fetch_detail(self, enricher, detail_url):
        import time

        from bs4 import BeautifulSoup
        from enrich_daycare_data import SELENIUM_WAIT_TIMEOUT, age_groups_to_numeric_range, clean_text, domain_of, normalize_phone, normalize_url, normalize_zip

        driver = enricher.get_search_driver()
        driver.get(detail_url)
        self.wait_for_component(driver, "c-eec_provider-details", max(SELENIUM_WAIT_TIMEOUT, 30))
        time.sleep(5)
        shadow_html = driver.execute_script("return document.querySelector('c-eec_provider-details').shadowRoot.innerHTML;")
        soup = BeautifulSoup(shadow_html, "html.parser")
        result = {
            "Mailing_Address": clean_text((soup.select_one(".account-address") or {}).get_text(" ", strip=True) if soup.select_one(".account-address") else ""),
            "Mailing_Zip": "",
            "Telephone": "",
            "URL": "",
            "Capacity (optional)": "",
            "Age Range (optional)": "",
            "Email": "",
        }
        tel = soup.select_one('a[href^="tel:"]')
        if tel:
            result["Telephone"] = normalize_phone(tel.get_text(" ", strip=True))
        email = soup.select_one('a[href^="mailto:"]')
        if email:
            result["Email"] = clean_text(email.get_text(" ", strip=True))
        result["Mailing_Zip"] = normalize_zip(result["Mailing_Address"])
        for block in soup.select(".view-only-info"):
            label = clean_text((block.select_one("label") or {}).get_text(" ", strip=True) if block.select_one("label") else "")
            value = clean_text((block.select_one(".read-only-info") or {}).get_text(" ", strip=True) if block.select_one(".read-only-info") else "")
            if label.startswith("Capacity"):
                result["Capacity (optional)"] = value
        age_groups = []
        for cell in soup.select("td.slds-cell-wrap"):
            text = clean_text(cell.get_text(" ", strip=True))
            if text in {"Infant Age Group", "Toddler Age Group", "Preschool Age Group", "School Age Group", "Kindergarten Age Group"}:
                age_groups.append(text.replace(" Age Group", ""))
        result["Age Range (optional)"] = age_groups_to_numeric_range(age_groups)
        for link in soup.select('a[href^="http"]'):
            href = normalize_url(link.get("href"))
            if not href:
                continue
            domain = domain_of(href)
            if domain in {"mass.gov", "google.com", "www.google.com"}:
                continue
            result["URL"] = href
            break
        return result

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER, build_source_entry, classify_match_status, clean_text, token_overlap_score

        try:
            candidates = self.search(enricher, record)
        except Exception:
            LOGGER.exception("Massachusetts portal search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            return {}, {}
        city = clean_text(record.get("Mailing_City"))
        best = None
        best_score = -999
        for candidate in candidates[:10]:
            combined = f"{candidate.title} {candidate.address} {candidate.program_type}"
            score = token_overlap_score(record.get("Daycare_Name", ""), combined) * 3
            if city and city.lower() in combined.lower():
                score += 2
            if clean_text(record.get("Mailing_State")) == "MA":
                score += 1
            if score > best_score:
                best_score = score
                best = candidate
        if not best or best_score < 7:
            return {}, {}
        try:
            detail = self.fetch_detail(enricher, best.detail_url)
        except Exception:
            LOGGER.exception("Massachusetts detail page fetch failed for PID=%s url=%s", record.get("PID", ""), best.detail_url)
            return {}, {}
        values = {
            "Mailing_Address": detail.get("Mailing_Address", ""),
            "Mailing_Zip": detail.get("Mailing_Zip", ""),
            "Telephone": detail.get("Telephone", ""),
            "URL": detail.get("URL", ""),
            "Capacity (optional)": detail.get("Capacity (optional)", ""),
            "Age Range (optional)": detail.get("Age Range (optional)", ""),
        }
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=best.title,
            candidate_city=city,
            candidate_address=values.get("Mailing_Address", ""),
            candidate_phone=values.get("Telephone", ""),
            candidate_url=best.detail_url,
        )
        values.update(
            {
                "Matched_Provider_Name": best.title,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        sources = {
            field: build_source_entry(
                value=value,
                source_url=best.detail_url,
                source_type="official_state_portal",
                notes="Massachusetts EEC portal",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

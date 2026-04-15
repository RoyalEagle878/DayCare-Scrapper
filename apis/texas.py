from apis.base import StateApi


class TexasApi(StateApi):
    state = "TX"

    def supports_post_run_city_retry(self) -> bool:
        return True

    def _build_result(self, record, best_candidate):
        from enrich_daycare_data import LOGGER, TEXAS_PROVIDER_DETAIL_URL_TEMPLATE, build_source_entry, classify_match_status, clean_text, normalize_age_groups_text_to_numeric_range, normalize_phone, normalize_url, normalize_zip

        source_url = TEXAS_PROVIDER_DETAIL_URL_TEMPLATE.format(provider_id=best_candidate.get("operation_id"))
        website_url = normalize_url(str(best_candidate.get("website_address", "")))
        address_value = clean_text(str(best_candidate.get("location_address", "") or best_candidate.get("mailing_address", "")))
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(str(best_candidate.get("zipcode", ""))),
            "Telephone": normalize_phone(str(best_candidate.get("phone_number", ""))),
            "URL": website_url,
            "Capacity (optional)": clean_text(str(best_candidate.get("total_capacity", ""))),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(str(best_candidate.get("licensed_to_serve_ages", ""))),
        }
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=clean_text(str(best_candidate.get("operation_name", ""))), candidate_city=clean_text(str(best_candidate.get("city", ""))), candidate_address=address_value, candidate_phone=values["Telephone"], candidate_url=website_url, closed_hint=" ".join([clean_text(str(best_candidate.get("operation_status", ""))), clean_text(str(best_candidate.get("temporarily_closed", ""))), clean_text(str(best_candidate.get("adverse_action", "")))]), prior_name_hint=False)
        values.update({"Matched_Provider_Name": clean_text(str(best_candidate.get("operation_name", ""))), "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        sources = {field: build_source_entry(value=value, source_url=website_url if field == "URL" and website_url else source_url, source_type="official_state_portal", notes="Texas official childcare public dataset") for field, value in values.items() if clean_text(value)}
        return values, sources

    def _select_best_candidate(self, enricher, record, candidates):
        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = enricher.score_texas_candidate(record, candidate)
            if score > best_score:
                best_score = score
                best_candidate = candidate
        return best_candidate, best_score

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER
        try:
            candidates = enricher.search_texas_portal_api(record)
        except Exception:
            LOGGER.exception("Texas portal API search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            enricher.queue_api_city_only_retry("TX", record)
            return {}, {}
        best_candidate, best_score = self._select_best_candidate(enricher, record, candidates)
        if not best_candidate or best_score < 6:
            return {}, {}
        return self._build_result(record, best_candidate)

    def run_city_retry(self, enricher, city, records_by_pid):
        from enrich_daycare_data import clean_text

        if not records_by_pid:
            return {}
        sample_record = next(iter(records_by_pid.values()))
        city_rows = enricher.search_texas_portal_api(sample_record, city_only=True)
        if not city_rows:
            return {}
        resolved = {}
        for pid, record in records_by_pid.items():
            best_candidate, best_score = self._select_best_candidate(enricher, record, city_rows)
            if not best_candidate:
                continue
            values, sources = self._build_result(record, best_candidate)
            confidence = int(clean_text(values.get("Match_Confidence", "0")) or "0")
            if clean_text(values.get("Match_Status", "")) != "not_found" or confidence > 60:
                resolved[pid] = (values, sources)
        return resolved

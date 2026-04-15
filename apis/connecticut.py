from apis.base import StateApi


class ConnecticutApi(StateApi):
    state = "CT"

    def supports_post_run_city_retry(self) -> bool:
        return True

    def _build_result(self, record, best_candidate):
        from enrich_daycare_data import CONNECTICUT_PROVIDER_SEARCH_API_URL, build_source_entry, classify_match_status, clean_text, first_non_empty, format_numeric_age_range, normalize_phone, normalize_zip

        address_value = ", ".join(part for part in [first_non_empty(best_candidate, ["address2"]), first_non_empty(best_candidate, ["address3", "city"]), first_non_empty(best_candidate, ["statecode"]) or "CT", normalize_zip(first_non_empty(best_candidate, ["zipcode"]))] if clean_text(part))
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": normalize_zip(first_non_empty(best_candidate, ["zipcode"])),
            "Telephone": normalize_phone(first_non_empty(best_candidate, ["phone"])),
            "URL": "",
            "Capacity (optional)": first_non_empty(best_candidate, ["maximumcapacity"]),
            "Age Range (optional)": format_numeric_age_range(first_non_empty(best_candidate, ["minimumage"]), first_non_empty(best_candidate, ["maximumage"]), unit="years"),
        }
        matched_provider_name = first_non_empty(best_candidate, ["name"])
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=first_non_empty(best_candidate, ["city"]), candidate_address=address_value, candidate_phone=values["Telephone"], candidate_url="", closed_hint="", prior_name_hint=False)
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = f"{CONNECTICUT_PROVIDER_SEARCH_API_URL}?$query="
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Connecticut official childcare public dataset") for field, value in values.items() if clean_text(value)}
        return values, sources

    def _select_best_candidate(self, enricher, record, candidates):
        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = enricher.score_connecticut_candidate(record, candidate)
            if score > best_score:
                best_score = score
                best_candidate = candidate
        return best_candidate, best_score

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER
        try:
            candidates = enricher.search_connecticut_dataset(record)
        except Exception:
            LOGGER.exception("Connecticut dataset search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            enricher.queue_api_city_only_retry("CT", record)
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
        city_rows = enricher.search_connecticut_dataset(sample_record, city_only=True)
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

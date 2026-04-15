from apis.base import StateApi


class PennsylvaniaApi(StateApi):
    state = "PA"

    def supports_post_run_city_retry(self) -> bool:
        return True

    def _build_result(self, record, best_candidate):
        from enrich_daycare_data import build_source_entry, classify_match_status, clean_text, first_non_empty, normalize_phone, normalize_zip

        address_value = ", ".join(part for part in [first_non_empty(best_candidate, ["facility_address"]), first_non_empty(best_candidate, ["facility_address_continued"]), first_non_empty(best_candidate, ["facility_city"]), first_non_empty(best_candidate, ["facility_state"]) or "PA", normalize_zip(first_non_empty(best_candidate, ["facility_zip_code"]))] if clean_text(part))
        mailing_address = clean_text(address_value.split(",", 1)[0]) if "," in address_value else clean_text(address_value)
        values = {
            "Mailing_Address": mailing_address,
            "Mailing_Zip": normalize_zip(first_non_empty(best_candidate, ["facility_zip_code"])),
            "Telephone": normalize_phone(first_non_empty(best_candidate, ["facility_phone"])),
            "URL": "",
            "Capacity (optional)": first_non_empty(best_candidate, ["capacity"]),
            "Age Range (optional)": "",
        }
        matched_provider_name = first_non_empty(best_candidate, ["facility_name"])
        match_status, match_confidence, match_reason = classify_match_status(record, candidate_name=matched_provider_name, candidate_city=first_non_empty(best_candidate, ["facility_city"]), candidate_address=mailing_address, candidate_phone=values["Telephone"], candidate_url="", closed_hint="", prior_name_hint=False)
        values.update({"Matched_Provider_Name": matched_provider_name, "Match_Status": match_status, "Match_Confidence": match_confidence, "Matched_Reason": match_reason})
        source_url = "https://data.pa.gov/resource/ajn5-kaxt.json?$query="
        sources = {field: build_source_entry(value=value, source_url=source_url, source_type="official_state_portal", notes="Pennsylvania official childcare public dataset") for field, value in values.items() if clean_text(value)}
        return values, sources

    def _select_best_candidate(self, enricher, record, candidates):
        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = enricher.score_pennsylvania_candidate(record, candidate)
            if score > best_score:
                best_score = score
                best_candidate = candidate
        return best_candidate, best_score

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER
        try:
            candidates = enricher.search_pennsylvania_dataset(record)
        except Exception:
            LOGGER.exception("Pennsylvania dataset search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            enricher.queue_api_city_only_retry("PA", record)
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
        city_rows = enricher.search_pennsylvania_dataset(sample_record, city_only=True)
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

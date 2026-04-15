from apis.base import StateApi


class ColoradoApi(StateApi):
    state = "CO"

    def supports_post_run_city_retry(self) -> bool:
        return True

    def _build_result(self, enricher, record, candidate, model):
        from enrich_daycare_data import (
            GENERIC_CITY_FIELDS,
            GENERIC_PROVIDER_NAME_FIELDS,
            build_source_entry,
            classify_match_status,
            clean_text,
            first_non_empty,
        )

        state = self.state
        values = enricher.build_generic_open_data_values(state, candidate, model)
        matched_provider_name = first_non_empty(candidate, enricher.resolve_model_filter_fields(model, "provider")) or first_non_empty(candidate, GENERIC_PROVIDER_NAME_FIELDS)
        candidate_city = first_non_empty(candidate, enricher.resolve_model_filter_fields(model, "city")) or first_non_empty(candidate, GENERIC_CITY_FIELDS)
        source_url = clean_text(str(model.get("endpoint", "")))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=candidate_city,
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
        sources = {
            field: build_source_entry(
                value=value,
                source_url=source_url,
                source_type="official_state_portal",
                notes=f"{state} official open-data API",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def _select_best_candidate(self, enricher, record, candidates, model):
        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = enricher.score_generic_open_data_candidate(record, candidate, model)
            if score > best_score:
                best_score = score
                best_candidate = candidate
        return best_candidate, best_score

    def run(self, enricher, record):
        from enrich_daycare_data import (
            clean_text,
        )

        state = self.state
        model = enricher.get_state_scraper_model(state)
        if not model:
            return {}, {}
        candidates = enricher.search_generic_open_data_api(record)
        if not candidates:
            enricher.queue_api_city_only_retry(state, record)
            return {}, {}
        best_candidate, best_score = self._select_best_candidate(enricher, record, candidates, model)
        if not best_candidate or best_score < 6:
            return {}, {}
        return self._build_result(enricher, record, best_candidate, model)

    def run_city_retry(self, enricher, city, records_by_pid):
        from enrich_daycare_data import clean_text

        state = self.state
        model = enricher.get_state_scraper_model(state)
        if not model or not records_by_pid:
            return {}
        sample_record = next(iter(records_by_pid.values()))
        city_rows = enricher.search_generic_open_data_api(sample_record, city_only=True)
        if not city_rows:
            return {}
        resolved = {}
        for pid, record in records_by_pid.items():
            best_candidate, best_score = self._select_best_candidate(enricher, record, city_rows, model)
            if not best_candidate:
                continue
            values, sources = self._build_result(enricher, record, best_candidate, model)
            confidence = int(clean_text(values.get("Match_Confidence", "0")) or "0")
            if clean_text(values.get("Match_Status", "")) != "not_found" or confidence > 60:
                resolved[pid] = (values, sources)
        return resolved

from apis.base import StateApi


class CaliforniaApi(StateApi):
    state = "CA"

    def supports_post_run_city_retry(self) -> bool:
        return True

    def _select_best_candidate(self, enricher, record, candidates):
        from enrich_daycare_data import LOGGER, first_non_empty

        best_candidate = None
        best_score = -999
        for candidate in candidates:
            score = enricher.score_california_candidate(record, candidate)
            LOGGER.info(
                "California candidate scored %s for PID=%s facility=%s",
                score,
                record.get("PID", ""),
                first_non_empty(candidate, ["facility_name", "licensee_name"]),
            )
            if score > best_score:
                best_score = score
                best_candidate = candidate
        return best_candidate, best_score

    def _build_result(self, record, best_candidate):
        from enrich_daycare_data import (
            STATE_PORTAL_URLS,
            build_source_entry,
            classify_match_status,
            clean_text,
            first_non_empty,
            normalize_age_groups_text_to_numeric_range,
            normalize_phone,
            normalize_url,
            normalize_zip,
        )

        website_url = normalize_url(first_non_empty(best_candidate, ["facility_website", "website", "website_address"]))
        address_value = ", ".join(
            part
            for part in [
                first_non_empty(best_candidate, ["facility_address", "address", "street_address"]),
                first_non_empty(best_candidate, ["facility_city", "city"]),
                first_non_empty(best_candidate, ["facility_state", "state"]) or "CA",
                normalize_zip(first_non_empty(best_candidate, ["facility_zip", "zip", "zipcode"])),
            ]
            if clean_text(part)
        )
        mailing_address = clean_text(address_value.split(",", 1)[0]) if "," in address_value else clean_text(address_value)
        values = {
            "Mailing_Address": mailing_address,
            "Mailing_Zip": normalize_zip(first_non_empty(best_candidate, ["facility_zip", "zip", "zipcode"])),
            "Telephone": normalize_phone(first_non_empty(best_candidate, ["telephone", "phone", "facility_phone"])),
            "URL": website_url,
            "Capacity (optional)": first_non_empty(best_candidate, ["capacity", "licensed_capacity", "facility_capacity"]),
            "Age Range (optional)": normalize_age_groups_text_to_numeric_range(first_non_empty(best_candidate, ["age_range", "ages_served", "facility_ages"])),
        }
        matched_provider_name = first_non_empty(best_candidate, ["facility_name", "licensee_name"])
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=first_non_empty(best_candidate, ["facility_city", "city"]),
            candidate_address=mailing_address,
            candidate_phone=values["Telephone"],
            candidate_url=website_url,
            closed_hint=" ".join(
                [
                    first_non_empty(best_candidate, ["status", "facility_status", "license_status"]),
                    first_non_empty(best_candidate, ["closed", "temporarily_closed"]),
                ]
            ),
            prior_name_hint=False,
        )
        values.update(
            {
                "Matched_Provider_Name": matched_provider_name,
                "Match_Status": match_status,
                "Match_Confidence": match_confidence,
                "Matched_Reason": match_reason,
            }
        )
        source_url = website_url or STATE_PORTAL_URLS["CA"]
        sources = {}
        for field, value in values.items():
            if not clean_text(value):
                continue
            notes = "California official childcare public dataset"
            if field.startswith("Match_") or field == "Matched_Provider_Name":
                notes = "California official childcare public dataset; accepted candidate metadata"
            sources[field] = build_source_entry(
                value=value,
                source_url=website_url if field == "URL" and website_url else source_url,
                source_type="official_state_portal",
                notes=notes,
            )
        return values, sources

    def run(self, enricher, record):
        from enrich_daycare_data import LOGGER

        try:
            candidates = enricher.search_california_dataset(record)
        except Exception:
            LOGGER.exception("California dataset search failed for PID=%s", record.get("PID", ""))
            return {}, {}
        if not candidates:
            enricher.queue_api_city_only_retry("CA", record)
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
        city_rows = enricher.search_california_dataset(sample_record, city_only=True)
        if not city_rows:
            return {}
        resolved = {}
        for pid, record in records_by_pid.items():
            best_candidate, _best_score = self._select_best_candidate(enricher, record, city_rows)
            if not best_candidate:
                continue
            values, sources = self._build_result(record, best_candidate)
            confidence = int(clean_text(values.get("Match_Confidence", "0")) or "0")
            if clean_text(values.get("Match_Status", "")) != "not_found" or confidence > 60:
                resolved[pid] = (values, sources)
        return resolved

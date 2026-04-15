from apis.base import StateApi


class NewYorkApi(StateApi):
    state = "NY"

    def supports_post_run_city_retry(self) -> bool:
        return True

    def fetch_rows(self, record, city_only=False):
        from urllib.parse import quote
        import time

        import requests

        from enrich_daycare_data import (
            LOGGER,
            REQUEST_TIMEOUT,
            RETRY_BACKOFF_SECONDS,
            SEARCH_RETRIES,
            build_random_request_headers,
            clean_text,
            get_record_name_profile,
        )

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        base_url = "https://data.ny.gov/resource/fymg-3wv3.json"

        if city_only:
            variants = [""]
        else:
            variants = list(profile.search_name_variants)

        for variant in variants:
            if city_only:
                soql = (
                    'SELECT `facility_name`, `address_omitted`, `street_number`, `street_name`, `zip_code`, `phone_number`, `total_capacity`, `city` '
                    f'WHERE caseless_one_of(`city`, "{city.replace(chr(34), chr(92) + chr(34))}")'
                )
            else:
                soql = (
                    'SELECT `facility_name`, `address_omitted`, `street_number`, `street_name`, `zip_code`, `phone_number`, `total_capacity`, `city` '
                    f'WHERE caseless_contains(`facility_name`, "{variant.replace(chr(34), chr(92) + chr(34))}") '
                    f'AND caseless_one_of(`city`, "{city.replace(chr(34), chr(92) + chr(34))}")'
                )
            query_url = f"{base_url}?$query={quote(soql, safe='')}"
            response = None
            for attempt in range(SEARCH_RETRIES + 1):
                try:
                    response = requests.get(
                        query_url,
                        headers=build_random_request_headers(),
                        timeout=REQUEST_TIMEOUT,
                    )
                    response.raise_for_status()
                    break
                except Exception:
                    if attempt >= SEARCH_RETRIES:
                        LOGGER.exception(
                            "New York dataset search failed for PID=%s variant=%s city_only=%s",
                            record.get("PID", ""),
                            variant,
                            city_only,
                        )
                        response = None
                        break
                    time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
            if not response:
                continue
            rows = response.json() if response.content else []
            if rows:
                return rows
        return []

    def _build_result(self, record, best_candidate):
        import re

        from enrich_daycare_data import (
            build_source_entry,
            classify_match_status,
            clean_text,
            normalize_phone,
            normalize_zip,
        )

        base_url = "https://data.ny.gov/resource/fymg-3wv3.json"
        street_number = clean_text(best_candidate.get("street_number", ""))
        street_name = clean_text(best_candidate.get("street_name", ""))
        address_value = clean_text(" ".join(part for part in [street_number, street_name] if clean_text(part)))
        zip_value = normalize_zip(best_candidate.get("zip_code", ""))
        if zip_value and not re.fullmatch(r"\d{5}(?:-\d{4})?", zip_value):
            zip_value = ""
        values = {
            "Mailing_Address": address_value,
            "Mailing_Zip": zip_value,
            "Telephone": normalize_phone(best_candidate.get("phone_number", "")),
            "URL": "",
            "Capacity (optional)": clean_text(best_candidate.get("total_capacity", "")),
            "Age Range (optional)": "",
        }
        matched_provider_name = clean_text(best_candidate.get("facility_name", ""))
        match_status, match_confidence, match_reason = classify_match_status(
            record,
            candidate_name=matched_provider_name,
            candidate_city=clean_text(best_candidate.get("city", "")),
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
        sources = {
            field: build_source_entry(
                value=value,
                source_url=base_url,
                source_type="official_state_portal",
                notes="New York official childcare public dataset",
            )
            for field, value in values.items()
            if clean_text(value)
        }
        return values, sources

    def _select_best_candidate(self, record, candidates):
        from enrich_daycare_data import clean_text, get_record_name_profile, token_overlap_metrics

        profile = get_record_name_profile(record)
        city = clean_text(record.get("Mailing_City"))
        best_candidate = None
        best_score = -999
        best_overlap = 0.0
        best_city_match = False
        for candidate in candidates:
            provider_name = clean_text(candidate.get("facility_name", ""))
            shared, recall, _ = token_overlap_metrics(record.get("Daycare_Name", ""), provider_name)
            score = shared * 4
            city_match = clean_text(candidate.get("city", "")).lower() == city.lower()
            if city_match:
                score += 3
            if clean_text(candidate.get("phone_number", "")):
                score += 1
            variant_hit = any(
                clean_text(v) and len(clean_text(v)) >= 4 and clean_text(v).lower() in provider_name.lower()
                for v in profile.search_name_variants
            )
            if variant_hit:
                score += 4
            if score > best_score:
                best_score = score
                best_candidate = candidate
                best_overlap = recall
                best_city_match = city_match
        if not best_candidate:
            return None, -999, 0.0, False
        return best_candidate, best_score, best_overlap, best_city_match

    def run_city_retry(self, enricher, city, records_by_pid):
        from enrich_daycare_data import LOGGER, clean_text

        if not records_by_pid:
            return {}
        sample_record = next(iter(records_by_pid.values()))
        city_rows = self.fetch_rows(sample_record, city_only=True)
        if not city_rows:
            return {}
        resolved = {}
        for pid, record in records_by_pid.items():
            best_candidate, best_score, _best_overlap, _best_city_match = self._select_best_candidate(record, city_rows)
            if not best_candidate:
                continue
            values, sources = self._build_result(record, best_candidate)
            confidence = int(clean_text(values.get("Match_Confidence", "0")) or "0")
            if clean_text(values.get("Match_Status", "")) != "not_found" or confidence > 60:
                resolved[pid] = (values, sources)
                LOGGER.info("NY city retry matched PID=%s city=%s confidence=%s", pid, city, confidence)
        return resolved

    def run(self, enricher, record):
        from enrich_daycare_data import (
            clean_text,
        )

        city_only = "NY" in getattr(enricher, "api_city_only_retry_active_states", set())
        candidates = self.fetch_rows(record, city_only=city_only)

        if not candidates:
            if not city_only:
                enricher.queue_api_city_only_retry("NY", record)
            return {}, {}

        best_candidate, best_score, best_overlap, best_city_match = self._select_best_candidate(record, candidates)

        if not best_candidate or (best_score < 6 and not (best_city_match and best_overlap >= 0.35)):
            return {}, {}

        return self._build_result(record, best_candidate)

from typing import Dict, Tuple, TYPE_CHECKING


if TYPE_CHECKING:
    from enrich_daycare_data import DaycareEnricher


class StateApi:
    state = ""

    def run(self, enricher: "DaycareEnricher", record: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        raise NotImplementedError

    def supports_post_run_city_retry(self) -> bool:
        return False

    def run_city_retry(self, enricher: "DaycareEnricher", city: str, records_by_pid: Dict[str, Dict[str, str]]) -> Dict[str, Tuple[Dict[str, str], Dict[str, Dict[str, str]]]]:
        return {}

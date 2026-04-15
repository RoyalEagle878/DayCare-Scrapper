from typing import Dict, Tuple, TYPE_CHECKING


if TYPE_CHECKING:
    from enrich_daycare_data import DaycareEnricher


class StateAdapter:
    state = ""

    def run(self, enricher: "DaycareEnricher", record: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        raise NotImplementedError


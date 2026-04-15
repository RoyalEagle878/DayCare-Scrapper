from apis.california import CaliforniaApi
from apis.colorado import ColoradoApi
from apis.connecticut import ConnecticutApi
from apis.delaware import DelawareApi
from apis.new_york import NewYorkApi
from apis.pennsylvania import PennsylvaniaApi
from apis.texas import TexasApi
from apis.utah import UtahApi
from apis.washington import WashingtonApi


API_REGISTRY = {
    "CA": CaliforniaApi(),
    "CO": ColoradoApi(),
    "CT": ConnecticutApi(),
    "DE": DelawareApi(),
    "NY": NewYorkApi(),
    "PA": PennsylvaniaApi(),
    "TX": TexasApi(),
    "UT": UtahApi(),
    "WA": WashingtonApi(),
}

from adapters.arizona import ArizonaAdapter
from adapters.illinois import IllinoisAdapter
from adapters.maryland import MarylandAdapter
from adapters.michigan import MichiganAdapter
from adapters.minnesota import MinnesotaAdapter
from adapters.new_hampshire import NewHampshireAdapter
from adapters.new_jersey import NewJerseyAdapter
from adapters.north_carolina import NorthCarolinaAdapter
from adapters.oklahoma import OklahomaAdapter
from adapters.south_carolina import SouthCarolinaAdapter
from adapters.virginia import VirginiaAdapter


ADAPTER_REGISTRY = {
    "AZ": ArizonaAdapter(),
    "IL": IllinoisAdapter(),
    "MD": MarylandAdapter(),
    "MI": MichiganAdapter(),
    "MN": MinnesotaAdapter(),
    "NC": NorthCarolinaAdapter(),
    "NH": NewHampshireAdapter(),
    "NJ": NewJerseyAdapter(),
    "OK": OklahomaAdapter(),
    "SC": SouthCarolinaAdapter(),
    "VA": VirginiaAdapter(),
}

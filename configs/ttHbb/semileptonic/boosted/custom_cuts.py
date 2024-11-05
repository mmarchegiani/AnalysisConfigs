from collections.abc import Iterable
import awkward as ak
import custom_cut_functions as cuts_f
from pocket_coffea.lib.cut_definition import Cut

semileptonic_presel = Cut(
    name="semileptonic",
    params={
        "njet": 4,
        "nbjet": 3,
        "pt_leading_electron": {
            '2016_PreVFP': 29,
            '2016_PostVFP': 29,
            '2017': 30,
            '2018': 30,
        },
        "pt_leading_muon": {
            '2016_PreVFP': 26,
            '2016_PostVFP': 26,
            '2017': 29,
            '2018': 26,
        },
        "met": 20,
    },
    function=cuts_f.semileptonic,
)

semileptonic_presel_boosted = Cut(
    name="semileptonic",
    params={
        "njet": 2,
        "nbjet": 0,
        "nlightjet": 2,
        "nfatjet": 1,
        "pt_leading_electron": {
            '2016_PreVFP': 29,
            '2016_PostVFP': 29,
            '2017': 30,
            '2018': 30,
        },
        "pt_leading_muon": {
            '2016_PreVFP': 26,
            '2016_PostVFP': 26,
            '2017': 29,
            '2018': 26,
        },
        "met": 20,
    },
    function=cuts_f.semileptonic,
)

# Selection for ttbar background categorization
def get_genTtbarId_100_eq(genTtbarId, name=None):
    if name == None:
        if type(genTtbarId) == int:
            name = f"genTtbarId_100_eq_{genTtbarId}"
        if isinstance(genTtbarId, Iterable):
            name = f"genTtbarId_100_eq_" + "_".join([str(s) for s in genTtbarId])
    return Cut(name=name, params={"genTtbarId" : genTtbarId}, function=cuts_f.eq_genTtbarId_100)

SR = Cut(name="boosted_sr", params=None, function=cuts_f.signal_region)
CR = Cut(name="boosted_cr", params=None, function=cuts_f.control_region)

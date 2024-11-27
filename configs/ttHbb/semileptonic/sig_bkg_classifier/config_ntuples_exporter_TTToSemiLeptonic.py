from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_definition import Cut
from pocket_coffea.lib.columns_manager import ColOut
from pocket_coffea.lib.cut_functions import get_nObj_min, get_HLTsel, get_nBtagMin, get_nPVgood, goldenJson, eventFlags
from pocket_coffea.parameters.cuts import passthrough
from pocket_coffea.parameters.histograms import *

from configs.ttHbb.semileptonic.common.workflows import workflow_tthbb as workflow
from configs.ttHbb.semileptonic.common.workflows.workflow_tthbb import ttHbbPartonMatchingProcessor

import custom_cut_functions
import custom_cuts
from custom_cut_functions import *
from custom_cuts import *
import os
localdir = os.path.dirname(os.path.abspath(__file__))

# Loading default parameters
from pocket_coffea.parameters import defaults
default_parameters = defaults.get_default_parameters()
defaults.register_configuration_dir("config_dir", localdir+"/params")

parameters = defaults.merge_parameters_from_files(default_parameters,
                                                  f"{localdir}/params/object_preselection_semileptonic.yaml",
                                                  f"{localdir}/params/triggers.yaml",
                                                  f"{localdir}/params/lepton_scale_factors.yaml",
                                                  f"{localdir}/params/btagging.yaml",
                                                  f"{localdir}/params/btagSF_calibration.yaml",
                                                  f"{localdir}/params/plotting_style.yaml",
                                                  update=True)

cfg = Configurator(
    parameters = parameters,
    datasets = {
        "jsons": [
                  f"{localdir}/datasets/backgrounds_MC_ttbar_local.json"
                  ],
        "filter" : {
            "samples": [
                        "TTToSemiLeptonic"
                        ],
            "samples_exclude" : [],
            "year": [
                     "2018"
                     ]
        },
        "subsamples": {
            'TTbbSemiLeptonic' : {
                'TTbbSemiLeptonic_tt+LF'   : [get_genTtbarId_100_eq(0)],
                'TTbbSemiLeptonic_tt+C'    : [get_genTtbarId_100_eq([41, 42, 43, 44, 45, 46])],
                'TTbbSemiLeptonic_tt+B'    : [get_genTtbarId_100_eq([51, 52, 53, 54, 55, 56])],
            },
            'TTToSemiLeptonic' : {
                'TTToSemiLeptonic_tt+LF'   : [get_genTtbarId_100_eq(0)],
                'TTToSemiLeptonic_tt+C'    : [get_genTtbarId_100_eq([41, 42, 43, 44, 45, 46])],
                'TTToSemiLeptonic_tt+B'    : [get_genTtbarId_100_eq([51, 52, 53, 54, 55, 56])],
            },
        }
    },

    workflow = ttHbbPartonMatchingProcessor,
    workflow_options = {"parton_jet_min_dR": 0.3,
                        "dump_columns_as_arrays_per_chunk": "root://t3se01.psi.ch:1094//store/user/mmarcheg/ttHbb/ntuples/sig_bkg_ntuples_TTToSemiLeptonic_2018/"},
    
    skim = [get_nPVgood(1),
            eventFlags,
            goldenJson,
            get_nObj_min(4, 15., "Jet"),
            get_nBtagMin(3, 15., coll="Jet", wp="M"),
            get_HLTsel(primaryDatasets=["SingleEle", "SingleMuon"])],
    
    preselections = [semileptonic_presel],
    categories = {
        "baseline": [passthrough],
        "semilep_LHE": [semilep_lhe]
    },

    weights= {
        "common": {
            "inclusive": [
                "genWeight", "lumi","XS",
                "pileup",
                "sf_ele_reco", "sf_ele_id", "sf_ele_trigger",
                "sf_mu_id", "sf_mu_iso", "sf_mu_trigger",
                "sf_btag", "sf_btag_calib",
                "sf_jet_puId",
            ],
            "bycategory": {},
        },
        "bysample": {},
    },
    variations = {
        "weights": {"common": {"inclusive": [], "bycategory": {}}, "bysample": {}},
    },
    
    variables = {},
    columns = {
        "common": {
            "inclusive": [],
            "bycategory": {
                    "semilep_LHE": [
                        ColOut(
                            "Parton",
                            ["pt", "eta", "phi", "mass", "pdgId", "provenance"],
                            flatten=False
                        ),
                        ColOut(
                            "PartonMatched",
                            ["pt", "eta", "phi","mass", "pdgId", "provenance", "dRMatchedJet"],
                            flatten=False
                        ),
                        ColOut(
                            "JetGood",
                            ["pt", "eta", "phi", "hadronFlavour", "btagDeepFlavB", "btag_L", "btag_M", "btag_H"],
                            flatten=False
                        ),
                        ColOut(
                            "JetGoodMatched",
                            ["pt", "eta", "phi", "hadronFlavour", "btagDeepFlavB", "btag_L", "btag_M", "btag_H", "dRMatchedJet"],
                            flatten=False
                        ),
                        ColOut(
                            "LeptonGood",
                            ["pt","eta","phi", "pdgId", "charge", "mvaTTH"],
                            pos_end=1,
                            flatten=False
                        ),
                        ColOut(
                            "MET",
                            ["phi","pt","significance"],
                            flatten=False
                        ),
                        ColOut(
                            "Generator",
                            ["x1","x2","id1","id2","xpdf1","xpdf2"],
                            flatten=False
                        ),
                        ColOut(
                            "LeptonParton",
                            ["pt","eta","phi","mass","pdgId"],
                            flatten=False
                        ),
                    ]
            }
        },
        "bysample": {
        },
    },
)

# Registering custom functions
import cloudpickle
cloudpickle.register_pickle_by_value(workflow)
cloudpickle.register_pickle_by_value(custom_cut_functions)
cloudpickle.register_pickle_by_value(custom_cuts)

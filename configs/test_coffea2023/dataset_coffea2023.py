import gzip
import json
import argparse
from distributed import LocalCluster, Client
from multiprocessing import freeze_support

from coffea.dataset_tools import rucio_utils
from coffea.dataset_tools.dataset_query import print_dataset_query
from rich.console import Console
from rich.table import Table
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI

dataset_definition = {
    "/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X*/NANOAODSIM": {"short_name": "ZJets",
                                                                                                   "metadata": {"xsec": 100.0,"isMC":True}},
    "/SingleMuon/Run2018C-UL20*_MiniAODv2_NanoAODv9_GT36*/NANOAOD": {"short_name": "SingleMuon", "metadata": {"isMC":False}}
}

if __name__ == '__main__':
    freeze_support()

    parser = argparse.ArgumentParser(description='Create json dataset via CLI')
    parser.add_argument('-j', '--workers', default=8, type=int, help='Number of workers')
    args = parser.parse_args()

    # Start LocalCluster
    cluster = LocalCluster(
    	n_workers=1,
    	memory_limit="4GiB",
    	processes=True,
    	threads_per_worker=1,
    #	scheduler_port=8786
    #	asynchronous=True
    )

    cluster.scale(args.workers)

    # Start dask client
    client = Client(cluster)

    ddc = DataDiscoveryCLI()
    ddc.do_regex_sites(r"T[123]_(CH|IT|UK|FR|DE)_\w+")
    ddc.load_dataset_definition(dataset_definition,
                               query_results_strategy="all",
                               replicas_strategy="round-robin")

    fileset_total = ddc.do_preprocess(output_file="fileset",
                      step_size=100000,  #chunk size for files splitting
                      align_to_clusters=False,
                      scheduler_url=client.scheduler.address)

    with gzip.open("fileset_available.json.gz", "rt") as file:
        fileset_available = json.load(file)

    dataset = '/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM'
    for i, (file, meta) in enumerate(fileset_available[dataset]["files"].items()):
        print(file, meta)
        if i>3: break

import os

import luigi
import law
import json

from pocket_coffea.utils.dataset import build_datasets
from pocket_coffea.utils.run import DaskRunner

class TaskBase(law.Task):
    
    cfg = luigi.Parameter(description="Config file with parameters specific to the current run")
    datasets_definition = luigi.Parameter(description="Datasets definition file")

class CreateDataset(TaskBase):

    keys = luigi.TupleParameter(default=[], description="Keys of the datasets to be created. If None, the keys are read from the datasets definition file")
    download = luigi.BoolParameter(default=False, description="If True, the datasets are downloaded from the DAS")
    overwrite = luigi.BoolParameter(default=False, description="If True, existing .json datasets are overwritten")
    check = luigi.BoolParameter(default=False, description="If True, the existence of the datasets is checked")
    split_by_year = luigi.BoolParameter(default=False, description="If True, the datasets are split by year")
    local_prefix = luigi.Parameter(default="", description="Prefix of the local path where the datasets are stored")
    whitelist_sites = luigi.TupleParameter(default=[], description="List of sites to be whitelisted")
    blacklist_sites = luigi.TupleParameter(default=[], description="List of sites to be blacklisted")
    regex_sites = luigi.Parameter(default="", description="Regex string to be used to filter the sites")
    parallelize = luigi.IntParameter(default=4, description="Number of parallel processes to be used to fetch the datasets")

    def read_datasets_definition(self):
        with open(os.path.abspath(self.datasets_definition), "r") as f:
            return json.load(f)
    
    def output(self):
        datasets = self.read_datasets_definition()
        dataset_paths = set()
        for dataset in datasets.values():
            filepath = os.path.abspath(f"{dataset['json_output']}")
            dataset_paths.add(filepath)
            dataset_paths.add(f"{filepath}".replace(".json", "_redirector.json"))
            
        return [law.LocalFileTarget(d) for d in dataset_paths]
    
    def run(self):
        build_datasets(
            self.datasets_definition,
            keys=self.keys,
            download=self.download,
            overwrite=self.overwrite,
            check=self.check,
            split_by_year=self.split_by_year,
            local_prefix=self.local_prefix,
            whitelist_sites=self.whitelist_sites,
            blacklist_sites=self.blacklist_sites,
            regex_sites=self.regex_sites,
            parallelize=self.parallelize,
        )

class Runner(TaskBase):

    output_dir = luigi.Parameter(default=os.path.join(os.getcwd(), "test"))
    test = luigi.BoolParameter(default=False, description="Run with limit 1 interactively")
    limit_files = luigi.IntParameter(default=None, description="Limit number of files")
    limit_chunks = luigi.IntParameter(default=None, description="Limit number of chunks") # FIXME: check that this None actually wroks
    executor = luigi.ChoiceParameter(
        choices=["iterative", "futures", "dask", "parsl"],
        default="iterative",
        description="Overwrite executor from config (to be used only with the --test options)")
    architecture = luigi.ChoiceParameter(
        choices=["slurm", "condor", "local"],
        default="local",
        description="Overwrite architecture from config")
    scaleout = luigi.IntParameter(default=0, description="Overwrite scalout config")
    loglevel = luigi.Parameter(default="INFO", description="Logging level")
    full = luigi.BoolParameter(default=False, description="Process all datasets at the same time")

    def requires(self):
        return CreateDataset.req(self)

    def output(self):
        required_files = [os.path.join(self.output_dir, "output_all.coffea"), os.path.join(self.output_dir, "parameters_dump.yaml")]
        return [law.LocalFileTarget(file) for file in required_files]

    def run(self):
        if self.executor == "iterative":
            if self.architecture != "local":
                raise ValueError("Iterative executor can only be used with local architecture")
            raise NotImplementedError
            # runner = IterativeRunner(
            #     architecture=self.architecture,
            #     output_dir=self.output_dir,
            #     cfg=self.cfg,
            #     loglevel=self.loglevel,
            #     )
        if self.executor == "dask":
            runner = DaskRunner(
                architecture=self.architecture,
                output_dir=self.output_dir,
                cfg=self.cfg,
                loglevel=self.loglevel,
                )
        elif self.executor == "parsl":
            raise NotImplementedError
        runner.run(
            full=self.full,
            test=self.test,
            limit_files=self.limit_files,
            limit_chunks=self.limit_chunks,
            scaleout=self.scaleout,
        )

class Plotter(TaskBase):

    output_dir = luigi.Parameter(default=os.path.join(os.getcwd(), "test"))

    def requires(self):
        return Runner.req(self)

    def output(self):
        # Here we should define the list of the output files of plots
        pass

    run_plots = False
    def complete(self):
        if self.run_plots == True:
            return True
        else:
            return False

    def run(self):
        config_dir = os.path.abspath(os.path.dirname(self.cfg))
        os.system(f"make_plots.py --cfg {self.output_dir}/parameters_dump.yaml -op {config_dir}/params/plotting_style.yaml -i {self.output_dir}/output_all.coffea -o {self.output_dir}/plots -j 8")
        self.run_plots = True

import os

import luigi
import law
import json

from pocket_coffea.utils.dataset import build_datasets

class TaskBase(law.Task):
    
    cfg = luigi.Parameter()
    datasets_definition = luigi.Parameter()

class CreateDataset(TaskBase):

    keys = luigi.Parameter()

    def read_datasets_definition(self, path):
        with open(path, "r") as f:
            return json.load(f)
    
    def output(self):
        datasets = self.read_datasets_definition(self.datasets_definition)
        dataset_paths = set()
        for dataset in datasets.values():
            filepath = os.path.abspath(f"{dataset['json_output']}")
            dataset_paths.add(filepath)
            dataset_paths.add(f"{filepath}".replace(".json", "_redirector.json"))
            
        return [law.LocalFileTarget(d) for d in dataset_paths]
    
    def run(self):
        args = {'cfg' : self.datasets_definition}
        build_datasets(**args)

class Runner(TaskBase):

    output_dir = luigi.Parameter(default=os.path.join(os.getcwd(), "test"))

    def requires(self):
        return CreateDataset.req(self)

    def output(self):
        required_files = [os.path.join(self.output_dir, "output_all.coffea"), os.path.join(self.output_dir, "parameters_dump.yaml")]
        return [law.LocalFileTarget(file) for file in required_files]

    def run(self):
        os.system(f"runner.py --cfg {self.cfg} -o {self.output_dir} --full --test -lf 1")

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

import os

import luigi
import law
import json

class TaskBase(law.Task):
    
    cfg = luigi.Parameter()
    datasets_definition = luigi.Parameter()

class CreateDataset(TaskBase):

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
        os.system(f"build_datasets.py --cfg {self.datasets_definition}")

class Runner(TaskBase):

    output_dir = luigi.Parameter(default=os.path.join(os.getcwd(), "test"))

    def requires(self):
        return CreateDataset.req(self)

    def output(self):
        return law.LocalFileTarget(os.path.join(self.output_dir, "output_all.coffea"))

    def run(self):
        print("Running runner.py....")

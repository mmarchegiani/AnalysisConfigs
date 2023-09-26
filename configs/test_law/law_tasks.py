import os

import luigi
import law
import json

class TaskBase(law.Task):
    
    config_dir = luigi.Parameter(default=os.getcwd())
    datasets_dir = luigi.Parameter(default=os.getcwd()+"/datasets")
    datasets_definition = luigi.Parameter(default=os.getcwd()+"/datasets/datasets_definitions_example.json")

class CreateDataset(TaskBase):
    
    def read_datasets_definition(self, path):
        with open(path, "r") as f:
            return json.load(f)
    
    def output(self):
        datasets = self.read_datasets_definition(self.datasets_definition)
        dataset_paths = set()
        for dataset in datasets.values():
            dataset_paths.add(f"{self.datasets_dir}/{dataset['json_output']}")
            dataset_paths.add(f"{self.datasets_dir}/{dataset['json_output']}".replace(".json", "_redirector.json"))
            
        return [law.LocalFileTarget(d) for d in dataset_paths]
    
    def run(self):
        os.system(f"build_datasets.py --cfg {self.datasets_definition}")

class Runner(TaskBase):
    
    def requires(self):
        return CreateDataset.req(self)
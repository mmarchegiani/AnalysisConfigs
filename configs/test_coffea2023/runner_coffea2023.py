import gzip
import json
from coffea.dataset_tools import rucio_utils
from coffea.dataset_tools.dataset_query import print_dataset_query
from rich.console import Console
from rich.table import Table
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI

with gzip.open("fileset_available.json.gz", "rt") as file:
    fileset_available = json.load(file)

events = NanoEventsFactory.from_root(
    {filename: "Events"},
    steps_per_file=2_000,
    metadata={"dataset": "DoubleMuon"},
    schemaclass=BaseSchema,
).events()
p = MyProcessor()
out = p.process(events)
(computed,) = dask.compute(out)


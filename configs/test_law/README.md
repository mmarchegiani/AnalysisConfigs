# Workflow management with law

The law python package allows to define a workflow composed of several tasks. Tasks are organized in a tree dependency such that the output (or _target_) of one task is the input for the next task.

## Identify analysis tasks to automize

In the first toy example, 3 tasks usually performed with PocketCoffea tools are identified to be automized in a workflow:

- Create .json datasets from the `datasets_definitions.json` file
- Run the analysis processor to produce output histograms
- Produce output Data/MC plots

The idea is to make use of the pre-existent PocketCoffea tools to perform these operations and organize them into law tasks.
The dataset creation is done using the `Dataset` class. The analysis is run using the newest `Runner` class that manages all the operations for the analysis run, interfacing different scheduler, architectures and computing sites. The plotting is performed using the `PlotManager` class that produces Data/MC plots.

## Workflow management with law

We define 3 tasks in law:

- `CreateDataset`: task that outputs a list of .json files containing the list of files to be read by the processor
- `Runner`: task that outputs a .coffea output file and the parameters in .yaml format
- `Plotter`: task that outputs Data/MC plots in the plot folders

### CreateDataset task

TO DO.

### Runner task

TO DO.

### Plotter task

TO DO.

## How to run

TO DO.

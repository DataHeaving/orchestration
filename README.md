# Data Heaving - Orchestration
[![Code Coverage](https://codecov.io/gh/DataHeaving/orchestration/branch/develop/graph/badge.svg)](https://codecov.io/gh/DataHeaving/orchestration)

This repository is part of [Data Heaving project](https://github.com/DataHeaving).
There are multiple packages in the repository, all of which are related to transforming data [Data Heaving Orchestration API](https://github.com/DataHeaving/orchestration/pipelines):
- [Pipelines package](pipelines) to manage definitions of data pipelines: source, transformation(s), and sink, and
- [Scheduler package](scheduler) to provide very simple way to manage executing the data pipelines.

# Orchestration - The Driving Reason for Data Heaving
These days, most organizations have identified the need to work with data: to read or write it, to move it around, to transform it, to _heave_ it around the organizational landscape of on-prem and cloud resources.
The [Data Heaving project](https://github.com/DataHeaving) aims to provide one way to handle such data needs.
The project consists of a group of NPM packages designed to be used for reading (`data source`), writing (`data sink`), and transforming the data in various ways.

Since this kind of data manipulation quickly becomes either too fragmented or too monolithic, the Data Heaving project provides modular libraries to be used in a compact and uniform way for all various data pipelines.
This _orchestration_ of data pipelines is captured the best by [pipelines package](pipelines) in this repository.
That package can be thought of entrypoint to Data Heaving for people searching for a way to manage their data pipelines.

# Usage
All packages of Data Heaving project are published as NPM packages to public NPM repository under `@data-heaving` organization.

# More information
One can explore the [Pipelines package](pipelines) documentation and source code for more information.
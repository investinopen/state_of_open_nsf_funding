State of Open Infrastructure - NSF Funding Data Ingest
======================================================

This repository contains scripts that were used to process NSF Funding XML documents to a jsonlines format for ingest into Big Query as 
part of the [State of Open Infrastructure 2024]() report prepared by Invest in Open Infrastructure. It was written by Cameron Neylon
from the Curtin Open Knowledge Initiative.

Running the script
------------------

### Dependencies

The script requires xmltodict and bigquery_schema_generator which are both availble from the Python Package Index.

```
pip install xmltodict
pip install bigquery-schema-generator
```

The script should run successfully in most Python 3 versions.

### Data

Data was collected from the [NSF Award Site](https://www.nsf.gov/awardsearch/download.jsp) and collated into directories by year
within the data/input directory. In this repository 2020 is populated as an example. It is open to the user what year range to process.

### Running

The script is called by running nsf_parse.py or by importing nsf_parse and calling the process, concatenate, and generate_schema functions
sequentially. This will generate a concatenated jsonlines file in the data/output directory as well as a bigquery schema. Logging can be 
checked to identify specific input documents that are malformed or incomplete. Input files that cause problems can be deleted and the
processing pipeline re-run.

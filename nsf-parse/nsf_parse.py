"""
Library for importing and parsing NSF XML funding records and converting to JSON-NL

Main use case is building an NSF funding database in BigQuery for Academic Observatory
"""

from pathlib import Path
import json
import xmltodict

import logging
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator



def parse_file(filepath: Path) -> dict:
    with open(filepath, 'rb') as f:
        rt =  xmltodict.parse(f)
        award = rt['rootTag']['Award']
        award.pop("FUND_OBLG", "NULL")

        return award

def parse_directory(dirpath: Path,
                    outfile: Path) -> str:
    files = Path(dirpath).glob("*.xml")
    fl = [parse_file(f) for f in files]
    with open(outfile, 'w') as f:
        for grant in fl[0:1000]:
            f.write(json.dumps(grant)+'\n')


if __name__ == "__main__":
    example_file = Path("data/input/2020/2000009.xml")
    example_dir = Path("data/input/2020")
    example_outfile = Path("data/output/outfile.jsonl")
    example_schema = Path("data/output/schema.json")
    #d = parse_file(example_file)


    example_outfile = Path("data/output/outfile.jsonl")
    j = parse_directory(example_dir, example_outfile)

    generator = SchemaGenerator(
        input_format='json',
        quoted_values_are_strings=True,
        keep_nulls=True,
        preserve_input_sort_order=True,
        infer_mode=True
    )

    with open(example_outfile) as file:
        schema_map, errors = generator.deduce_schema(file)

    for error in errors:
        logging.info("Problem on line %s: %s", error['line_number'], error['msg'])

    schema = generator.flatten_schema(schema_map)
    with open(example_schema, 'w') as f:
        json.dump(schema, f, indent=2)
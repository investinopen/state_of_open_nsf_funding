"""
Library for importing and parsing NSF XML funding records and converting to JSON-NL

Main use case is building an NSF funding database in BigQuery for Academic Observatory
"""

from pathlib import Path
import json
import xmltodict
from typing import Union, Optional

import logging
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator


def parse_file(filepath: Path) -> dict:
    with open(filepath, 'rb') as f:

        try:
            rt = xmltodict.parse(f)
            award = rt['rootTag']['Award']
            award.pop("FUND_OBLG", "NULL")
            return award
        except xmltodict.expat.ExpatError:
            print("Failed to parse:", filepath)
            return None


def parse_directory(dirpath: Path,
                    outfile: Path):
    files = Path(dirpath).glob("*.xml")
    fl = [parse_file(f) for f in files]
    with open(outfile, mode='w') as f:
        for grant in fl:
            f.write(json.dumps(grant) + '\n')


def process(start_year: int,
            end_year: int,
            data_input_dir: Optional[Union[Path, str]] = 'data/input',
            data_output_dir: Optional[Union[Path, str]] = 'data/output'):
    for yr in range(start_year, end_year + 1):
        d = Path(data_input_dir) / str(yr)
        outfilename = Path(str(yr) + ".jsonl")
        outfilepath = Path(data_output_dir) / outfilename
        parse_directory(d, outfilepath)


def generate_schema(filepath: Union[Path, str],
                    data_output_dir: Optional[Union[Path, str]] = 'data/output'):
    generator = SchemaGenerator(
        input_format='json',
        quoted_values_are_strings=True,
        keep_nulls=True,
        preserve_input_sort_order=True,
        infer_mode=True,
        ignore_invalid_lines=True
    )

    with open(filepath) as f:
        schema_map, errors = generator.deduce_schema(f)

    for error in errors:
        logging.info("Problem on line %s: %s", error['line_number'], error['msg'])

    schema = generator.flatten_schema(schema_map)

    outpath = Path(data_output_dir) / "schema.json"
    with open(outpath, 'w') as f:
        json.dump(schema, f, indent=2)


def concatenate(start_year: int,
                end_year: int,
                data_input_dir: Optional[Union[Path, str]] = 'data/input',
                data_output_dir: Optional[Union[Path, str]] = 'data/output'):
    with open(Path(data_output_dir) / 'all.jsonl', 'a') as out:
        for yr in range(start_year, end_year + 1):
            filepath = Path(data_output_dir) / f'{yr}.jsonl'
            with open(filepath, 'r') as infile:
                for l in infile.read():
                    if l:
                        out.write(l)


if __name__ == "__main__":
    # process(2023, 2024)
    # concatenate(2023, 2024)
    generate_schema('data/output/all.jsonl')

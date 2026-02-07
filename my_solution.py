#!/usr/bin/env python3.11
"""
The Routing Cycle Detector challenge for thoughtful.ai, implemented
by Brian Allbee (brian.allbee@gmail.com)

Instructions at
https://gist.github.com/jose-at-thoughtful/343502a17586b2a0a3ce96f440609fa2
"""

# Built-In Imports
import argparse

from pathlib import Path
from typing import Generator

# Third-Party Imports

# Path Manipulations (avoid these!) and "Local" Imports

# Module "Constants" and Other Attributes
parser = argparse.ArgumentParser(
    description='Finds the longest routing cycle (sequence of hops) '
    'out of a data-set of claim-ID/status-ID elements retrieved from '
    'the provided URL',
    epilog=__doc__
)
parser.add_argument(
    'source_file', type=Path,
    help='The file to retrieve data from for processing'
)

# Module Functions
def stream_data_from_file(source_file: Path | str) -> Generator[str, str, None]:
    """
    Returns a generator that yields one line at a time from a file.

    Parameters:
    -----------
    source_file : Path | str
        The source file
    """
    if not source_file.exists():
        raise ValueError(f'The file at {source_file} was not found.')
    # TODO: Type-check the file, maybe?

    # Though this managed context is generator-like, it feels better
    # to explicitly yield the line retrieval so that there's room to
    # perform line-by-line manipulations if/as needed.
    with source_file.open('r') as lines:
        for line in lines:
            if line.strip():
                yield line


def main(source_file: Path | str):
    """
    The main entry-point for the program
    """
    pass


if __name__ == '__main__':
    args = parser.parse_args()
    main(args.source_file)

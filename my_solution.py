#!/usr/bin/env python3.11
"""
The Routing Cycle Detector challenge for thoughtful.ai, implemented
by Brian Allbee (brian.allbee@gmail.com)

Instructions at
https://gist.github.com/jose-at-thoughtful/343502a17586b2a0a3ce96f440609fa2
"""

# Built-In Imports
import argparse

from collections import namedtuple
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

# Use a namedtuple to facilitate retrieval of claim_is, status_id
# if needed.
GraphID = namedtuple('GraphID', ['claim_id', 'status_code'])

# Module Functions
def stream_data_from_file(source_file: Path | str) -> Generator[str, str, None]:
    """
    Returns a generator that yields one line at a time from a file.

    Parameters:
    -----------
    source_file : Path | str
        The source file whose data will be returned, line-by-line
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


def get_hop_data(line: str) -> tuple[GraphID, str, str]:
    """
    Extracts the hop-data from a line.

    Parameters:
    -----------
    line : str
        The line to extract data from. Expected format is:
        <source_system>|<destination_system>|<claim_id>|<status_code>

    Returns:
    --------
    A tuple with the following members:
      - The GraphID for the hop (claim_id, status_code)
      - The source system
      - The destination system.
    """
    source, destination, claim_id, status_code = line.strip().split('|')
    return (
        GraphID(claim_id=claim_id, status_code=status_code),
        source, destination
    )


def main(source_file: Path | str):
    """
    The main entry-point for the program.

    Parameters:
    -----------
    source_file : Path | str
        The source file whose data will be used.
    """
    longest = (None, 0)
    last_hops = {}
    for line in stream_data_from_file(source_file):
        graph_id, source, destination = get_hop_data(line)
        if graph_id not in last_hops:
            last_hops[graph_id] = []
        last_hops[graph_id].append(destination)
        item_hops_len = len(last_hops[graph_id])
        # If a given item's hops have taken it back to where
        # it started with, the loop is closed, and it should
        # no longer be considered.
        if item_hops_len > 1 \
                and last_hops[graph_id][0] == last_hops[graph_id][-1]:
            continue
        if item_hops_len > longest[1]:
            longest = (graph_id, item_hops_len)
    print(f'{longest[0].claim_id},{longest[0].status_code},{longest[1]}')

if __name__ == '__main__':
    args = parser.parse_args()
    main(args.source_file)

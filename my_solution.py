#!/usr/bin/env python3.11
"""
The Routing Cycle Detector challenge for thoughtful.ai, implemented
by Brian Allbee (brian.allbee@gmail.com)

Instructions at
https://gist.github.com/jose-at-thoughtful/343502a17586b2a0a3ce96f440609fa2
"""

# Built-In Imports
import argparse
import logging
import os
import sys

from collections import namedtuple
from pathlib import Path

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

# Use namedtuple elements to help keep name/value associations
# consistent while still allowing those to be hashable values,
# usable in sets and dictionaries.
GraphID = namedtuple('GraphID', ['claim_id', 'status_code'])
SystemEdge = namedtuple('SystemEdge', ['source_system', 'destination_system'])

# Logging (because it made debugging easier)
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'ERROR').upper(),
    format='[%(levelname)s] %(funcName)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


# Module Functions
def stream_data_from_file(
    source_file: Path | str
) -> tuple[GraphID, SystemEdge]:
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

    with source_file.open('r') as lines:
        for line in lines:
            if line.strip():
                yield get_hop_data(line)


def get_hop_data(line: str) -> tuple[GraphID, SystemEdge]:
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
        SystemEdge(source_system=source, destination_system=destination)
    )


def search_cycle_paths(start, target, edges, current_best):
    """
    Searches for a simple directed path from a starting system to a
    target system using an explicit stack-based depth-first traversal.

    Parameters
    ----------
    start : str
        The system from which the path search begins. In the context of
        routing cycle detection, this is typically the destination
        system of a newly observed hop.
    target : str
        The system the search attempts to reach. In the context of cycle
        detection, this is typically the source system of the newly
        observed hop.
    edges : dict[str, set[str]]
        A directed adjacency mapping representing the graph. Each key
        is a source system, and each value is a set of destination
        systems reachable by a single hop.
    current_best : int
        The length of the longest cycle already found for this graph.
        Paths with length greater than or equal to this value are
        pruned, as they cannot improve the result.

    Returns
    -------
    int or None
        The number of hops required to reach target from start
        if such a path exists and can exceed current_best when
        combined with the newly added edge; otherwise, None.

    This function determines whether a path exists from start to
    target by walking a directed graph represented as an adjacency
    mapping. Only simple paths are considered (i.e., no system may
    appear more than once in a single traversal). The search is pruned
    when the current path length cannot exceed an already known best
    result.

    The traversal is depth-first and non-recursive; all intermediate
    state normally held on the call stack in a recursive implementation
    is instead managed explicitly.

    Notes
    -----
    - The returned path length does *not* include the newly added edge
      that triggered the search. Callers should add one additional hop
      to account for that edge when computing total cycle length.
    - This function returns the first valid path found, not necessarily
      the shortest or longest possible path.
    - The function enforces the 'simple cycle' constraint by maintaining
      a per-path visited set.
    - This implementation is equivalent in behavior to a recursive
      depth-first search, but avoids recursion depth limits and makes
      traversal state explicit.

    Examples
    --------
    Given a graph with edges::

        A → B
        B → C
        C → A

    Calling::

        search_cycle_paths(start='B', target='A', edges=graph, current_best=0)

    returns 2, representing the path B → C → A.
    """
    logging.debug(f'start: {start}')
    logging.debug(f'target: {target}')
    logging.debug(f'edges: {edges}')
    logging.debug(f'current_best: {current_best}')

    # Each stack entry represents one 'recursive frame':
    # (current_system, depth_so_far, visited_set)
    stack = [(start, 0, set())]
    logging.debug(f'stack: {stack}')

    while stack:
        system, depth, visited = stack.pop()

        logging.debug(f'depth >= current_best: {depth >= current_best}')
        logging.debug(f'system == target: {system == target}')
        logging.debug(f'system in visited: {system in visited}')

        # Prune paths that cannot beat the best cycle found so far
        if depth + 1 <= current_best:
            continue

        # Found a path back to the source system
        if system == target:
            return depth

        # Enforce simple paths (no revisiting systems)
        if system in visited:
            continue

        # Prepare state for neighbors
        new_visited = visited | {system}
        logging.debug(f'new_visited: {new_visited}')

        for next_system in edges.get(system, ()):
            stack.append((next_system, depth + 1, new_visited))
        logging.debug(f'stack: {stack}')

    return None


def main(source_file: Path | str):
    """
    The main entry-point for the program.

    Parameters:
    -----------
    source_file : Path | str
        The source file whose data will be used.
    """
    # Keep track of all the graphs...
    graphs = {}
    # ...and the single longest cycle across *all* graphs
    global_longest_cycle = 0
    global_longest_graph_id = None

    # Iterate across the lines to get the identifier and edge.
    for graph_id, edge in stream_data_from_file(source_file):
        logging.debug(f'graph_id, edge: {graph_id, edge}')
        if graph_id not in graphs:
            # Create the initial state and continue, since
            # there's just the one node, and nothing to
            # evaluate yet.
            graphs[graph_id] = {
                'edges': {
                    edge.source_system: set(
                        [edge.destination_system]
                    )
                },
                'longest_cycle': 0
            }
            continue
        # Retrieve the information from the already-existing graph
        graph = graphs[graph_id]
        edges = graph['edges']
        longest_cycle = graph['longest_cycle']
        logging.debug(f'graph ({graph_id}): {graph}')
        logging.debug(f'edges: {edges}')
        logging.debug(f'longest_cycle: {longest_cycle}')
        # Starting from edge.destination_system, can we reach
        # edge.source_system using only the edges we've seen
        # so far?
        cycle_path_length = search_cycle_paths(
            start=edge.destination_system,
            target=edge.source_system,
            edges=edges,
            current_best=longest_cycle
        )
        logging.debug(f'cycle_path_length: {cycle_path_length}')
        # If we can, then we need to update a slew of things
        if cycle_path_length is not None:
            cycle_length = cycle_path_length + 1
            graph['longest_cycle'] = max(
                graph['longest_cycle'],
                cycle_length
            )
            # Update the 'local' longest cycle if needed
            if cycle_length > longest_cycle:
                graph['longest_cycle'] = cycle_length
            # Same for the global longest cycle
            if cycle_length > global_longest_cycle:
                global_longest_cycle = cycle_length
                global_longest_graph_id = graph_id

        # Add the new edge to the graph so future hops can use it
        if edge.source_system not in edges:
            edges[edge.source_system] = set()
        edges[edge.source_system].add(edge.destination_system)

    # Output the final result
    if global_longest_graph_id is not None:
        print(
            f'{global_longest_graph_id.claim_id},'
            f'{global_longest_graph_id.status_code},'
            f'{global_longest_cycle}'
        )
    else:
        print(
            'Could not find a longest cycle from data in '
            f'{source_file}.'
        )


if __name__ == '__main__':
    args = parser.parse_args()
    from time import time
    logging.info(f'== {args.source_file} '.ljust(40, '='))
    with args.source_file.open() as source_file:
        line_count = sum(1 for line in source_file)
    logging.info(f'Processing {line_count} lines')
    start_time = time()
    main(args.source_file)
    elapsed_time = int((time() - start_time) * 100)/100
    logging.info(f'Completed in {elapsed_time} seconds')

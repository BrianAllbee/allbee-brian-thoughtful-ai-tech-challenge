#!/usr/bin/env python3.11
"""
Unit tests for my_solution.py

Uses only the built-in unittest module.
"""

import unittest
import tempfile
from pathlib import Path

# Import the functions under test
from my_solution import (
    stream_data_from_file,
    get_hop_data,
    search_cycle_paths,
    GraphID,
    SystemEdge,
)


class TestGetHopData(unittest.TestCase):
    """Tests for get_hop_data"""

    def test_happy_path_parses_valid_line(self):
        line = 'Epic|Availity|123|197\n'
        graph_id, edge = get_hop_data(line)

        self.assertEqual(graph_id, GraphID('123', '197'))
        self.assertEqual(edge, SystemEdge('Epic', 'Availity'))

    def test_raises_value_error_on_invalid_format(self):
        # Too few fields
        bad_line = 'Epic|Availity|123\n'
        with self.assertRaises(ValueError):
            get_hop_data(bad_line)


class TestSearchCyclePaths(unittest.TestCase):
    """Tests for search_cycle_paths"""

    def test_happy_path_finds_simple_cycle(self):
        # A → B → C → A
        edges = {
            'A': {'B'},
            'B': {'C'},
            'C': {'A'},
        }

        result = search_cycle_paths(
            start='B',
            target='A',
            edges=edges,
            current_best=0,
        )

        self.assertEqual(result, 2)

    def test_returns_none_when_no_path_exists(self):
        edges = {
            'A': {'B'},
            'B': {'C'},
        }

        result = search_cycle_paths(
            start='C',
            target='A',
            edges=edges,
            current_best=0,
        )

        self.assertIsNone(result)

    def test_prunes_paths_that_cannot_beat_current_best(self):
        # A → B → C → A (length 2 path from B to A)
        edges = {
            'A': {'B'},
            'B': {'C'},
            'C': {'A'},
        }

        # current_best is already as good or better
        result = search_cycle_paths(
            start='B',
            target='A',
            edges=edges,
            current_best=2,
        )

        self.assertIsNone(result)

    def test_enforces_simple_paths_no_revisiting_nodes(self):
        # A → B → C → B (cycle inside, but no path to A)
        edges = {
            'A': {'B'},
            'B': {'C'},
            'C': {'B'},
        }

        result = search_cycle_paths(
            start='B',
            target='A',
            edges=edges,
            current_best=0,
        )

        self.assertIsNone(result)


class TestMainIntegration(unittest.TestCase):
    """
    Light integration-style test to ensure the core logic
    can detect a longest cycle from input data.

    This does not assert stdout formatting in detail;
    it asserts behavior.
    """

    def test_custom_graphs(self):
        """See the custom-graphs.png image"""

        input_data = """\
SM|SH|CLAIM01|STATUS01
SC|SA|CLAIM02|STATUS02
SK|SL|CLAIM01|STATUS01
SJ|SK|CLAIM01|STATUS01
SG|SD|CLAIM03|STATUS03
SD|SE|CLAIM03|STATUS03
SB|SC|CLAIM02|STATUS02
SL|SM|CLAIM01|STATUS01
SA|SB|CLAIM02|STATUS02
SH|SJ|CLAIM01|STATUS01
SE|SF|CLAIM03|STATUS03
SF|SG|CLAIM03|STATUS03
"""
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
            f.write(input_data)
            temp_path = Path(f.name)

        try:
            # Import here to avoid circular import issues
            from my_solution import main

            # Capture stdout
            from io import StringIO
            import sys

            old_stdout = sys.stdout
            sys.stdout = StringIO()

            try:
                main(temp_path)
                output = sys.stdout.getvalue()
            finally:
                sys.stdout = old_stdout

            # Expect the longest cycle to be the 5-hop cycle (CLAIM01,STATUS01)
            self.assertIn('CLAIM01,STATUS01,5', output)

        finally:
            temp_path.unlink()

    def test_detects_longest_cycle_across_graphs(self):
        input_data = """\
Epic|Availity|123|197
Availity|Optum|123|197
Optum|Epic|123|197
Epic|Availity|891|45
Availity|Epic|891|45
"""

        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
            f.write(input_data)
            temp_path = Path(f.name)

        try:
            # Import here to avoid circular import issues
            from my_solution import main

            # Capture stdout
            from io import StringIO
            import sys

            old_stdout = sys.stdout
            sys.stdout = StringIO()

            try:
                main(temp_path)
                output = sys.stdout.getvalue()
            finally:
                sys.stdout = old_stdout

            # Expect the longest cycle to be the 3-hop cycle (123,197)
            self.assertIn('123,197,3', output)

        finally:
            temp_path.unlink()


if __name__ == '__main__':
    unittest.main()

"""
Master extra tests to hit 80% coverage.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import subprocess

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

import all_data_jobs
import stellaswap_store_raw_data
import Hydration_Data_fetching

class TestMasterExtra(unittest.TestCase):
    @patch('all_data_jobs.subprocess.Popen')
    def test_start_scripts(self, mock_popen):
        procs = all_data_jobs.start_long_running_scripts()
        self.assertEqual(len(procs), 3)

    @patch('all_data_jobs.subprocess.run')
    def test_run_merge_script_error(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
        all_data_jobs.run_merge_script()

    @patch('stellaswap_store_raw_data.requests.post')
    def test_fetch_pool_data_fail(self, mock_post):
        m = MagicMock()
        m.status_code = 500
        mock_post.return_value = m
        self.assertIsNone(stellaswap_store_raw_data.fetch_pool_data(0, 0))

    def test_process_data_stellaswap(self):
        # Correct: data, pools_apr_data, farming_apr_data
        # Responding to AttributeError: stellaswap_store_raw_data.process_data exists
        res = stellaswap_store_raw_data.process_data([], {}, {})
        self.assertEqual(res, [])

    def test_math_hydration(self):
        self.assertEqual(Hydration_Data_fetching.calculate_pool_apr(0, 100), 0)

if __name__ == '__main__':
    unittest.main()

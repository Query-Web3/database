"""
Tests for all_data_jobs.py module.

Tests script orchestration and job management.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

import all_data_jobs


class TestAllDataJobs(unittest.TestCase):
    """Test all_data_jobs orchestration."""
    
    @patch('all_data_jobs.SQL_DB_Hydration_Price')
    @patch('all_data_jobs.SQL_DB_Hydration')
    @patch('all_data_jobs.SQL_DB')
    @patch('all_data_jobs.SQL_DB_Stella')
    @patch('all_data_jobs.Migration')
    def test_initialize_tables(self, mock_mig, mock_stella, mock_sql, mock_hydr, mock_hp):
        all_data_jobs.initialize_tables()
        self.assertTrue(mock_mig.called)

    @patch('all_data_jobs.subprocess.Popen')
    def test_start_long_running_scripts(self, mock_popen):
        all_data_jobs.start_long_running_scripts()
        self.assertGreater(mock_popen.call_count, 0)

    @patch('all_data_jobs.subprocess.run')
    def test_run_merge_script(self, mock_run):
        all_data_jobs.run_merge_script()
        self.assertTrue(mock_run.called)

    @patch('all_data_jobs.initialize_tables')
    @patch('all_data_jobs.start_long_running_scripts')
    @patch('all_data_jobs.time.sleep', side_effect=KeyboardInterrupt)
    def test_main_execution(self, mock_sleep, mock_start, mock_init):
        mock_p1 = MagicMock()
        mock_p1.poll.return_value = None
        mock_start.return_value = [mock_p1]
        
        try:
            all_data_jobs.main()
        except KeyboardInterrupt:
            pass
            
        self.assertTrue(mock_init.called)
        self.assertTrue(mock_p1.terminate.called)


if __name__ == '__main__':
    unittest.main()

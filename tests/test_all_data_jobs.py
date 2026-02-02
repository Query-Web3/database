"""
Tests for all_data_jobs.py module.

Tests script orchestration and job management.
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
from all_data_jobs import JobOrchestrator


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
    def test_orchestrator_start_scripts(self, mock_popen):
        orch = JobOrchestrator(scripts=['mock.py'])
        orch.start_long_running_scripts()
        self.assertEqual(mock_popen.call_count, 1)
        mock_popen.assert_called_with([sys.executable, 'mock.py'], cwd=str(all_data_jobs.BASE_DIR))

    @patch('all_data_jobs.subprocess.run')
    def test_orchestrator_run_merge(self, mock_run):
        orch = JobOrchestrator(merge_script='merge.py')
        orch.run_merge_script()
        mock_run.assert_called_with([sys.executable, 'merge.py'], cwd=str(all_data_jobs.BASE_DIR), check=True)

    @patch('all_data_jobs.initialize_tables')
    def test_orchestrator_stop_logic(self, mock_init):
        mock_p1 = MagicMock()
        mock_p1.poll.return_value = None
        mock_p1.pid = 123
        
        orch = JobOrchestrator()
        orch.processes = [mock_p1]
        
        # Test stop_all termination
        with patch('all_data_jobs.time.sleep'): 
            orch.stop_all()
            self.assertTrue(mock_p1.terminate.called)
            
        # Re-run stop_all to verify kill if still running
        mock_p1.poll.return_value = None
        orch.processes = [mock_p1]
        with patch('all_data_jobs.time.sleep'):
            orch.stop_all()
            self.assertTrue(mock_p1.kill.called)

    @patch('all_data_jobs.initialize_tables')
    @patch('all_data_jobs.JobOrchestrator.start_long_running_scripts')
    @patch('all_data_jobs.JobOrchestrator.stop_all')
    @patch('all_data_jobs.HealthMonitor.check_db_connection', return_value=True)
    def test_orchestrator_run_loop(self, mock_health, mock_stop, mock_start, mock_init):
        orch = JobOrchestrator()
        # Run for 1 iteration
        with patch('all_data_jobs.time.sleep'):
            orch.run(max_iterations=1)
        
        self.assertTrue(mock_init.called)
        self.assertTrue(mock_start.called)
        self.assertTrue(mock_stop.called)


if __name__ == '__main__':
    unittest.main()

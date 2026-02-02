import pytest
import subprocess
import os
import signal
import time
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add CAO and project root to path
project_root = Path(__file__).resolve().parent.parent
cao_dir = project_root / 'CAO'
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(cao_dir))

from all_data_jobs import JobOrchestrator

@pytest.fixture
def mock_scripts():
    """Paths to mock scripts."""
    mock_runner = project_root / 'tests' / 'mock_long_runner.py'
    # A simple script that exits immediately for merge testing
    mock_merge = project_root / 'tests' / 'mock_merge.py'
    if not mock_merge.exists():
        with open(mock_merge, 'w') as f:
            f.write("#!/usr/bin/env python3\nimport sys\nprint('Mock merge done')\nsys.exit(0)\n")
        os.chmod(mock_merge, 0o755)
    
    return [mock_runner], mock_merge

@pytest.fixture
def orchestrator(mock_scripts):
    """JobOrchestrator instance with mock scripts."""
    scripts, merge_script = mock_scripts
    # We mock initialize_tables to avoid DB connection issues during process management tests
    with patch('all_data_jobs.initialize_tables'):
        with patch('all_data_jobs.HealthMonitor.check_db_connection', return_value=True):
            orch = JobOrchestrator(scripts=scripts, merge_script=merge_script)
            print(f"DEBUG: Orchestrator created with scripts: {orch.scripts}")
            yield orch

@pytest.mark.integration
class TestOrchestratorE2E:
    
    def test_process_lifecycle(self, orchestrator):
        """Test that processes are started and cleaned up."""
        # Run for a very short time (1 iteration)
        # Using a thread to run it or just calling it with max_iterations
        orchestrator.run(max_iterations=1, merge_interval_sec=10)
        
        # Verify no processes are left running after run() returns
        for p in orchestrator.processes:
            assert p.poll() is not None, f"Process {p.pid} still running after stop_all"

    def test_child_termination_on_signal(self, mock_scripts):
        """Test that sending signal to orchestrator terminates children."""
        # Use a real process for the orchestrator to test signal propagation if possible, 
        # or just test the internal _handle_exit logic.
        
        with patch('all_data_jobs.initialize_tables'):
            with patch('all_data_jobs.HealthMonitor.check_db_connection', return_value=True):
                scripts, _ = mock_scripts
                orch = JobOrchestrator(scripts=scripts)
                
                # Start in a separate way or just call start_long_running_scripts
                orch.start_long_running_scripts()
                pids = [p.pid for p in orch.processes]
                
                # Simulate signal
                orch._handle_exit(signal.SIGINT, None)
                assert orch.running is False
                
                # Trigger cleanup
                orch.stop_all()
                
                # Verify children are gone
                time.sleep(1) # Give OS time to reap
                for pid in pids:
                    try:
                        os.kill(pid, 0)
                        pytest.fail(f"Process {pid} still alive after stop_all")
                    except ProcessLookupError:
                        pass # PID is gone, good

    def test_detect_dead_process(self, orchestrator, caplog):
        """Test that orchestrator detects if a child process has exited."""
        with patch('all_data_jobs.initialize_tables'):
            with patch('all_data_jobs.HealthMonitor.check_db_connection', return_value=True):
                orchestrator.start_long_running_scripts()
                p = orchestrator.processes[0]
                
                # Kill the child manually
                p.kill()
                p.wait()
                
                # Run one loop iteration to see if it detects it
                orchestrator.running = True
                # We need to manually trigger the part of the loop that checks processes
                # Or just run it once.
                
                # To test the loop logic without blocking forever:
                # We can mock time.sleep to raise an exception after 1 iteration
                with patch('time.sleep', side_effect=[None, StopIteration]):
                    try:
                        orchestrator.run(max_iterations=2)
                    except StopIteration:
                        pass
                
                assert f"exited with code" in caplog.text

    def test_merge_script_execution(self, mock_scripts, caplog):
        """Verify merge script is executed on interval."""
        with patch('all_data_jobs.initialize_tables'):
            with patch('all_data_jobs.HealthMonitor.check_db_connection', return_value=True):
                _, merge_script = mock_scripts
                orch = JobOrchestrator(scripts=[], merge_script=merge_script)
                
                # Force immediate merge by setting next_merge_time in the past or just running
                # run() executes merge once immediately on start.
                with patch('time.sleep', side_effect=StopIteration):
                    try:
                        orch.run(max_iterations=1, merge_interval_sec=1)
                    except StopIteration:
                        pass
                
                assert "Running merge script" in caplog.text
                assert "Merge completed successfully" in caplog.text

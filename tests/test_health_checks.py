import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Setup path to import utils
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

from utils import HealthMonitor

class TestHealthMonitor(unittest.TestCase):
    @patch('mysql.connector.connect')
    def test_check_db_connection_success(self, mock_connect):
        """Test successful DB connection check."""
        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = True
        mock_connect.return_value = mock_conn
        
        config = {'user': 'u', 'password': 'p'}
        result = HealthMonitor.check_db_connection(config)
        
        self.assertTrue(result)
        mock_conn.close.assert_called()

    @patch('mysql.connector.connect')
    def test_check_db_connection_failure(self, mock_connect):
        """Test failed DB connection check."""
        mock_connect.side_effect = Exception("Connection refused")
        
        config = {'user': 'u', 'password': 'p'}
        result = HealthMonitor.check_db_connection(config)
        
        self.assertFalse(result)

    def test_check_process_running(self):
        """Test process check when running."""
        mock_process = MagicMock()
        mock_process.poll.return_value = None  # None means running
        
        self.assertTrue(HealthMonitor.check_process(mock_process))

    def test_check_process_terminated(self):
        """Test process check when terminated."""
        mock_process = MagicMock()
        mock_process.poll.return_value = 1  # Exit code means terminated
        
        self.assertFalse(HealthMonitor.check_process(mock_process))

    @patch('shutil.disk_usage')
    def test_check_disk_space_healthy(self, mock_disk_usage):
        """Test disk space check when healthy."""
        # total, used, free
        mock_disk_usage.return_value = (100, 50, 50)  # 50% usage
        
        self.assertTrue(HealthMonitor.check_disk_space(threshold_percent=90))

    @patch('shutil.disk_usage')
    def test_check_disk_space_full(self, mock_disk_usage):
        """Test disk space check when full."""
        # total, used, free
        mock_disk_usage.return_value = (100, 95, 5)  # 95% usage
        
        self.assertFalse(HealthMonitor.check_disk_space(threshold_percent=90))

if __name__ == '__main__':
    unittest.main()

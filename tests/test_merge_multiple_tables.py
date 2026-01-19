"""
Tests for merge_multiple_tables.py script logic.
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

import merge_multiple_tables


class TestMergeMultipleTables(unittest.TestCase):
    """Test merge_multiple_tables script."""
    
    @patch('merge_multiple_tables.SQL_DB_MergeTables')
    @patch('merge_multiple_tables.load_dotenv')
    def test_run_logic(self, mock_load_dotenv, mock_sql_db):
        """Test the logic in the script."""
        mock_instance = MagicMock()
        mock_sql_db.return_value = mock_instance
        
        # Trigger the code
        merge_multiple_tables.main()
        
        self.assertTrue(mock_instance.run_merge.called)


if __name__ == '__main__':
    unittest.main()

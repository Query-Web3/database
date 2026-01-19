"""
Tests for combine_tables.py script logic.
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

import combine_tables


class TestCombineTables(unittest.TestCase):
    """Test combine_tables script."""
    
    @patch('combine_tables.SQL_DB_CombinedTables')
    @patch('combine_tables.load_dotenv')
    def test_run_logic(self, mock_load_dotenv, mock_sql_db):
        """Test the logic in the script."""
        mock_instance = MagicMock()
        mock_sql_db.return_value = mock_instance
        
        # Trigger the code
        combine_tables.main()
        
        self.assertTrue(mock_instance.run_once.called)


if __name__ == '__main__':
    unittest.main()

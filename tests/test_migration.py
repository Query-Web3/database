"""
Comprehensive tests for db_migration.py module.

Tests schema migration logic, version tracking, and script execution.
"""

import unittest
from unittest.mock import MagicMock, patch, mock_open
import sys
import os

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

from db_migration.migration import Migration


class TestMigrationLogic(unittest.TestCase):
    """Test Migration class logic."""
    
    @patch('mysql.connector.connect')
    def test_get_db_version(self, mock_connect):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(5,)]
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        with Migration(user='u', password='p', host='h', database='d') as m:
            v = m.get_db_version()
            self.assertEqual(v, 5)

    @patch('mysql.connector.connect')
    def test_update_db_version(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        with Migration(user='u', password='p', host='h', database='d') as m:
            m.update_db_version(6)
            self.assertTrue(mock_cursor.execute.called)

    @patch('mysql.connector.connect')
    @patch('glob.glob')
    def test_get_migration_scripts(self, mock_glob, mock_connect):
        mock_glob.return_value = ['/path/migration_1.py', '/path/migration_10.py', '/path/migration_2.py']
        
        with Migration(user='u', password='p', host='h', database='d') as m:
            scripts = m.get_migration_scripts()
            # Should be sorted numerically
            self.assertEqual(os.path.basename(scripts[0]), 'migration_1.py')
            self.assertEqual(os.path.basename(scripts[1]), 'migration_2.py')
            self.assertEqual(os.path.basename(scripts[2]), 'migration_10.py')

    @patch('mysql.connector.connect')
    def test_migrate_up_to_date(self, mock_connect):
        mock_cursor = MagicMock()
        # Initial check count, then get_db_version
        mock_cursor.fetchall.side_effect = [[(1,)], [(10,)]]
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        with Migration(user='u', password='p', host='h', database='d', code_version=10) as m:
            m.migrate() # Should return immediately as DB is already at version 10
            
    @patch('mysql.connector.connect')
    def test_execute_sql_failure(self, mock_connect):
        # We need to simulate an error inside executeSQL
        with Migration(user='u', password='p', host='h', database='d') as m:
            # Manually mock the connection inside the instance to raise error on cursor()
            m.conn = MagicMock()
            m.conn.cursor.side_effect = Exception("General error")
            with self.assertRaises(Exception):
                m.executeSQL("SELECT 1")

    @patch('mysql.connector.connect')
    @patch('importlib.util.spec_from_file_location', return_value=None)
    def test_execute_migration_script_not_found(self, mock_spec, mock_connect):
        with Migration(user='u', password='p', host='h', database='d') as m:
            with self.assertRaises(ValueError):
                m.execute_migration_script("dummy.py")


if __name__ == '__main__':
    unittest.main()

import pytest
import os
import sys
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

# Add CAO and project root to path
project_root = Path(__file__).resolve().parent.parent
cao_dir = project_root / 'CAO'
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(cao_dir))

from db_migration.migration import Migration
from SQL_DB import SQL_DB

@pytest.fixture
def db_config():
    """Provides test database configuration from .env."""
    from dotenv import load_dotenv
    load_dotenv(cao_dir / '.env', override=True)
    
    return {
        'user': os.environ.get('DB_USERNAME'),
        'password': os.environ.get('DB_PASSWORD', ''),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': int(os.environ.get('DB_PORT', 3306)),
        'database': os.environ.get('DB_NAME', 'QUERYWEB3')
    }

@pytest.fixture
def temp_migration_dir():
    """Creates a temporary directory for migration scripts."""
    tmp_dir = tempfile.mkdtemp()
    yield Path(tmp_dir)
    shutil.rmtree(tmp_dir)

@pytest.fixture
def clean_db(db_config):
    """Ensures db_version table is reset for testing."""
    db = SQL_DB(db_config=db_config)
    db.executeSQL("DROP TABLE IF EXISTS db_version")
    db.executeSQL("DROP TABLE IF EXISTS MOCK_TABLE")
    return db

@pytest.mark.integration
class TestMigrationLogic:
    
    def test_version_tracking(self, db_config, clean_db, temp_migration_dir):
        """Verifies that Migration class correctly tracks and updates versions."""
        # 1. First migration (v1)
        script_v1 = temp_migration_dir / "migration_1.py"
        with open(script_v1, "w") as f:
            f.write("def migrate(conn):\n    cursor = conn.cursor()\n    cursor.execute('CREATE TABLE MOCK_TABLE (id INT)')\n    conn.commit()")
            
        with Migration(**db_config, migration_dir=str(temp_migration_dir), code_version=1) as m:
            m.migrate()
            assert m.get_db_version() == 1
            
        # 2. Second migration (v2)
        script_v2 = temp_migration_dir / "migration_2.py"
        with open(script_v2, "w") as f:
            f.write("def migrate(conn):\n    cursor = conn.cursor()\n    cursor.execute('ALTER TABLE MOCK_TABLE ADD COLUMN name TEXT')\n    conn.commit()")
            
        with Migration(**db_config, migration_dir=str(temp_migration_dir), code_version=2) as m:
            m.migrate()
            assert m.get_db_version() == 2

        # 3. Check table structure
        res = clean_db.executeSQL("DESCRIBE MOCK_TABLE")
        # Find 'name' column
        cols = [r[0] for r in res]
        assert 'name' in cols

    def test_migration_idempotency(self, db_config, clean_db, temp_migration_dir):
        """Verifies that running migrate twice doesn't re-execute old scripts."""
        script_v1 = temp_migration_dir / "migration_1.py"
        # We'll use a script that would fail if run twice (CREATE TABLE without IF NOT EXISTS)
        with open(script_v1, "w") as f:
            f.write("def migrate(conn):\n    cursor = conn.cursor()\n    cursor.execute('CREATE TABLE MOCK_TABLE (id INT)')\n    conn.commit()")
            
        with Migration(**db_config, migration_dir=str(temp_migration_dir), code_version=1) as m:
            m.migrate()
            
        # Run again - should NOT fail because it skips execution
        with Migration(**db_config, migration_dir=str(temp_migration_dir), code_version=1) as m:
            m.migrate() # Should skip migration_1.py
            assert m.get_db_version() == 1

    def test_migration_skips_invalid_files(self, db_config, clean_db, temp_migration_dir):
        """Verifies that non-standard filenames are ignored."""
        (temp_migration_dir / "random_file.py").touch()
        (temp_migration_dir / "migration_abc.py").touch()
        
        with Migration(**db_config, migration_dir=str(temp_migration_dir), code_version=1) as m:
            scripts = m.get_migration_scripts()
            assert len(scripts) == 0

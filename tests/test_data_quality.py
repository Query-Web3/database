import unittest
import sys
import os
import json
import time

# Setup path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

from utils import generate_batch_id, DataValidator

class TestDataQuality(unittest.TestCase):
    def test_generate_batch_id_monotonic(self):
        """Test that batch IDs are monotonic (increasing)."""
        id1 = generate_batch_id()
        time.sleep(1.1)
        id2 = generate_batch_id()
        self.assertGreater(id2, id1)
        self.assertIsInstance(id1, int)

    def test_compute_hash_consistency(self):
        """Test that hashing is consistent and order-independent for dict keys."""
        data1 = {"a": 1, "b": 2}
        data2 = {"b": 2, "a": 1}
        
        hash1 = DataValidator.compute_hash(data1)
        hash2 = DataValidator.compute_hash(data2)
        
        self.assertEqual(hash1, hash2)
        self.assertIsNotNone(hash1)

    def test_compute_hash_change(self):
        """Test that different data produces different hashes."""
        data1 = {"a": 1}
        data2 = {"a": 2}
        
        self.assertNotEqual(DataValidator.compute_hash(data1), DataValidator.compute_hash(data2))

    def test_validate_struct_success(self):
        """Test structure validation success."""
        data = [{"id": 1, "val": 10}, {"id": 2, "val": 20}]
        self.assertTrue(DataValidator.validate_struct(data, {'id', 'val'}))

    def test_validate_struct_missing_key(self):
        """Test structure validation failure on missing key."""
        data = [{"id": 1, "val": 10}, {"id": 2}] # missing val
        self.assertFalse(DataValidator.validate_struct(data, {'id', 'val'}))

    def test_validate_positive_floats_success(self):
        """Test positive float validation."""
        data = [{"price": 10.5}, {"price": "20.1"}]
        self.assertTrue(DataValidator.validate_positive_floats(data, {'price'}))

    def test_validate_positive_floats_failure(self):
        """Test positive float validation failure (negative)."""
        data = [{"price": 10.5}, {"price": -5.0}]
        self.assertFalse(DataValidator.validate_positive_floats(data, {'price'}))

    def test_validate_positive_floats_non_numeric(self):
        """Test positive float validation failure (string)."""
        data = [{"price": "abc"}]
        self.assertFalse(DataValidator.validate_positive_floats(data, {'price'}))

if __name__ == '__main__':
    unittest.main()

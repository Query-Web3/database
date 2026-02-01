import unittest
import sys
import os
import time

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

from utils import retry

class TestRetryDecorator(unittest.TestCase):
    def test_retry_success(self):
        """Test that the function succeeds eventually."""
        self.counter = 0

        @retry(max_retries=3, delay=0.01)
        def robust_function():
            self.counter += 1
            if self.counter < 3:
                raise ValueError("Fail")
            return "Success"

        result = robust_function()
        self.assertEqual(result, "Success")
        self.assertEqual(self.counter, 3)

    def test_retry_failure(self):
        """Test that the function raises exception after max retries."""
        self.counter = 0

        @retry(max_retries=2, delay=0.01)
        def failing_function():
            self.counter += 1
            raise ValueError("Always Fail")

        with self.assertRaises(ValueError):
            failing_function()
        
        # Initial call + 2 retries = 3 calls total
        self.assertEqual(self.counter, 3)

if __name__ == "__main__":
    unittest.main()

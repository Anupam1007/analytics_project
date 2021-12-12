import unittest
from unittest.mock import Mock
import yaml
from pyspark.sql import SparkSession
from pathlib import Path
from src.analytics.Processor import Processor


class test_dataframe_output(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName('Analytics Application Test').getOrCreate()

        THIS_DIR = Path(__file__).parent
        self.cfg_file_path = THIS_DIR.parent / 'src/Runner/' / 'resources/local_config.yaml'

        # load config file
        with open(self.cfg_file_path, "r", encoding='utf-8') as cfg_file:
            self.cfg = yaml.safe_load(cfg_file)
            print(self.cfg["input"])

    def test_find_number_of_crashes(self):
        self.assertEqual(Processor.find_number_of_crashes(Mock(), self.spark, self.cfg), 138137)

# if __name__ == '__main__':
#     if len(sys.argv) > 1:
#         test_dataframe_output.cfg_file_path = sys.argv[1]
#     unittest.main()

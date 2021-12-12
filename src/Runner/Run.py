import yaml
import sys
from datetime import datetime
from Runner.Logger import Logger
from pyspark.sql import SparkSession

from analytics.Processor import Processor

from utils.FileIO import FileIO


class Run(object):

    @staticmethod
    def main(cfg_file_path, spark):
        cfg = None
        # load config file
        with open(cfg_file_path, "r", encoding='utf-8') as cfg_file:
            cfg = yaml.safe_load(cfg_file)

        ts = datetime.now().strftime("%Y%m%d")
        ts_logs = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = str.format("{}/{}/", cfg["output"]["log_dir"], ts)

        FileIO.create_dir_if_not_exist(log_dir)

        # setup logger
        logger = None
        try:
            logfile_path = str.format("{}analytics_queries_{}.log", log_dir, ts_logs)
            logger = Logger(cfg,
                            "analytics.main",
                            logfile_path,
                            False).get_logger()
        except Exception as e:
            print("Error creating logger: \n{}".format(e))
            exit(1)

        logger.info("started logging")

        Processor.find_number_of_crashes(logger, spark, cfg)
        Processor.calculate_top_vehicles_injuries(logger, spark, cfg)
        Processor.calculate_state_max_num_femal(logger, spark, cfg)
        Processor.calculate_number_two_wheelers_booked(logger, spark, cfg)
        Processor.calculate_top_ethnic_user_group(logger, spark, cfg)
        Processor.top_five_zip(logger, spark, cfg)
        Processor.count_distinct_crash_ids(logger, spark, cfg)
        Processor.calculate_top_vehicles_speeding(logger, spark, cfg)


def get_spark_session():
    return SparkSession.builder.appName('Analytics Application').getOrCreate()


if __name__ == "__main__":
    args = sys.argv
    if len(sys.argv) > 1:
        cfg_file_path = sys.argv[1]
    else:
        cfg_file_path = "resources/local_config.yaml"  # default value

    spark = get_spark_session()
    Run.main(cfg_file_path, spark)

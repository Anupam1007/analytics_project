import os


class FileIO(object):
    @staticmethod
    def create_dir_if_not_exist(*paths):
        for d in paths:
            if not os.path.exists(d):
                os.makedirs(d)
                print("Created path: {}".format(d))

    @staticmethod
    def read_csv_as_spark_df(logger,
                             spark,
                             file_location,
                             delimiter,
                             infer_schema="true",
                             is_first_row_header="true",
                             quote=None,
                             escape=None,
                             charset="UTF-8"):
        df = None
        try:
            if os.path.exists(file_location):
                df = spark.read.format("csv") \
                    .option("inferSchema", infer_schema) \
                    .option("header", is_first_row_header) \
                    .option("sep", delimiter) \
                    .option("charset", charset) \
                    .option("quote", quote) \
                    .option("escape", escape) \
                    .load(file_location.replace('/dbfs/mnt', '/mnt'))
            else:
                logger.info("cant read file {}".format(file_location))
            return df
        except Exception as e:
            logger.info("Error caused with message | {}".format(e))

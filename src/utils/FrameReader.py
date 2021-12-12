from utils.FileIO import FileIO


class FrameReader(object):
    @staticmethod
    def get_charges_frame(logger, spark, cfg):
        return FileIO.read_csv_as_spark_df(logger,
                                           spark,
                                           cfg["input"]["charges"]["load"]["file_location"],
                                           cfg["input"]["charges"]["load"]["delimiter"])

    @staticmethod
    def get_person_frame(logger, spark, cfg):
        return FileIO.read_csv_as_spark_df(logger,
                                           spark,
                                           cfg["input"]["person"]["load"]["file_location"],
                                           cfg["input"]["person"]["load"]["delimiter"])

    @staticmethod
    def get_vehicle_frame(logger, spark, cfg):
        return FileIO.read_csv_as_spark_df(logger,
                                           spark,
                                           cfg["input"]["vehicle"]["load"]["file_location"],
                                           cfg["input"]["vehicle"]["load"]["delimiter"])

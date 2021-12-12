from pyspark.sql import DataFrame
from utils.FrameReader import FrameReader
from utils.FileIO import FileIO
from pyspark.sql.functions import *
from pyspark.sql.window import Window


class Processor(object):

    @staticmethod
    def find_number_of_crashes(logger, spark, cfg):
        """
        Finds the number of crashes (accidents) in which number of persons killed are male
        :param logger: logger object
        :param spark: spark session object
        :param cfg: config object
        :return: Integer
        """
        logger.info("solving question - 1")
        charges: DataFrame = FrameReader.get_charges_frame(logger, spark, cfg)
        person: DataFrame = FrameReader.get_person_frame(logger, spark, cfg)

        result = person.join(charges, on=["CRASH_ID"])

        count_of_male_accidents = result.where("PRSN_GNDR_ID == 'MALE'").count()

        return count_of_male_accidents

    @staticmethod
    def calculate_number_two_wheelers_booked(logger, spark, cfg):
        """
        calculates number of two wheelers booked for crashes
        :param logger: logger object
        :param spark: spark session object
        :param cfg: config object
        :return: None
        """
        logger.info("solving question - 2")
        charges: DataFrame = FrameReader.get_charges_frame(logger, spark, cfg)
        vehicle: DataFrame = FrameReader.get_vehicle_frame(logger, spark, cfg)

        two_wheeler_types = ["MOTOR VEHICLE"]

        # can be saved or returned
        num_two_wheelers = vehicle.join(charges, on=["CRASH_ID"]).filter(
            col("UNIT_DESC_ID").isin(two_wheeler_types)).count()

        print(num_two_wheelers)

    @staticmethod
    def calculate_state_max_num_femal(logger, spark, cfg):
        """
        Calculates state with highest number of accidents in which females are involved
        :param logger: logger object
        :param spark: spark session object
        :param cfg: config object
        :return: None
        """
        logger.info("solving question - 3")
        charges: DataFrame = FrameReader.get_charges_frame(logger, spark, cfg)
        person: DataFrame = FrameReader.get_person_frame(logger, spark, cfg)

        # solution, can be saved or returned
        state_with_maximum_female_injuries = \
            person.join(charges, on=["CRASH_ID"]).where("PRSN_GNDR_ID == 'FEMALE'").groupBy(
                "DRVR_LIC_STATE_ID").count().orderBy(desc("count")).select("DRVR_LIC_STATE_ID").first()[0]
        print(state_with_maximum_female_injuries)

    @staticmethod
    def calculate_top_vehicles_injuries(logger, spark, cfg):
        """
        Calculates top 5 to 15th VEH_MAKE_IDs that contribute to largest number of injuries
        including death
        :param logger: logger object
        :param spark: spark session object
        :param cfg: config object
        :return: None
        """
        logger.info("solving question - 4")
        vehicle: DataFrame = FrameReader.get_vehicle_frame(logger, spark, cfg)

        vehicle = vehicle.select(["VEH_MAKE_ID", "POSS_INJRY_CNT"]).groupBy(["VEH_MAKE_ID"]).agg(
            sum("POSS_INJRY_CNT").alias("POSS_INJRY_CNT_SUM"))
        w = Window().orderBy(desc("POSS_INJRY_CNT_SUM"))
        vehicle = vehicle.withColumn("row_num", row_number().over(w)).filter("row_num > 4 and row_num <16").select(
            "VEH_MAKE_ID")

    @staticmethod
    def calculate_top_ethnic_user_group(logger, spark, cfg):
        """
        Top ethnic user group of each unique body style
        :param logger: logger object
        :param spark: spark session object
        :param cfg: config object
        :return: None
        """
        logger.info("solving question - 5")
        vehicle: DataFrame = FrameReader.get_vehicle_frame(logger, spark, cfg)
        person: DataFrame = FrameReader.get_person_frame(logger, spark, cfg)

        # unique body styles involved in crashes
        windowSpec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("count"))

        vehicle_with_person = vehicle.join(person, on=["CRASH_ID"]).select(["VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"])
        vehicle_with_person = vehicle_with_person.groupBy(["VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"]).count().orderBy(
            desc("count"))

        vehicle_with_person = vehicle_with_person.withColumn("rank", rank().over(windowSpec)).where("rank==1").drop(
            "rank")

        vehicle_with_person.show(truncate=False)

    @staticmethod
    def top_five_zip(logger, spark, cfg):
        """
        Calculates top 5 zip codes with highest number of crashes due to alcohol
        :param logger: logger object
        :param spark: spark session object
        :param cfg: config object
        :return: None
        """
        logger.info("solving question - 6")
        charges: DataFrame = FrameReader.get_charges_frame(logger, spark, cfg)
        vehicle: DataFrame = FrameReader.get_vehicle_frame(logger, spark, cfg)
        person: DataFrame = FrameReader.get_person_frame(logger, spark, cfg)

        car_keyword = ["MOTOR VEHICLE"]
        alcohol_keyword = "ALCOHOL"
        # to find charges and cause
        q6 = vehicle.join(charges, on=["CRASH_ID"]).filter(col("UNIT_DESC_ID").isin(car_keyword)).join(person, on=[
            "CRASH_ID"]).where(col("CHARGE").like("%{}%".format(alcohol_keyword))).filter("DRVR_ZIP is not null")

        # window = Window.partitionBy("DRVR_ZIP")
        # q6 = q6.withColumn("countPerZip", count("CHARGE").over(window))

        q6 = q6.select(["DRVR_ZIP", "CRASH_ID"]).groupBy(["DRVR_ZIP"]).count().orderBy(desc("count")).limit(5)

    @staticmethod
    def count_distinct_crash_ids(logger, spark, cfg):
        """
        Distinct crash ids where no damaged property was observed and damage level is above 4 and
        cars avail insurance
        :param logger: logger object
        :param spark: spark session object
        :param cfg: config object
        :return: None
        """
        logger.info("solving question - 7")
        vehicle: DataFrame = FrameReader.get_vehicle_frame(logger, spark, cfg)
        damages: DataFrame = FileIO.read_csv_as_spark_df(logger,
                                                         spark,
                                                         cfg["input"]["damage"]["load"]["file_location"],
                                                         cfg["input"]["damage"]["load"]["delimiter"])

        q7 = vehicle.join(damages, on=["CRASH_ID"], how="leftanti").where("FORCE_DIR_1_ID>4").where(
            col("FIN_RESP_TYPE_ID").like("%INSURANCE%"))
        count_of_distinct_id = q7.select("CRASH_ID").distinct().count()

        print(count_of_distinct_id)

    @staticmethod
    def calculate_top_vehicles_speeding(logger, spark, cfg):
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with
        highest number of offences
        :param logger: logger object
        :param spark: spark session object
        :param cfg: config object
        :return: None
        """
        logger.info("solving question - 8")
        vehicle: DataFrame = FrameReader.get_vehicle_frame(logger, spark, cfg)
        person: DataFrame = FrameReader.get_person_frame(logger, spark, cfg)

        speed_keyword = "SPEED"
        vehicle = vehicle.where(col("CONTRIB_FACTR_1_ID").like("%{}%".format(speed_keyword))
                                | col("CONTRIB_FACTR_2_ID").like("%{}%".format(speed_keyword))
                                | col("CONTRIB_FACTR_2_ID").like("%{}%".format(speed_keyword))
                                )
        licenced_vehicle = vehicle.join(person, on=["CRASH_ID"]).where(
            col("DRVR_LIC_TYPE_ID").like("%{}%".format("DRIVER LICENSE")))

        # top 10 vehicle color
        top_10_vehicle_color = vehicle.groupBy("VEH_COLOR_ID").count().orderBy(desc("count")).limit(10)

        licenced_vehicle = licenced_vehicle.join(top_10_vehicle_color, on=["VEH_COLOR_ID"])

        # topp 25 states with hihest umber of offences
        top_25_states_offence = vehicle.groupBy("VEH_LIC_STATE_ID").count().orderBy(desc("count")).limit(25)

        # join to filter top 25 states having offence
        licenced_vehicle = licenced_vehicle.join(top_25_states_offence, on=["VEH_LIC_STATE_ID"])

        q8 = licenced_vehicle.groupBy("VEH_MAKE_ID").count().orderBy(desc("count")).limit(5)

        q8.show(truncate=False)

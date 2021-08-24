from pyspark.sql.window import Window
from pyspark.sql.functions import concat, lpad, rpad, row_number, lit, col, md5
from pyspark.sql.types import StructField, StructType, LongType


class Transform:

    def __init__(self, spark, df, offset=0, timestamp=None):
        self.spark = spark
        self.df = df
        self.offset = offset
        self.timestamp = timestamp

    def __get_spark(self):
        return self.spark

    def __get_df(self):
        return self.df

    def __get_offset(self):
        return self.offset

    def __get_timestamp(self):
        return self.timestamp

    def transform(self):
        """
        Method that calls the various transformations one by one
        :return: returns transformed DataFrame
        """
        try:
            spark = self.__get_spark()
            df = self.__get_df()
            timestamp = self.__get_timestamp()
            offset = self.__get_offset()
            columns = df.columns
            df1 = self.generate_request_id(df=df, timestamp=timestamp, target_col_name="request_id")
            df2 = self.last_sent_records(df=df1)
            df2 = df2.repartition(100)
            df3 = self.generate_record_checksum(df=df2, columns=columns)
            transformed_df = self.dfzipwithuniqueid(df=df3, offset=offset)

        except Exception as err:
            print("\nError in transformation")
            raise Exception(
                f"****** Error Occurred in transformation - error message {err}") from err
        return transformed_df

    def generate_request_id(self, df, timestamp, target_col_name="request_id"):
        """
        Generates a REQUEST_ID column by concatenating the timestamp with row_number
        :param df: Input DataFrame
        :param timestamp: timestamp taken from source arguments
        :param target_col_name: column name of additional column
        :return: returns transformed DataFrame
        """
        windowSpec = Window.orderBy("sku", "name", "description")
        df = df.withColumn("timestamp", lit(timestamp))
        df = df.withColumn(
            target_col_name, concat(rpad("timestamp", 20, "0"), lpad(row_number().over(windowSpec), 12, "0")))
        df = df.drop("timestamp")

        return df

    def last_sent_records(self, df):
        '''
        Filters records and chooses only those records that have the greatest timestamp
        :param df: Source DataFrame
        :return: New DataFrame with only latest records
        '''
        windowSpec = Window.partitionBy("sku").orderBy(col("request_id").desc())
        df = df.withColumn(
            "rank", row_number().over(windowSpec))
        df = df.filter(df.rank == "1")
        df = df.drop("rank")
        return df

    def generate_record_checksum(self, df, columns):
        '''
        Generates a checksum value based on initial raw columns
        :param df: Source DataFrame
        :param columns: list of raw columns
        :return: returns transformed DataFrame
        '''
        df = df.withColumn("record_checksum", md5(concat(*columns)))
        return df

    def dfzipwithuniqueid(self, df, offset=0, colName="p_id"):
        '''
            Generates an ID of type INT for every record
            :param spark: Instance of spark session
            :param df: source dataframe
            :param offset: adjustment to zipWithIndex()'s index
            :param colName: name of the index column
            :return: returns transformed DataFrame
        '''
        spark = self.__get_spark()
        offset = offset + 1
        new_schema = StructType([StructField(colName, LongType(), True)] + df.schema.fields)
        zipped_rdd = df.rdd.zipWithUniqueId()
        new_rdd = zipped_rdd.map(lambda args: ([args[1] + offset] + list(args[0])))
        df = spark.createDataFrame(new_rdd, new_schema)
        return df

    def agg_transform(self, offset):
        try:
            df = self.__get_df()
            df.repartition(100)
            spark = self.__get_spark()
            df.createOrReplaceTempView("products_table")
            agg_df = spark.sql("select name, count(*) as count_of_names from products_table group by name")

        except Exception as err:
            print("\nError in aggregation")
            raise Exception(
                f"****** Error Occurred in aggregation - error message {err}") from err

        return agg_df

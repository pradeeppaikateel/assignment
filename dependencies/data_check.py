class DataCheck:

    def __init__(self, df, spark):
        self.df = df
        self.spark = spark

    def __get_spark(self):
        return self.spark

    def __get_df(self):
        return self.df

    def check_for_count(self):
        spark = self.__get_spark()
        df = self.__get_df()
        count = df.count()
        if count == 0:
            print("Source has no records")
            spark.SparkContext.stop()
        return df

    def check_for_error_records(self):
        df = self.__get_df()
        spark = self.__get_spark()
        self.check_for_count()
        df_error = df.filter("sku IS NULL OR name IS NULL OR description IS NULL")
        df_valid = df.filter("sku IS NOT NULL AND name IS NOT NULL AND description IS NOT NULL")
        if df_error.count() > 0:
            df_error.write \
                .format("csv") \
                .option("header", "true") \
                .mode("overwrite") \
                .save("/opt/bitnami/spark/postman-assignment/error_records/error_records.csv")
        return df_valid

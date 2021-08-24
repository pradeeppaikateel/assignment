class Extract:

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def __get_spark(self):
        return self.spark

    def __get_logger(self):
        return self.logger

    def read_from_local(self, file_format, paths):
        """
        Reads data from local file into a DataFrame
        :param file_format: format of input file
        :param paths: path of input file
        :return: Source DataFrame
        """
        logger = self.__get_logger()
        spark = self.__get_spark()
        df = None
        try:
            if file_format == "csv":
                df = spark.read.load(paths,
                                     format=file_format,
                                     header='True',
                                     inferSchema='True',
                                     delimiter=',',
                                     multiLine='True')
                print("\nRead from Source", paths)
        except Exception as err:
            raise Exception(
                f"****** Error Occur in : {paths} - error message {err}") from err

        return df

    def read_from_table(self, postgresql):
        logger = self.__get_logger()
        spark = self.__get_spark()
        df = None
        try:
            postgres_configs = postgresql.configs
            agg_df = spark.read.format("jdbc").option("url", postgres_configs["url"]). \
                option("query", "SELECT * FROM public.products"). \
                option("user", postgres_configs["user"]). \
                option("password", postgres_configs["password"]). \
                load()
            print("\nRead from Source: postman.products")

        except Exception as err:
            raise Exception(
                f"****** Error Occur in reading from products - error message {err}") from err

        return agg_df

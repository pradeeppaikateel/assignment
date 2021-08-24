import argparse
from pyspark.sql import SparkSession
from dependencies.env_config_parser import EnvConfigParser
from dependencies.table_config_parser import TableConfigParser


class Utils:

    def parse_job_args(self, args=None):
        """Parses the job arguments passed in to the Spark job.
        Args:
            args (list): List of arguments.
        Returns:
            argparse.Namespace: Job arguments.
        """
        parser = argparse.ArgumentParser(description="Run a PySpark job.")
        parser.add_argument(
            "--source",
            required=True,
            help="Source"
        )
        parser.add_argument(
            "--env",
            required=True,
            help="Environment"
        )
        parser.add_argument(
            "--job_run_date",
            required=False,
            help="Job run date"
        )
        args = parser.parse_args(args)
        return args

    def create_spark_session(self,app_name):
        """Initializes the Spark session object.
        Args:
            app_name (str): The name of the Spark application.
            env (str): Environment.
        Returns:
            The Spark session object.
        """

        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()
        return spark

    def get_configs(self,job_args):
        """Returns the environment & table configurations based on the job
        arguments & data object YAML file.
        Args:
            job_args (argparse.Namespace): Job arguments.
        Returns:
            Object: EnvConfigParser object containing environment configurations.
            Object: TableConfigParser object containing table configurations.
        """
        env_configs = EnvConfigParser(job_args)
        table_configs = TableConfigParser(job_args.source)

        return env_configs, table_configs

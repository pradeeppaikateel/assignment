import sys
import datetime
from dependencies.utils import parse_job_args, create_spark_session, get_configs
from dependencies.log import SparkLogger
from dependencies.extract import Extract
from dependencies.postgresql import PostgreSql
from dependencies.transforms import Transform
from dependencies.load import Loader
from dependencies.data_check import DataCheck


def main():
    """Main ETL script definition.

    :return: None
    """
    sys.stdout = open("/opt/bitnami/spark/logs/spark.txt", "w")
    print("\n****************************************** STDOUT *********************************************")
    job_args = parse_job_args(sys.argv[1:])
    app_name = f"{__file__.split('.py')[0]}-{job_args.source}"

    spark_logger = SparkLogger(
        app_name="spark"
    )
    logger = spark_logger.get_logger()
    logger.info(f"Job Arguments: {job_args}")

    spark = create_spark_session(app_name)
    application_id = spark.sparkContext.applicationId

    job_run_date_split = job_args.job_run_date.split('-')
    day_hr = job_run_date_split[2].split('T')
    job_run_date = datetime.datetime(int(job_run_date_split[0]), int(job_run_date_split[1]), int(day_hr[0]),
                                     int(day_hr[1].split(':')[0]))
    get_vals = list([val for val in job_args.job_run_date
                     if val.isnumeric()])

    timestamp = "".join(get_vals)

    env_configs, table_configs = get_configs(job_args)

    source = table_configs.sources[0]
    data_object = source['data_object']
    target = table_configs.targets[0]
    dbtable = target['dbtable']

    source_df = extract_data(
        spark=spark,
        source=source,
        dbtable=dbtable,
        data_object=data_object,
        application_id=application_id,
        spark_logger=spark_logger
    )
    if not source_df:
        print("\nData has not been extracted")
        spark.sparkContext.stop()
    data_check = DataCheck(df=source_df, spark=spark)
    source_df = data_check.check_for_error_records()
    database = PostgreSql(env_configs.postgres_configs)
    database.initialise_db()
    database.initialise_tables()
    offset = database.query_sql("Select max(p_id) from products")

    if not offset[0][0]:
        offset = 0
    else:
        offset = offset[0][0]

    offset = int(offset)

    transformed_df = transform_data(
        spark=spark,
        df=source_df,
        offset=offset,
        timestamp=timestamp,
        dbtable=dbtable,
        data_object=data_object,
        application_id=application_id,
        spark_logger=spark_logger
    )
    if not transformed_df:
        print("\nData has not been transformed")
        spark.sparkContext.stop()
    print("\nDataFrame Schema after transformation is as follows:")
    print("")
    transformed_df.printSchema()

    loaded = load_data(df=transformed_df,
                       postgresql=database,
                       spark_logger=spark_logger,
                       dbtable=dbtable,
                       data_object=data_object,
                       application_id=application_id)
    if loaded:
        print("\nEnd of initial data loading process")

    print("\nStarting extraction from products table for aggregation")
    extracted_df = extract_table_data(spark=spark,
                                      dbtable="products_agg",
                                      data_object="products_agg",
                                      postgresql=database,
                                      application_id=application_id,
                                      spark_logger=spark_logger)

    offset = database.query_sql("Select max(p_id) from products_agg")
    if not offset[0][0]:
        offset = 0
    else:
        offset = offset[0][0]

    offset = int(offset)

    aggregated_df = transform_data_agg(spark=spark,
                                       df=extracted_df,
                                       dbtable="products_agg",
                                       data_object="products_agg",
                                       offset=offset,
                                       application_id=application_id,
                                       spark_logger=spark_logger)
    if not aggregated_df:
        print("\nData has not been aggregated")
        spark.sparkContext.stop()
    print("\nDataFrame Schema after aggregation is as follows:")
    print("")
    aggregated_df.printSchema()

    loaded = 0
    loaded = load_agg_data(df=aggregated_df,
                           postgresql=database,
                           spark_logger=spark_logger,
                           dbtable="products_agg",
                           data_object="products_agg",
                           application_id=application_id)

    print("\n***********************************************************************************************")
    sys.stdout.close()
    spark.sparkContext.stop()


def extract_data(
        spark,
        source,
        dbtable,
        data_object,
        application_id,
        spark_logger=None):
    """Extract data from source and create dataframe.
        :param spark: instance of spark session
        :param source: data object name
        :param dbtable: target table name
        :param application_id: spark application id
        :param spark_logger: spark logger instance
        """

    source_df = None
    source_count = None
    temp_view_name = source["temp_view_name"]
    source_type = source["source_type"].lower()
    logger = spark_logger.get_logger()

    try:
        paths = ""
        if source_type in ["local"]:
            paths = source["path"]
            if paths:
                extract = Extract(spark=spark, logger=logger)
                source_df = extract.read_from_local(
                    file_format=source["format"],
                    paths=paths,
                )
        if source_df:
            # source_df.createOrReplaceTempView(temp_view_name)
            source_count = source_df.count()
            spark_logger.handler(
                business_object='Business_object',
                batch_count='Batch_count',
                batch_status='Received',
                subject_area=dbtable,
                business_domain=data_object,
                count=str(source_count),
                err_message='',
                application_id=application_id)
        else:
            print("\nNo Data has been extracted")
            spark.sparkContext.stop()

    except Exception as err:
        spark_logger.handler(
            business_object="Business_object",
            batch_count="Batch_count",
            batch_status="Failure",
            subject_area=dbtable,
            business_domain=data_object,
            count=str(source_count),
            err_message=str(err),
            application_id=application_id)
        raise Exception(
            f"Batch failed to read {source_count} records - error message {err}") from err

    return source_df


def transform_data(spark,
                   df,
                   dbtable,
                   data_object,
                   offset,
                   application_id,
                   spark_logger,
                   timestamp):
    """Transform original dataset by initialising Transform class and calling its methods
    :param spark: instance of spark session
    :param df: source dataframe
    :param dbtable: target table
    :param data_object: data object
    :param offset: existing max p_id from database table
    :param application_id: spark application id
    :param spark_logger: spark logger instance
    :param timestamp: timestamp
    """
    df_transformed = None
    transform_count = 0
    try:

        transform = Transform(spark=spark,
                              df=df,
                              offset=offset,
                              timestamp=timestamp
                              )
        df_transformed = transform.transform()
        if df_transformed:
            transform_count = df_transformed.count()
            spark_logger.handler(
                business_object='Business_object',
                batch_count='Batch_count',
                batch_status='Transformed',
                subject_area=dbtable,
                business_domain=data_object,
                count=str(transform_count),
                err_message='',
                application_id=application_id)

    except Exception as err:
        spark_logger.handler(
            business_object="Business_object",
            batch_count="Batch_count",
            batch_status="Failure",
            subject_area=dbtable,
            business_domain=data_object,
            count=str(transform_count),
            err_message=str(err),
            application_id=application_id)
        raise Exception(
            f"Batch failed to transform {transform_count} records - error message {err}") from err

    return df_transformed


def load_data(df,
              postgresql,
              spark_logger,
              dbtable,
              data_object,
              application_id):
    """Collect data locally and write to CSV.

    :param application_id:
    :param data_object:
    :param dbtable:
    :param spark_logger: spark logger instance
    :param postgresql: Database connection object
    :param spark: spark session instance
    :param df: DataFrame to load to database.
    :return: None
    """
    load_count = 0
    try:
        loaded = Loader(df=df, postgresql=postgresql)
        load_count = loaded.load()

        if load_count > 0:
            spark_logger.handler(
                business_object='Business_object',
                batch_count='Batch_count',
                batch_status='Delivered',
                subject_area=dbtable,
                business_domain=data_object,
                count=str(load_count),
                err_message='',
                application_id=application_id)
            return 1

    except Exception as err:
        spark_logger.handler(
            business_object="Business_object",
            batch_count="Batch_count",
            batch_status="Failure",
            subject_area=dbtable,
            business_domain=data_object,
            count=str(load_count),
            err_message=str(err),
            application_id=application_id)
        raise Exception(
            f"Batch failed to load {load_count} records - error message {err}") from err

    return 0


def extract_table_data(
        spark,
        dbtable,
        data_object,
        postgresql,
        application_id,
        spark_logger=None):
    """Extract data from source and create dataframe.
        :param spark: instance of spark session
        :param source: data object name
        :param dbtable: target table name
        :param application_id: spark application id
        :param spark_logger: spark logger instance
        """

    agg_df_count = None
    agg_df = None
    logger = spark_logger.get_logger()

    try:
        extract = Extract(spark=spark, logger=logger)
        agg_df = extract.read_from_table(postgresql)

        if agg_df:
            agg_df_count = agg_df.count()
            spark_logger.handler(
                business_object='Business_object',
                batch_count='Batch_count',
                batch_status='Received',
                subject_area=dbtable,
                business_domain=data_object,
                count=str(agg_df_count),
                err_message='',
                application_id=application_id)
        else:
            print("\nNo Data has been extracted")
            spark.sparkContext.stop()

    except Exception as err:
        spark_logger.handler(
            business_object="Business_object",
            batch_count="Batch_count",
            batch_status="Failure",
            subject_area=dbtable,
            business_domain=data_object,
            count=str(agg_df_count),
            err_message=str(err),
            application_id=application_id)
        raise Exception(
            f"Batch failed to read {agg_df_count} records - error message {err}") from err

    return agg_df


def transform_data_agg(spark,
                       df,
                       dbtable,
                       data_object,
                       offset,
                       application_id,
                       spark_logger
                       ):
    """Transform original dataset by initialising Transform class and calling its methods
    :param spark: instance of spark session
    :param df: source dataframe
    :param dbtable: target table
    :param data_object: data object
    :param offset: existing max p_id from database table
    :param application_id: spark application id
    :param spark_logger: spark logger instance
    :param timestamp: timestamp
    """
    aggregated_df = None
    aggregated_count = 0
    try:

        transform_agg = Transform(spark=spark,
                                  df=df)
        aggregated_df = transform_agg.agg_transform(offset=offset)
        if aggregated_df:
            aggregated_count = aggregated_df.count()
            spark_logger.handler(
                business_object='Business_object',
                batch_count='Batch_count',
                batch_status='Transformed',
                subject_area=dbtable,
                business_domain=data_object,
                count=str(aggregated_count),
                err_message='',
                application_id=application_id)

    except Exception as err:
        spark_logger.handler(
            business_object="Business_object",
            batch_count="Batch_count",
            batch_status="Failure",
            subject_area=dbtable,
            business_domain=data_object,
            count=str(aggregated_count),
            err_message=str(err),
            application_id=application_id)
        raise Exception(
            f"Batch failed to aggregate {aggregated_count} records - error message {err}") from err

    return aggregated_df


def load_agg_data(df,
                  postgresql,
                  spark_logger,
                  dbtable,
                  data_object,
                  application_id):
    """Collect data locally and write to CSV.

    :param application_id:
    :param data_object:
    :param dbtable:
    :param spark_logger: spark logger instance
    :param postgresql: Database connection object
    :param spark: spark session instance
    :param df: DataFrame to load to database.
    :return: None
    """
    load_agg_count = 0
    try:
        loaded_agg = Loader(df=df, postgresql=postgresql)
        load_agg_count = loaded_agg.load_agg()

        if load_agg_count > 0:
            spark_logger.handler(
                business_object='Business_object',
                batch_count='Batch_count',
                batch_status='Delivered',
                subject_area=dbtable,
                business_domain=data_object,
                count=str(load_agg_count),
                err_message='',
                application_id=application_id)
            return 1

    except Exception as err:
        spark_logger.handler(
            business_object="Business_object",
            batch_count="Batch_count",
            batch_status="Failure",
            subject_area=dbtable,
            business_domain=data_object,
            count=str(load_agg_count),
            err_message=str(err),
            application_id=application_id)
        raise Exception(
            f"Batch failed to load aggregated {load_agg_count} records - error message {err}") from err

    return 0


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

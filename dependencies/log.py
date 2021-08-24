from datetime import datetime
from pathlib import Path
import json
import logging
import os


class SparkLogger:

    def __init__(self,
                 app_name,
                 log_path=None,
                 formatter=logging.Formatter(
                     '%(message)s'),
                 level=logging.INFO):
        self.logger = SparkLogger._setup_logger(
            app_name, log_path, formatter, level)

    @staticmethod
    def _setup_logger(name, log_path, formatter, level):
        """To setup as many loggers as you want"""
        logger = logging.getLogger(name)
        logger.setLevel(level)

        if log_path:
            os.umask(0)
            Path(log_path.rsplit('/', 1)[0]).mkdir(exist_ok=True)
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger

    def get_logger(self):
        return self.logger

    def createauditmessage(
            self,
            business_key,
            business_value,
            message,
            log_level,
            componentName,
            status,
            application_id,
            information=None):

        audit_msg = {}
        audit_msg["timestamp"] = datetime.now().replace(
            microsecond=0).isoformat()
        audit_msg["message"] = information
        audit_msg['logEvent'] = message
        audit_msg['logLevel'] = log_level
        audit_msg['componentName'] = componentName
        audit_msg['applicationId'] = application_id
        audit_msg['status'] = status
        business_dict = dict(
            zip(business_key.split("|"), business_value.split("|")))
        audit_msg['businessKeyStructure'] = business_key
        audit_msg['businessKeyValue'] = business_value
        audit_msg['businessKey'] = business_dict
        if log_level == "INFO":
            self.logger.info(json.dumps(audit_msg))
            print("")
            print(audit_msg)
        elif log_level == "DEBUG":
            self.logger.debug(json.dumps(audit_msg))
            print("")
            print(audit_msg)
        elif log_level == "ERROR":
            self.logger.error(json.dumps(audit_msg))
            print("")
            print(audit_msg)
        else:
            raise Exception("Invalid Log Level")

    def handler(
            self,
            business_object,
            batch_count,
            batch_status,
            subject_area,
            business_domain,
            count,
            err_message,
            application_id):

        if batch_status == 'Received':
            logging.info("****** Received ******")
            self.createauditmessage(
                business_key=business_object + "|" + batch_count,
                business_value=business_domain + "|" + count,
                message="Received",
                log_level="INFO",
                status="success",
                componentName=subject_area,
                information="Received",
                application_id=application_id)
        elif batch_status == 'Delivered':
            logging.info("****** Delivered ******")
            self.createauditmessage(
                business_key=business_object + "|" + batch_count,
                business_value=business_domain + "|" + count,
                message="Delivered",
                log_level="INFO",
                status="success",
                componentName=subject_area,
                information="Delivered",
                application_id=application_id)
        elif batch_status == 'Transformed':
            logging.info("****** Transformed ******")
            self.createauditmessage(
                business_key=business_object + "|" + batch_count,
                business_value=business_domain + "|" + count,
                message="Transformed",
                log_level="INFO",
                status="success",
                componentName=subject_area,
                information="Transformed",
                application_id=application_id)
        else:
            logging.info("****** ERROR ******")
            self.createauditmessage(
                business_key=business_object + "|" + batch_count,
                business_value=business_domain + "|" + count,
                message="Batch Failure",
                log_level="ERROR",
                status="Failure",
                componentName=subject_area,
                information=err_message,
                application_id=application_id)

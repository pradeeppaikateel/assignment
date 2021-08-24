import yaml


class TableConfigParser:

    def __init__(self, source):
        self.configs = TableConfigParser.__get_configs(source)
        self.ingestion_interval = self.__get_ingestion_interval()
        self.sources = self.__get_sources()
        self.targets = self.__get_targets()

    @staticmethod
    def __get_configs(source):
        if ".yaml" in source:
            path = source
        else:
            path = f"/opt/bitnami/spark/postman-assignment/configs/{source}.yaml"

        with open(path, "r") as config_file:
            configs = yaml.safe_load(config_file)
        return configs

    def __get_ingestion_interval(self):
        return self.configs.get("ingestion_interval")

    def __get_sources(self):
        return self.configs.get("sources")

    def __get_targets(self):
        return self.configs.get("targets")
class EnvConfigParser:

    def __init__(self, job_args):
        self.configs = job_args
        self.env = self.__get_env()
        self.postgres_configs = self.__get_postgres_configs()

    def __get_env(self):
        return self.configs.env

    def __get_postgres_configs(self):
        postgres_configs = {
            "url": 'jdbc:postgresql://172.18.0.22:5432/postman',
            "driver": 'org.postgresql.Driver',
            "user": 'postgres',
            "password": 'postgres',
            "host": '172.18.0.22',
            "port": "5432",
            "dbtable": "products",
            "database": "postman"
        }

        return postgres_configs

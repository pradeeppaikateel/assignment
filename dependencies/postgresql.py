import psycopg2


class PostgreSql:

    def __init__(self, configs):
        self.configs = configs

    def get_configs(self):
        return self.configs

    def initialise_db(self, database_name="postman"):
        connection = None
        cursor = None
        configs = self.get_configs()
        temp = []
        try:
            connection = psycopg2.connect(user=configs["user"],
                                          password=configs["password"],
                                          host=configs["host"],
                                          port=configs["port"]
                                          )
            print("\nPostgreSQL connection is open to default DB")
            if connection:
                connection.autocommit = True
                cursor = connection.cursor()
                cursor.execute("SELECT datname FROM pg_database;")
                list_database = cursor.fetchall()
                for i in list_database:
                    temp.append(i[0])
                if database_name in temp:
                    print(f"{database_name} database already exists")
                else:
                    sql = f"create database {database_name};"
                    cursor.execute(sql)
                    print("\nDatabase created successfully")

        except (Exception, psycopg2.Error) as error:
            # print("Error while connecting to PostgreSQL", error)
            raise Exception(
                f"Error while connecting to PostgreSQL : {error}") from error

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("\nPostgreSQL connection to default DB is closed")

    def initialise_tables(self):
        connection = None
        cursor = None
        configs = self.get_configs()
        try:
            connection = psycopg2.connect(user=configs["user"],
                                          password=configs["password"],
                                          host=configs["host"],
                                          port=configs["port"],
                                          database=configs["database"]
                                          )
            print("\nPostgreSQL connection is open to postman DB")
            if connection:
                connection.autocommit = True
                cursor = connection.cursor()
                sql1 = '''CREATE TABLE IF NOT EXISTS products( 
                        p_id bigint primary key,
                        sku varchar(70) unique not null,
                        name varchar(70) not null,
                        description varchar(300) not null,
                        request_id varchar(40),
                        record_checksum varchar(45) not null,
                        updt_tmstmp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ;'''
                cursor.execute(sql1)

                sql2 = '''CREATE TABLE IF NOT EXISTS products_agg( 
                        p_id bigint primary key,
                        name varchar(70) unique not null,
                        "no. of products" integer not null,
                        updt_tmstmp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ;'''
                cursor.execute(sql2)

                print("\ntables created successfully")

        except (Exception, psycopg2.Error) as error:
            # print("Error while connecting to PostgreSQL", error)
            raise Exception(
                f"Error while connecting to PostgreSQL : {error}") from error

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("\nPostgreSQL connection is closed")

    def query_sql(self, sql):
        connection = None
        cursor = None
        configs = self.get_configs()
        try:
            connection = psycopg2.connect(user=configs["user"],
                                          password=configs["password"],
                                          host=configs["host"],
                                          port=configs["port"],
                                          database=configs["database"])
            print("\nPostgreSQL connection is open")
            cursor = connection.cursor()

            cursor.execute(sql)
            record = cursor.fetchall()
            print("\nCurrent Max p_id is : ", record)
            return record

        except (Exception, psycopg2.Error) as error:
            # print("Error while connecting to PostgreSQL", error)
            raise Exception(
                f"Error while connecting to PostgreSQL : {error}") from error

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("\nPostgreSQL connection is closed")

    def load_data(self, partition):
        connection = None
        cursor = None
        configs = self.get_configs()
        try:
            connection = psycopg2.connect(user=configs["user"],
                                          password=configs["password"],
                                          host=configs["host"],
                                          port=configs["port"],
                                          database=configs["database"])
            print("\nPostgreSQL connection is open")
            cursor = connection.cursor()

            for row in partition:
                sql = f'''INSERT INTO products (p_id, sku, name, description, request_id, record_checksum)
                VALUES({row.p_id},'{row.sku}','{row.name}','{row.description}','{row.request_id}','{row.record_checksum}') 
                ON CONFLICT (sku) 
                DO 
                UPDATE SET name = '{row.name}', description = '{row.description}', request_id = '{row.request_id}',
                 record_checksum = '{row.record_checksum}', updt_tmstmp = current_timestamp ;'''
                cursor.execute(sql)

            connection.commit()
            print("\nData inserted successfully")
        except (Exception, psycopg2.Error) as error:
            connection.rollback()
            raise Exception(
                f"Error while connecting to PostgreSQL : {error}") from error

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("\nPostgreSQL connection is closed")

    def load_aggregated_data(self, partition):
        connection = None
        cursor = None
        configs = self.get_configs()
        try:
            connection = psycopg2.connect(user=configs["user"],
                                          password=configs["password"],
                                          host=configs["host"],
                                          port=configs["port"],
                                          database=configs["database"])
            print("\nPostgreSQL connection is open")
            cursor = connection.cursor()

            for row in partition:
                sql = f'''INSERT INTO products_agg (p_id, name, "no. of products")
                VALUES({row.p_id}, '{row.name}',{row.count_of_names}) 
                ON CONFLICT (name) 
                DO 
                UPDATE SET "no. of products" = '{row.count_of_names}', updt_tmstmp = current_timestamp ;'''
                cursor.execute(sql)

            connection.commit()
            print("\nAggregated data inserted successfully")
        except (Exception, psycopg2.Error) as error:
            connection.rollback()
            raise Exception(
                f"Error while connecting to PostgreSQL : {error}") from error

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("\nPostgreSQL connection is closed")

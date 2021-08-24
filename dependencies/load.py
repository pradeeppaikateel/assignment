class Loader:

    def __init__(self, df, postgresql):
        self.df = df
        self.postgresql = postgresql

    def get_df(self):
        return self.df

    def get_postgresql(self):
        return self.postgresql

    def load(self):

        try:
            df = self.get_df()
            postgresql = self.get_postgresql()
            load_count = df.count()
            print("\ncount of load:", load_count)
            df.foreachPartition(postgresql.load_data)
            print("\nData has been loaded successfully")

        except Exception as err:
            print("\nError in loading")
            raise Exception(
                f"****** Error Occur in loading : error message {err}") from err
        return load_count

    def load_agg(self):

        try:
            df = self.get_df()
            postgresql = self.get_postgresql()
            load_count = df.count()
            print("\ncount of agg load:", load_count)
            df.foreachPartition(postgresql.load_aggregated_data)
            print("\nAggregated data has been loaded successfully")

        except Exception as err:
            print("\nError in loading aggregated data")
            raise Exception(
                f"****** Error Occur in loading aggregated data: error message {err}") from err
        return load_count

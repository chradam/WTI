import json
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from wtiproj03_ETL import dict_list_to_df


class CassandraClient:
    def __init__(self, host, port, keyspace, table):
        self.cluster = Cluster([host], port=port)
        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory
        self.keyspace = keyspace
        self.table = table

        self.create_keyspace()
        self.__create_table()

        self.lastindex = 0

    def create_keyspace(self):
        self.session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS """ + self.keyspace + """
                    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
                    """
        )

    def  __create_table(self):
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS """ + self.keyspace + """.""" + self.table + """ (
            id int,
            ratings text,
            PRIMARY KEY(id)
            )
            """
        )
        self.lastindex = 0

    def push_data_table(self, id, ratings):
        self.session.execute(
            """
            INSERT INTO """ + self.keyspace + """.""" + self.table + """ (id, ratings)
                VALUES (%(id)s, %(ratings)s)
            """,
            {
                'id': id,
                'ratings': ratings
            }
        )
        self.lastindex = len(self.get_data_table())

    def get_data_table(self):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        table_as_list = list(row for row in rows)

        return table_as_list

    def pull_data_table(self, key):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")

        table_as_list = list(json.loads(row[key]) for row in rows)
        genre_names = list(dict_list_to_df(table_as_list))[:-3]

        return table_as_list, genre_names

    def pull_avg_data_table(self, key, user_id):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + " WHERE id=" + user_id + ";")

        table_as_list = list(json.loads(row[key]) for row in rows)
        genre_names = list(dict_list_to_df(table_as_list))[:-3]

        return table_as_list, genre_names

    def print_data_table(self):
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        for row in rows:
            print(json.loads(row['ratings']))

    def clear_table(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")
        self.lastindex = 0

    def delete_table(self):
        self.session.execute("DROP TABLE " + self.keyspace + "." + self.table + ";")


if __name__ == "__main__":
    cc = CassandraClient('127.0.0.1', 9042, 'ratings', 'test')
    print("cassandra client ready")
    dict_list = [{'a': 1, 'b': 2},
                 {'c': 3, 'd': 4},
                 {'e': 5, 'f': 6},
                 {'g': 7, 'h': 8}]

    # for x, d in enumerate(dict_list):
    #     s = json.dumps(d)
    #     # print(s)
    #     # print(json.loads(s))
    #     cc.push_data_table('test', x + 1, s)

    print(cc.pull_data_table('ratings')[0])
    if not cc.pull_data_table('ratings')[0]:
        print('empty')
    else:
        a, b = cc.pull_data_table('ratings')
        print(a)
        print(b)
        cc.print_data_table()

    cc.delete_table()

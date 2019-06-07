from wtiproj03_ETL import get_rated_movies_with_genres, join_df, dict_list_to_df, df_to_dict_list
import json
import pandas as pd
import numpy as np
from wtiproj06_cassandra_client import CassandraClient

import colorama
from colorama import Fore
colorama.init(strip=False)

'''
POST /rating
GET, DELETE /ratings
GET /avg-genre-ratings/all-users, /avg-genre-ratings/user<userID>
'''


class ApiLogic:
    def __init__(self, host='192.168.137.1', port=9043, file_records=None):
        self.cassandra_client = CassandraClient(host, port, 'ratings', 'ratings_all')
        print(Fore.YELLOW + 'Cassandra loading...')

        if not self.cassandra_client.pull_data_table('ratings')[0]:
            print(Fore.RED + 'Cassandra is empty')
            print(Fore.YELLOW + 'loading from file...')

            _, self.rated_movies_with_genres_dict_list, self.genres_column_names = \
                get_rated_movies_with_genres(file_records)
            for index, rated_movies_with_genres_dict in enumerate(self.rated_movies_with_genres_dict_list):
                self.cassandra_client.push_data_table(index + 1, json.dumps(rated_movies_with_genres_dict))

            print(Fore.GREEN + 'Loaded {0} records to cassandra table'.format(file_records))
        else:
            self.rated_movies_with_genres_dict_list, self.genres_column_names = \
                self.cassandra_client.pull_data_table('ratings')
            print(Fore.GREEN + 'Cassandra loaded')

        self.cassandra_avg_all = CassandraClient(host, port, 'avg', 'avg_genre_ratings')
        self.cassandra_avg_user = CassandraClient(host, port, 'avg', 'avg_genre_ratings_for_user')

        self.cassandra_profiles_client = CassandraClient(host, port, 'avg', 'user_profile')

        self.avg_genre_ratings = {}
        self.avg_genre_ratings_for_user = {}
        self.user_profile = {}

    def add_rating(self, new_rating):
        self.rated_movies_with_genres_dict_list.append(new_rating)
        self.cassandra_client.push_data_table(self.cassandra_client.lastindex + 1, json.dumps(new_rating))

    def list_rating(self):
        self.rated_movies_with_genres_dict_list = self.cassandra_client.pull_data_table('ratings')[0]
        return self.rated_movies_with_genres_dict_list

    def delete_ratings(self):
        self.rated_movies_with_genres_dict_list = []
        self.cassandra_client.clear_table()

    def compute_avg_genre_ratings(self):
        rated_movies_with_genres_df = dict_list_to_df(self.cassandra_client.pull_data_table('ratings')[0])
        unpivoted_joined_df = pd.melt(rated_movies_with_genres_df, id_vars=['userID', 'movieID', 'rating'], var_name='genre')
        unpivoted_joined_df = unpivoted_joined_df[unpivoted_joined_df.value != 0]

        pivoted = unpivoted_joined_df.pivot_table(columns='genre', fill_value=0, aggfunc=np.mean, values="rating")

        for genre in self.genres_column_names:
            if genre not in pivoted.columns:
                pivoted.insert(len(pivoted.columns), genre, 0)

        # reordering column names
        pivoted = pivoted.reindex(sorted(pivoted.columns), axis=1)

        avg_genre_ratings_dict_list = df_to_dict_list(pivoted)
        self.avg_genre_ratings = avg_genre_ratings_dict_list

        self.cassandra_avg_all.push_data_table(1, json.dumps(avg_genre_ratings_dict_list))

        return avg_genre_ratings_dict_list, pivoted

    def compute_avg_genre_ratings_for_user(self, user_id):
        rated_movies_with_genres_df = dict_list_to_df(self.cassandra_client.pull_data_table('ratings')[0])
        unpivoted_joined_df = pd.melt(rated_movies_with_genres_df, id_vars=['userID', 'movieID', 'rating'],  var_name='genre')
        unpivoted_joined_df = unpivoted_joined_df[
            (unpivoted_joined_df.value != 0) & (unpivoted_joined_df.userID == user_id)]

        if unpivoted_joined_df.empty:
            return []

        pivoted = unpivoted_joined_df.pivot_table(columns='genre', fill_value=0, aggfunc=np.mean, values="rating")

        for genre in self.genres_column_names:
            if genre not in pivoted.columns:
                pivoted.insert(len(pivoted.columns), genre, 0)

        # reordering column names
        pivoted = pivoted.reindex(sorted(pivoted.columns), axis=1)

        avg_genre_ratings_for_user_list = df_to_dict_list(pivoted)
        self.avg_genre_ratings_for_user[user_id] = avg_genre_ratings_for_user_list

        self.cassandra_avg_user.push_data_table(user_id, json.dumps(avg_genre_ratings_for_user_list))

        return avg_genre_ratings_for_user_list, pivoted

    def compute_user_profile(self, user_id):
        _, avg_genre_ratings_df = self.compute_avg_genre_ratings()
        _, avg_genre_ratings_for_user_df = self.compute_avg_genre_ratings_for_user(user_id)

        self.avg_genre_ratings_for_user[user_id] = self.cassandra_avg_user.pull_avg_data_table('ratings', str(user_id))[0]
        self.avg_genre_ratings = self.cassandra_avg_all.pull_data_table('ratings')[0]

        self.user_profile[user_id] = df_to_dict_list(avg_genre_ratings_for_user_df.subtract(avg_genre_ratings_df).fillna(0))

        self.cassandra_profiles_client.push_data_table(user_id, json.dumps(self.user_profile[user_id]))

        return self.user_profile[user_id]

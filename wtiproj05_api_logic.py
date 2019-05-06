from wtiproj03_ETL import get_rated_movies_with_genres, join_df, dict_list_to_df, df_to_dict_list
import json
import pandas as pd
import numpy as np
from wtiproj05_redis_client import RedisClient
import colorama
from colorama import Fore
colorama.init(strip=False)

'''
POST /rating
GET, DELETE /ratings
GET /avg-genre-ratings/all-users, /avg-genre-ratings/user<userID>
'''


class ApiLogic:
    def __init__(self, host='localhost', port=6379, file_records=None):
        self.redis_client = RedisClient(host, port, 0)
        self.queue_name = 'ratings'
        print(Fore.YELLOW + 'Redis loading...')

        if self.redis_client.exists(self.queue_name) == 0:
            print(Fore.RED + 'Redis is empty')
            print(Fore.YELLOW + 'loading from file...')

            _, self.rated_movies_with_genres_dict_list, self.genres_column_names = get_rated_movies_with_genres(file_records)
            for rated_movies_with_genres_dict in self.rated_movies_with_genres_dict_list:
                self.redis_client.rpush(self.queue_name, rated_movies_with_genres_dict)

            print(Fore.GREEN + 'Loaded {0} records to redis queue'.format(file_records))
        else:
            self.rated_movies_with_genres_dict_list, self.genres_column_names = self.redis_client.pull_queue(self.queue_name)
            print(Fore.GREEN + 'Redis loaded')

        self.redis_ratings_client = RedisClient(host, port, 0)
        self.redis_profiles_client = RedisClient(host, port, 0)

        self.avg_genre_ratings = {}
        self.avg_genre_ratings_for_user = {}
        self.user_profile = {}

    def add_rating(self, new_rating):
        self.rated_movies_with_genres_dict_list.append(new_rating)
        for rated_movies_with_genres_dict in self.rated_movies_with_genres_dict_list:
            self.redis_client.rpush(self.queue_name, rated_movies_with_genres_dict)

    def list_rating(self):
        self.rated_movies_with_genres_dict_list = self.redis_client.pull_queue(self.queue_name)[0]
        return self.rated_movies_with_genres_dict_list

    def delete_ratings(self):
        self.rated_movies_with_genres_dict_list = []
        self.redis_ratings_client.clear_queue(self.queue_name)

    def compute_avg_genre_ratings(self):
        rated_movies_with_genres_df = dict_list_to_df(self.redis_client.pull_queue(self.queue_name)[0])
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

        self.redis_ratings_client.set('avg_genre_ratings', json.dumps(avg_genre_ratings_dict_list))

        return avg_genre_ratings_dict_list, pivoted

    def compute_avg_genre_ratings_for_user(self, user_id):
        rated_movies_with_genres_df = dict_list_to_df(self.redis_client.pull_queue(self.queue_name)[0])
        unpivoted_joined_df = pd.melt(rated_movies_with_genres_df, id_vars=['userID', 'movieID', 'rating'], var_name='genre')
        unpivoted_joined_df = unpivoted_joined_df[(unpivoted_joined_df.value != 0) & (unpivoted_joined_df.userID == user_id)]

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

        self.redis_ratings_client.set(
            'avg_genre_ratings_for_user_' + str(user_id),
            json.dumps(avg_genre_ratings_for_user_list)
        )

        return avg_genre_ratings_for_user_list, pivoted

    def compute_user_profile(self, user_id):
        _, avg_genre_ratings_df = self.compute_avg_genre_ratings()
        _, avg_genre_ratings_for_user_df = self.compute_avg_genre_ratings_for_user(user_id)

        self.avg_genre_ratings_for_user[user_id] = self.redis_profiles_client.get('avg_genre_ratings_for_user_' + str(user_id))
        self.avg_genre_ratings = self.redis_profiles_client.get('avg_genre_ratings')

        self.user_profile[user_id] = df_to_dict_list(avg_genre_ratings_for_user_df.subtract(avg_genre_ratings_df).fillna(0))

        self.redis_profiles_client.set(
            'user_profile' + str(user_id),
            json.dumps(self.user_profile[user_id])
        )

        return self.user_profile[user_id]

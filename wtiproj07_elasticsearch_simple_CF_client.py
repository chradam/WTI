import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# pd.set_option('display.max_columns', 2000)
# pd.set_option('display.max_rows', 2000)

class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)
        # ------ Simple operations ------

    def __find_n_neighbours(self, df, n):
        order = np.argsort(df.values, axis=1)[:, :n]
        df = df.apply(lambda x: pd.Series(x.sort_values(ascending=False)
                                          .iloc[:n].index,
                                          index=['top{}'.format(i) for i in range(1, n + 1)]), axis=1)
        return df

    def index_documents(self):
        df = pd \
            .read_csv('user_ratedmovies.dat', delimiter='\t') \
            .loc[:, ['userID', 'movieID', 'rating']]

        means = df.groupby(['userID'], as_index=False, sort=False) \
            .mean() \
            .loc[:, ['userID', 'rating']] \
            .rename(columns={'rating': 'ratingMean'})

        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']

        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']] \
            .rename(columns={'ratingNormal': 'rating'}) \
            .pivot_table(index='userID', columns='movieID', values='rating')\
            .fillna(0)

        # print(ratings)
        # df to array
        # all_users = ratings.values
        # user_75 = all_users[0]
        # # print(user_75)
        # denominator1 = np.sqrt(sum([np.square(x) for x in user_75]))
        # cosine_similarity = [(75, 1)]
        # i = 1
        # for user in all_users[1:]:
        #     numerator = [x*y for x, y in zip(user_75, user)]
        #     denominator2 = np.sqrt(sum([np.square(x) for x in user]))
        #     costheta = sum(numerator) / (denominator1 * denominator2)
        #     cosine_similarity.append((ratings.index[i], costheta))
        #     i += 1
        #
        # cosine_similarity.sort(key=lambda x: x[1], reverse=True)
        #
        # print(cosine_similarity[0:10])

        # user similarity
        cosine = cosine_similarity(ratings)
        np.fill_diagonal(cosine, 0)

        # user similarity df
        similarity_with_user = pd.DataFrame(cosine, index=ratings.index)
        similarity_with_user.columns = ratings.index

        sim_user_30_u = self.__find_n_neighbours(similarity_with_user, 30)
        #
        #
        # xd = ratings.loc[75]
        # xd = xd
        # print(xd)
        # score = []
        # c = ratings.loc[:, 2140]
        # c = c[c.ne(0)]
        # print(c)
        # d = c[c.index.isin([71534, 71509])]
        # f = d[d.notnull()]
        # index = f.index.values.tolist()
        # print(f)
        # # indeksy wszystkicj z user_similarity
        # print(index)
        # # print(d[d.notnull()])
        # # średia bez normalizacji dla użytkownika
        # avg_user = df.loc[df['userID'] == 75, 'ratingMean'].values[0]
        # print(avg_user)
        # corr = similarity_with_user.loc[75, index]
        # print(corr)
        # fin = pd.concat([f, corr], axis=1)
        # print(fin)
        # fin.columns = ['adg_score', 'correlation']
        # fin['score'] = fin.apply(lambda x:x['adg_score'] * x['correlation'], axis=1)
        # nume = fin['score'].sum()
        # deno = fin['correlation'].sum()
        # final_socre= avg_user + (nume/deno)
        # score.append(final_socre)
        # print(final_socre)


        # print("Indexing similar users...")
        #
        # index_similar_users = [{
        #     "_index": "user_similarity",
        #     "_type": "similarity",
        #     "_id": index,
        #     "_source": {
        #         'similar': tuple(
        #             zip(
        #                 similarity_with_user.loc[index, row.tolist()].index,
        #                 similarity_with_user.loc[index, row.tolist()]
        #             )
        #         )
        #     }
        # } for index, row in sim_user_30_u.iterrows()]
        # helpers.bulk(self.es, index_similar_users)
        # print("Done")

        # user = int(input("Enter the user id to whom you want to recommend : "))
        # predicted_movies = User_item_score1(user)
        # print(" ")
        # print("The Recommendations for User Id : %s", user)
        # print("   ")
        # for i in predicted_movies:
        #     print(i)
        #
        # a = get_user_similar_movies(370, 86309)
        # a = a.loc[:, ['rating_x_x', 'rating_x_y', 'title']]
        # a.head()

        # print("Indexing user watched movies...")
        #
        # index_user_watched = [{
        #     "_index": "user_watched",
        #     "_type": "watched",
        #     "_id": index,
        #     "_source": {
        #         'watched': tuple(
        #             zip(
        #                 row[row != 0].index,
        #                 row[row != 0]
        #             )
        #         )
        #     }
        # } for index, row in ratings.iterrows()]
        # helpers.bulk(self.es, index_user_watched)
        # print("Done")

        print("Indexing users...")

        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0] \
                    .sort_values(ascending=False) \
                    .index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")

        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] > 0] \
                    .sort_values(ascending=False) \
                    .index.values.tolist()
            }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

        # print("Indexing movie watched...")
        # index_movie_watched = [{
        #     "_index": "movie_watched",
        #     "_type": "watched",
        #     "_id": column,
        #     "_source": {
        #         "whoWatched": tuple(
        #             zip(
        #                 ratings[column][ratings[column] != 0].index,
        #                 ratings[column][ratings[column] != 0]
        #             )
        #         )
        #     }
        # } for column in ratings]
        # helpers.bulk(self.es, index_movie_watched)
        # print("Done")


    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_movies_watched_by_user(self, user_id, index='user_watched'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="watched", id=user_id)["_source"]

    def get_similar_users(self, user_id, index='user_similarity'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="similarity", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def get_users_that_watched_movie(self, movie_id, index='movie_watched'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="watched", id=movie_id)["_source"]

    # # metoda preselekcyjna
    # # zwraca filmy do rekomendacji dla użytkownika o podanym ID
    # # są to filmy nieocenione przez użytkownika, czyli równe zero
    #
    def get_preselection_for_user(self, user_id, index='users'):
        user_id = int(user_id)
        movies_rated_by_user = self.es.search(
            index=index,
            body={
                "query": {
                    "term": {
                        "_id": user_id
                    }
                }
            }
        )["hits"]["hits"][0]["_source"]["ratings"]
        # print(movies_rated_by_user)
        # print()
        users_that_rated_at_least_one_movie_from_the_given_set_of_movies = self.es.search(
            index=index,
            body={
                "query": {
                    "terms": {
                        "ratings": movies_rated_by_user
                    }
                }
            }
        )["hits"]["hits"]
        # print(users_that_rated_at_least_one_movie_from_the_given_set_of_movies)

        unique_movies = set()

        for ratings in users_that_rated_at_least_one_movie_from_the_given_set_of_movies:
            if ratings["_id"] != user_id:
                ratings = ratings["_source"]["ratings"]
                for rating in ratings:
                    if rating not in movies_rated_by_user:
                        unique_movies.add(rating)
        return list(unique_movies)






        # ---------------------------------------------------------------------

        # movies_watched_by_user = self.get_movies_watched_by_user(user_id)['watched']
        # movie_ids_watched_by_user = [x[0] for x in movies_watched_by_user]
        # movie_ratings_watched_by_user = [x[-1] for x in movies_watched_by_user]
        #
        # similar_users = self.get_similar_users(user_id)['similar']
        # similar_user_ids = [x[0] for x in similar_users]
        # similar_user_corrs = [x[-1] for x in similar_users]
        #
        # movie_ids_watched_by_similar_users = []
        # for similar_user_id in similar_user_ids:
        #     movies_watched_by_similar_user = self.get_movies_watched_by_user(similar_user_id)['watched']
        #     movie_ids_watched_by_similar_user_temp = [x[0] for x in movies_watched_by_similar_user]
        #     movie_ids_watched_by_similar_users.extend(movie_ids_watched_by_similar_user_temp)
        #
        # print(len(list(set(movie_ids_watched_by_similar_users))))
        # movies_under_consideration = list(set(movie_ids_watched_by_similar_users) - set(movie_ids_watched_by_user))
        # print(len(movies_under_consideration))

        # --------------------------------------------------------------

        # a = sim_user_30_m[sim_user_30_m.index == user].values
        # b = a.squeeze().tolist()
        # d = Movie_user[Movie_user.index.isin(b)]
        # l = ','.join(d.values)
        # Movie_seen_by_similar_users = l.split(',')
        # Movies_under_consideration = list(
        #     set(Movie_seen_by_similar_users) - set(list(map(str, Movie_seen_by_user))))
        # Movies_under_consideration = list(map(int, Movies_under_consideration))

        #  ----------------------------------------
        # users_that_watched_movie = self.get_users_that_watched_movie(2140)['whoWatched']
        # # user_ids_that_watched_movie = [x[0] for x in users_that_watched_movie]
        # user_normal_rating_that_watched_movie = [(x[0], x[-1]) for x in users_that_watched_movie if x[0] in [71055, 71420, 71509]]
        # print(user_normal_rating_that_watched_movie)
        # ---------------------------------------

        # xd = ratings.loc[75]
        # xd = xd
        # print(xd)
        # score = []
        # c = ratings.loc[:, 2140]
        # c = c[c.ne(0)]
        # print(c)
        # d = c[c.index.isin([71534, 71509])]
        # f = d[d.notnull()]
        # index = f.index.values.tolist()
        # print(f)
        # # indeksy wszystkicj z user_similarity
        # print(index)
        # # print(d[d.notnull()])
        # # średia bez normalizacji dla użytkownika
        # avg_user = df.loc[df['userID'] == 75, 'ratingMean'].values[0]
        # print(avg_user)
        # corr = similarity_with_user.loc[75, index]
        # print(corr)
        # fin = pd.concat([f, corr], axis=1)
        # print(fin)
        # fin.columns = ['adg_score', 'correlation']
        # fin['score'] = fin.apply(lambda x: x['adg_score'] * x['correlation'], axis=1)
        # nume = fin['score'].sum()
        # deno = fin['correlation'].sum()
        # final_socre = avg_user + (nume / deno)
        # score.append(final_socre)
        # print(final_socre)


        # score = []
        # for item in Movies_under_consideration:
        #     c = final_movie.loc[:, item]
        #     d = c[c.index.isin(b)]
        #     f = d[d.notnull()]
        #     avg_user = Mean.loc[Mean['userId'] == user, 'rating'].values[0]
        #     index = f.index.values.squeeze().tolist()
        #     corr = similarity_with_movie.loc[user, index]
        #     fin = pd.concat([f, corr], axis=1)
        #     fin.columns = ['adg_score', 'correlation']
        #     fin['score'] = fin.apply(lambda x: x['adg_score'] * x['correlation'], axis=1)
        #     nume = fin['score'].sum()
        #     deno = fin['correlation'].sum()
        #     final_score = avg_user + (nume / deno)
        #     score.append(final_score)
        # data = pd.DataFrame({'movieId': Movies_under_consideration, 'score': score})
        # top_5_recommendation = data.sort_values(by='score', ascending=False).head(5)
        # Movie_Name = top_5_recommendation.merge(movies, how='inner', on='movieId')
        # Movie_Names = Movie_Name.title.values.tolist()
        # return Movie_Names

    def get_preselection_for_movie(self, movie_id, index="movies"):
        movie_id = int(movie_id)

        users_that_rated_movie = self.es.search(
            index=index,
            body={
                "query": {
                    "term": {
                        "_id": movie_id
                    }
                }
            }
        )["hits"]["hits"][0]["_source"]["whoRated"]
        # print(users_that_rated_movie)
        # print()

        movies_that_was_rated_at_least_once_by_user_from_the_given_set_of_users = self.es.search(
            index=index,
            body={
                "query": {
                    "terms": {
                        "whoRated": users_that_rated_movie
                    }
                }
            }
        )["hits"]["hits"]
        # print(movies_that_was_rated_at_least_once_by_user_from_the_given_set_of_users)

        unique_users = set()

        for user_rates in movies_that_was_rated_at_least_once_by_user_from_the_given_set_of_users:
            if user_rates["_id"] != movie_id:
                user_rates = user_rates["_source"]["whoRated"]
                for user_rate in user_rates:
                    if user_rate not in users_that_rated_movie:
                        unique_users.add(user_rate)

        print(list(unique_users))


if __name__ == "__main__":
    ec = ElasticClient()
    # ec.index_documents()

    # ------ Simple operations ------
    # print()
    # user_document = ec.get_movies_liked_by_user(75)
    # movie_id = np.random.choice(user_document['ratings'])
    # movie_document = ec.get_users_that_like_movie(movie_id)
    # random_user_id = np.random.choice(movie_document['whoRated'])
    # random_user_document = ec.get_movies_liked_by_user(random_user_id)
    # print('User 75 likes following movies:')
    # print(user_document)
    # print('Movie {} is liked by following users:'.format(movie_id))
    # print(movie_document)
    # print('Is user 75 among users in movie {} document?'.format(movie_id))
    # print(movie_document['whoRated'].index(75) != -1)
    #
    # import random
    #
    # some_test_movie_ID = 1
    # print("Some test movie ID: ", some_test_movie_ID)
    #
    # list_of_users_who_liked_movie_of_given_ID = ec.get_users_that_like_movie(some_test_movie_ID)["whoRated"]
    # print("List of users who liked the test movie: ", *list_of_users_who_liked_movie_of_given_ID)
    #
    # index_of_random_user_who_liked_movie_of_given_ID = random.randint(0, len(list_of_users_who_liked_movie_of_given_ID))
    # print("Index of random user who liked the test movie: ",
    #       index_of_random_user_who_liked_movie_of_given_ID)
    #
    # some_test_user_ID = list_of_users_who_liked_movie_of_given_ID[index_of_random_user_who_liked_movie_of_given_ID]
    # print("ID of random user who liked the test movie: ", some_test_user_ID)
    #
    # movies_liked_by_user_of_given_ID = ec.get_movies_liked_by_user(some_test_user_ID)["ratings"]
    # print("IDs of movies liked by the random user who liked the test movie: ",
    #       *movies_liked_by_user_of_given_ID)
    #
    # if some_test_movie_ID in movies_liked_by_user_of_given_ID:
    #     print("As expected, the test movie ID is among the IDs of movies " +
    #           "liked by the random user who liked the test movie ;-)")

    # ec.get_preselection_for_user(75)
    ec.get_preselection_for_movie(3)
    # print(ec.get_movies_watched_by_user(75))
    # print(ec.get_users_that_watched_movie(3))
    # print([item[-1] for item in ec.get_similar_users(75)['similar']])

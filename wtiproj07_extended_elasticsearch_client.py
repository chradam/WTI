import pandas as pd
from elasticsearch import Elasticsearch, helpers

# pd.set_option('display.max_columns', 2000)
# pd.set_option('display.max_rows', 2000)

class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

    # HELPERS
    def index_exist(self, index):
        return self.es.indices.exists(index=index)

    def already_created(self, index, id):
        already_created = self.es.count(
            index=index,
            body={"query": {"match": {"_id": id}}}
        )
        return 1 if already_created["count"] > 0 else 0

    # ------ Simple operations ------
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

    def get_movies_liked_by_user(self, user_id, index='users'):
        if self.index_exist(index) == 1:
            user_id = int(user_id)
            if self.already_created(index, user_id) == 1:
                return self.es.get(index=index, doc_type="user", id=user_id)["_source"]
            return "User document doesn't exists"
        return "Index doesn't exists"

    def get_users_that_like_movie(self, movie_id, index='movies'):
        if self.index_exist(index) == 1:
            movie_id = int(movie_id)
            if self.already_created(index, movie_id) == 1:
                return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]
            return "Movie document doesn't exists"
        return "Index doesn't exists"

    # ------ Preselection ------
    def get_preselection_for_user(self, user_id, index='users'):
        user_id = int(user_id)
        movies_rated_by_user = self.es.search(
            index=index,
            body={"query": {"term": {"_id": user_id}}}
        )["hits"]["hits"][0]["_source"]["ratings"]

        users_that_rated_at_least_one_movie_from_the_given_set_of_movies = self.es.search(
            index=index,
            body={"query": {"terms": {"ratings": movies_rated_by_user}}}
        )["hits"]["hits"]

        unique_movies = set()
        for ratings in users_that_rated_at_least_one_movie_from_the_given_set_of_movies:
            if ratings["_id"] != user_id:
                ratings = ratings["_source"]["ratings"]
                for rating in ratings:
                    if rating not in movies_rated_by_user:
                        unique_movies.add(rating)
        return list(unique_movies)

    def get_preselection_for_movie(self, movie_id, index="movies"):
        movie_id = int(movie_id)
        users_that_rated_movie = self.es.search(
            index=index,
            body={"query": {"term": {"_id": movie_id}}}
        )["hits"]["hits"][0]["_source"]["whoRated"]

        movies_that_was_rated_at_least_once_by_user_from_the_given_set_of_users = self.es.search(
            index=index,
            body={"query": {"terms": {"whoRated": users_that_rated_movie}}}
        )["hits"]["hits"]

        unique_users = set()
        for user_rates in movies_that_was_rated_at_least_once_by_user_from_the_given_set_of_users:
            if user_rates["_id"] != movie_id:
                user_rates = user_rates["_source"]["whoRated"]
                for user_rate in user_rates:
                    if user_rate not in users_that_rated_movie:
                        unique_users.add(user_rate)

        return list(unique_users)

    # ------ Add/Update/Delete ------
    #DONE
    def add_user_document(self, user_id, movies_liked_by_user, user_index='users', movie_index='movies'):
        if self.index_exist(user_index) == 1:
            if self.already_created(user_index, user_id) == 1:
                raise Exception("Error! User already added")

        user_id = int(user_id)
        movies_liked_by_user = list(set(movies_liked_by_user))

        movies_to_update = []
        if self.index_exist(movie_index) == 1:
            for movie_id in movies_liked_by_user:
                movie_to_update = self.es.search(
                    index=movie_index,
                    body={"query": {"term": {"_id": movie_id}}}
                )
                if movie_to_update["hits"]["total"] == 1:
                    movies_to_update.append(movie_to_update["hits"]["hits"][0])

        if len(movies_to_update) != len(movies_liked_by_user):
            raise Exception("Error! User can't like unknown movie")

        for movie_doc in movies_to_update:
            users_who_liked_movie = movie_doc["_source"]["whoRated"]
            users_who_liked_movie.append(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(
                index=movie_index,
                id=int(movie_doc["_id"]),
                doc_type="movie",
                body={"doc": {"whoRated": users_who_liked_movie}}
            )

        self.es.create(
            index=user_index,
            id=user_id,
            body={"ratings": movies_liked_by_user},
            doc_type="user"
        )

    # DONE
    def add_movie_document(self, movie_id, users_who_liked_movie, user_index='users', movie_index='movies'):
        if self.index_exist(movie_index) == 1:
            if self.already_created(movie_index, movie_id) == 1:
                raise Exception("Error! Movie already added")

        movie_id = int(movie_id)
        users_who_liked_movie = list(set(users_who_liked_movie))

        users_to_update = []
        if self.index_exist(user_index) == 1:
            for user_id in users_who_liked_movie:
                user_to_update = self.es.search(
                    index=user_index,
                    body={"query": {"term": {"_id": user_id}}}
                )
                if user_to_update["hits"]["total"] == 1:
                    users_to_update.append(user_to_update["hits"]["hits"][0])

        if len(users_to_update) != len(users_who_liked_movie):
            raise Exception("Error! Movie can't be liked by unknown user")

        for user_doc in users_to_update:
            movies_liked_by_user = user_doc["_source"]["ratings"]
            movies_liked_by_user.append(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(
                index=user_index,
                id=int(user_doc["_id"]),
                doc_type="user",
                body={"doc": {"ratings": movies_liked_by_user}}
            )
        self.es.create(
            index=movie_index,
            id=movie_id,
            body={"whoRated": users_who_liked_movie},
            doc_type="movie"
        )

    # DONE
    def update_user_document(self, user_id, movies_liked_by_user, user_index='users', movie_index='movies'):
        user_id = int(user_id)
        movies_liked_by_user = list(set(movies_liked_by_user))

        if self.index_exist(user_index) == 0:
            self.add_user_document(user_id, movies_liked_by_user, user_index, movie_index)
        else:
            if self.already_created(user_index, user_id) == 0:
                self.add_user_document(user_id, movies_liked_by_user, user_index, movie_index)
            else:
                movies_to_update = []
                if self.index_exist(movie_index) == 1:
                    for movie_id in movies_liked_by_user:
                        movie_to_update = self.es.search(
                            index=movie_index,
                            body={"query": {"term": {"_id": movie_id}}}
                        )
                        if movie_to_update["hits"]["total"] == 1:
                            movies_to_update.append(movie_to_update["hits"]["hits"][0])

                if len(movies_to_update) != len(movies_liked_by_user):
                    raise Exception("Error! User can't like unknown movie")

                for movie_doc in movies_to_update:
                    users_who_liked_movie = movie_doc["_source"]["whoRated"]
                    users_who_liked_movie.append(user_id)
                    users_who_liked_movie = list(set(users_who_liked_movie))
                    self.es.update(
                        index=movie_index,
                        id=int(movie_doc["_id"]),
                        doc_type="movie",
                        body={"doc": {"whoRated": users_who_liked_movie}}
                    )

                movies_already_liked_by_user = self.es.search(
                    index=user_index,
                    body={"query": {"term": {"_id": user_id}}}
                )["hits"]["hits"][0]["_source"]["ratings"]

                movies_liked_by_user.extend(movies_already_liked_by_user)
                movies_liked_by_user = list(set(movies_liked_by_user))

                self.es.update(
                    index=user_index,
                    id=user_id,
                    doc_type="user",
                    body={"doc": {"ratings": movies_liked_by_user}}
                )

    # DONE
    def update_movie_document(self, movie_id, users_who_liked_movie, user_index='users', movie_index='movies'):
        movie_id = int(movie_id)
        users_who_liked_movie = list(set(users_who_liked_movie))

        if self.index_exist(movie_index) == 0:
            self.add_user_document(movie_id, users_who_liked_movie, user_index, movie_index)
        else:
            if self.already_created(movie_index, movie_id) == 0:
                self.add_user_document(movie_id, users_who_liked_movie, user_index, movie_index)
            else:
                users_to_update = []
                if self.index_exist(user_index) == 1:
                    for user_id in users_who_liked_movie:
                        user_to_update = self.es.search(
                            index=user_index,
                            body={"query": {"term": {"_id": user_id}}}
                        )
                        if user_to_update["hits"]["total"] == 1:
                            users_to_update.append(user_to_update["hits"]["hits"][0])

                if len(users_to_update) != len(users_who_liked_movie):
                    raise Exception("Error! Movie can't be liked by unknown user")

                for user_doc in users_to_update:
                    movies_liked_by_user = user_doc["_source"]["ratings"]
                    movies_liked_by_user.append(movie_id)
                    movies_liked_by_user = list(set(movies_liked_by_user))
                    self.es.update(
                        index=user_index,
                        id=int(user_doc["_id"]),
                        doc_type="user",
                        body={"doc": {"ratings": movies_liked_by_user}}
                    )

                users_who_already_liked_movie = self.es.search(
                    index=movie_index,
                    body={"query": {"term": {"_id": movie_id}}}
                )["hits"]["hits"][0]["_source"]["whoRated"]

                users_who_liked_movie.extend(users_who_already_liked_movie)
                users_who_liked_movie = list(set(users_who_liked_movie))

                self.es.update(
                    index=movie_index,
                    id=movie_id,
                    body={"doc": {"whoRated": users_who_liked_movie}},
                    doc_type="movie"
                )

    # DONE
    def delete_user_document(self, user_id, user_index='users', movie_index='movies'):
        if self.index_exist(user_index) == 0:
            raise Exception("Error! Index doesn't exists")
        if self.already_created(user_index, user_id) == 0:
            raise Exception("Error! User document doesn't exists")

        user_id = int(user_id)

        if self.index_exist(movie_index) == 1:
            movies_already_liked_by_user = \
                self.es.get(index=user_index, doc_type="user", id=user_id)["_source"]["ratings"]
            for movie_id in movies_already_liked_by_user:
                users_who_liked_movie = \
                    self.es.get(index=movie_index, doc_type="movie", id=movie_id)["_source"]["whoRated"]
                users_who_liked_movie.remove(user_id)

                self.es.update(
                    index=movie_index,
                    id=movie_id,
                    doc_type="movie",
                    body={"doc": {"whoRated": users_who_liked_movie}}
                )

        self.es.delete(index=user_index, doc_type="user", id=user_id)

    # DONE
    def delete_movie_document(self, movie_id, user_index='users', movie_index='movies'):
        if self.index_exist(movie_index) == 0:
            raise Exception("Error! Index doesn't exists")
        if self.already_created(movie_index, movie_id) == 0:
            raise Exception("Error! Movie document doesn't exists")

        movie_id = int(movie_id)

        if self.index_exist(user_index) == 1:
            users_who_liked_movie = \
                self.es.get(index=movie_index, doc_type="movie", id=movie_id)["_source"]["whoRated"]
            for user_id in users_who_liked_movie:
                movies_already_liked_by_user = \
                    self.es.get(index=user_index, doc_type="user", id=user_id)["_source"]["ratings"]
                movies_already_liked_by_user.remove(movie_id)

                self.es.update(
                    index=user_index,
                    id=user_id,
                    doc_type="user",
                    body={"doc": {"ratings": movies_already_liked_by_user}}
                )

        self.es.delete(index=movie_index, doc_type="movie", id=movie_id)

    def bulk_user_update(self, body, index):
        if self.index_exist(index) == 0:
            raise Exception("Error! Index doesn't exists")
        for bulk_dict in body:
            user_id = bulk_dict['user_id']
            liked_movies = bulk_dict['liked_movies']
            self.update_user_document(user_id, liked_movies)

    def bulk_update_movies(self, body, index):
        if self.index_exist(index) == 0:
            raise Exception("Error! Index doesn't exists")
        for bulk_dict in body:
            movie_id = bulk_dict['movie_id']
            users_who_liked_movie = bulk_dict['liked_movies']
            self.update_movie_document(movie_id, users_who_liked_movie)

    def get_list_of_indices(self):
        return list(self.es.indices.get_alias().keys())

    def add_new_index(self, new_index):
        if self.index_exist(new_index):
            raise ValueError("Index {0} already exist!".format(new_index))
        return self.es.indices.create(index=new_index)

    def delete_index(self, index):
        if not self.index_exist(index):
            raise ValueError("Index {0} does not exist!".format(index))
        return self.es.indices.delete(index=index)


    def reindex(self, body_dict):
        source_index = body_dict['source']
        if not self.index_exist(source_index):
            raise ValueError("Index {0} does not exist!".format(source_index))

        dest_index = body_dict['dest']
        if self.index_exist(dest_index):
            return "Index {0} already exists. ".format(dest_index)
        else:
            self.add_new_index(dest_index)
        helpers.reindex(self.es, source_index=source_index, target_index=dest_index)
        self.delete_index(source_index)



if __name__ == "__main__":
    ec = ElasticClient()
    # ec.index_documents()

    # ------ Simple operations ------
    # print()

    # ec.get_preselection_for_user(75)
    # ec.get_preselection_for_movie(3)

    # ec.add_movie_document(11, [1]) #good
    # ec.add_movie_document(22, []) #good
    # ec.add_user_document(1, []) #good
    # ec.update_user_document(1,[11, 22, 33]) #error
    # ec.update_user_document(1, [11, 22])  # good
    # ec.update_user_document(2,[11, 22]) #good
    # ec.add_user_document(3, [11,22]) #good
    # ec.update_movie_document(11, [3]) #good
    # ec.delete_user_document(3) #good
    # ec.delete_movie_document(22) #good
    # ec.get_list_of_indices() #good
    # ec.add_new_index('asd')
    # print(ec.reindex({'source': 'asd', 'dest': 'dsa'}))
    # print(ec.delete_index('dsa'))
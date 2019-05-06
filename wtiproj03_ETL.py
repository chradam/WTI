import pandas as pd
import numpy as np
import json

AVG_ALL_GENRES_COMPUTED = {
    'genre-Action': 3.3386521960487268, 'genre-Adventure': 3.401006543087401, 'genre-Animation': 3.5264503462708463,
    'genre-Children': 3.31040959493098, 'genre-Comedy': 3.3468511979936943, 'genre-Crime': 3.601391912146844,
    'genre-Documentary': 3.7068395137713495, 'genre-Drama': 3.608433954550544, 'genre-Fantasy': 3.435113355873141,
    'genre-Film-Noir': 3.925190019828156, 'genre-Horror': 3.2034112714092813, 'genre-IMAX': 3.7571906354515052,
    'genre-Musical': 3.4701565824649463, 'genre-Mystery': 3.608120688222093, 'genre-Romance': 3.466619030839304,
    'genre-Sci-Fi': 3.3251162753270997, 'genre-Short': 4.0, 'genre-Thriller': 3.431804492435667,
    'genre-War': 3.6639329883343565, 'genre-Western': 3.5375375375375375
}


def get_rated_movies_df(path, nrows=None):
    rated_movies_df = pd.read_csv(path, header=0,
                                  delimiter="\t", usecols=['userID', 'movieID', 'rating'],
                                  dtype={'userID': np.uint64, 'movieID': np.uint64, 'rating': np.float32},
                                  nrows=nrows)
    return rated_movies_df


def get_movie_genres_df(path, nrows=None):
    movie_genres_df = pd.read_csv(path, header=0,
                                  delimiter="\t", usecols=['movieID', 'genre'],
                                  dtype={'movieID': np.uint64, 'genre': np.str},
                                  nrows=nrows)
    return movie_genres_df


def join_df(rated_movies_path, movie_genres_path):
    # get dataframes
    rated_movies_df = get_rated_movies_df(rated_movies_path)
    movie_genres_df = get_movie_genres_df(movie_genres_path)

    # transform dataframes
    movie_genres_df['dummy_column'] = 1
    movie_genres_df_pivoted = movie_genres_df.pivot_table(index="movieID", columns="genre",
                                                          values="dummy_column").add_prefix("genre-")
    movie_genres_df_pivoted = movie_genres_df_pivoted.fillna(0)
    movie_genres_df_pivoted = movie_genres_df_pivoted.astype(int)

    rated_movies_with_genres_df = pd.merge(rated_movies_df, movie_genres_df_pivoted, on='movieID')

    rated_movies_with_genres_df.to_csv("rated_movies_with_genres_df.dat", sep="\t", index=False)
    # get genre names from columns with genre-prefix
    genre_names = list(rated_movies_with_genres_df)[3:]

    rated_movies_with_genres_dict_list = df_to_dict_list(rated_movies_with_genres_df)

    return rated_movies_with_genres_df, rated_movies_with_genres_dict_list, genre_names


def get_rated_movies_with_genres(nrows=None, skip=[]):
    rated_movies_with_genres_df = pd.read_csv("rated_movies_with_genres_df.dat", header=0, delimiter="\t",
                                              dtype={'userID': np.uint64, 'movieID': np.uint64, 'rating': np.float32},
                                              nrows=nrows, usecols=lambda x: x not in skip)

    rated_movies_with_genres_df = rated_movies_with_genres_df.astype(object)

    genre_names = list(rated_movies_with_genres_df)[3:]

    ''' Method with iterator '''
    # rated_movies_with_genres_dict_list = []
    # row_iterator = rated_movies_with_genres_df.iterrows()

    # for index, row in row_iterator:
    # print(json.loads(row.to_json()))
    # rated_movies_with_genres_dict_list.append(json.loads(row.to_json()))

    ''' New method '''
    rated_movies_with_genres_dict_list = df_to_dict_list(rated_movies_with_genres_df)

    return rated_movies_with_genres_df, rated_movies_with_genres_dict_list, genre_names


def get_avg_for_all_genres():
    rated_movies_df = get_rated_movies_df('user_ratedmovies.dat')
    movie_genres_df = get_movie_genres_df('movie_genres.dat')

    joined = pd.merge(rated_movies_df, movie_genres_df, on="movieID")
    pivoted = joined.pivot_table(columns='genre', fill_value=0, aggfunc=np.mean, values="rating").add_prefix("genre-")

    return pivoted


def get_avg_for_user_genres(user_id):
    rated_movies_df = get_rated_movies_df('user_ratedmovies.dat')
    movie_genres_df = get_movie_genres_df('movie_genres.dat')

    joined = pd.merge(rated_movies_df[rated_movies_df.userID == user_id], movie_genres_df, on="movieID")
    pivoted = joined.pivot_table(columns='genre', fill_value=0, aggfunc=np.mean, values="rating").add_prefix("genre-")

    return pivoted


def get_user_profile_vector(user_mean, genres_mean):
    return user_mean.subtract(genres_mean).fillna(0)


def dict_list_to_df(dict_list):
    return pd.DataFrame(dict_list)


def df_to_dict_list(df):
    return df.to_dict('records')


def get_avg_rating_by_genre():
    ''' Not used any more '''

    rated_movies_with_genres_df, _, genres_column_names = get_rated_movies_with_genres(100, skip=[])
    avg = get_avg_for_all_genres()

    rated_movies_df = get_rated_movies_df('user_ratedmovies.dat', 100)
    movie_genres_df = get_movie_genres_df('movie_genres.dat', 100)
    joined = pd.merge(rated_movies_df, movie_genres_df, on='movieID')

    for genre in genres_column_names:
        joined.loc[('genre-' + joined.genre) == genre, 'rating'] = joined.loc[
            ('genre-' + joined.genre) == genre, 'rating'].map(
            lambda x: x - avg[genre])

    return joined


def avg_rating_by_genre_by_user_id(user_id, nrows=None):

    ''' Not used any more '''

    rated_movies_with_genres_df, rated_movies_with_genres, genres_column_names = get_rated_movies_with_genres(nrows, skip=[])

    ''' New method '''

    genres_avg_dict = {}
    user = rated_movies_with_genres_df['userID'] == user_id
    for genre in genres_column_names:
        mean_calculable = rated_movies_with_genres_df[genre] == 1
        genres_avg_dict[genre] = rated_movies_with_genres_df.loc[mean_calculable & user, 'rating'].mean()

    genres_avg_dict = {k: 0 if pd.isna(v) else v for k, v in genres_avg_dict.items()}

    '''New method'''
    avg_vector_np = np.array(list(genres_avg_dict.values()))

    rated_movies_with_genres_df_unbiased = rated_movies_with_genres_df.copy()

    for k, v in genres_avg_dict.items():
        rated_movies_with_genres_df_unbiased.loc[rated_movies_with_genres_df_unbiased[k] == 1, 'rating'] -= v

    # print(rated_movies_with_genres_df_unbiased)

    genres_avg_dict.update({'UserID': user_id})

    '''Old method'''

    # dictf = {}
    # key_frequency = {}
    # for rated_movie_with_genres in rated_movies_with_genres:
    #     for k, v in rated_movie_with_genres.items():
    #         if v != 0 and k != 'rating' and rated_movie_with_genres['userID'] == user_id:
    #             if k == 'userID':
    #                 continue
    #             elif k in dictf:
    #                 dictf[k] = dictf[k] + rated_movie_with_genres['rating']
    #                 key_frequency[k] += 1
    #             else:
    #                 dictf[k] = rated_movie_with_genres['rating']
    #                 key_frequency[k] = 1
    #
    # genres_avg_dict = {k: dictf[k] / key_frequency[k] for k in dictf if k in key_frequency}
    # genres_avg_dict['userID'] = user_id

    return genres_avg_dict, dict_list_to_df([genres_avg_dict]), avg_vector_np, rated_movies_with_genres_df_unbiased


if __name__ == '__main__':
    # df, dict_list, genre_names = join_df('user_ratedmovies.dat', 'movie_genres.dat')
    df, di, genre_names = get_rated_movies_with_genres(10)
    print(di)
    print(genre_names)

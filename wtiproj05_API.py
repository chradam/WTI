import json
from flask import Flask, request
# from wtiproj05_api_logic import ApiLogic
from wtiproj06_api_logic import ApiLogic

app = Flask(__name__)
api_logic = ApiLogic(file_records=10)


@app.route("/rating", methods=['POST'])
def save_rating():
    result = request.get_json()
    api_logic.add_rating(result)

    return json.dumps(result), 200, {'Content-Type': 'application/json'}


@app.route("/ratings", methods=['GET', 'DELETE'])
def get_or_delete_ratings():
    if request.method == 'GET':
        ratings = api_logic.list_rating()
        return json.dumps(ratings), 200, {'Content-Type': 'application/json'}
    if request.method == 'DELETE':
        api_logic.delete_ratings()

        return 'DELETED'


@app.route("/avg-genre-ratings/all-users", methods=['GET'])
def get_avg_genre_all_ratings():
    avg_genre_all_ratings, _ = api_logic.compute_avg_genre_ratings()
    return json.dumps(avg_genre_all_ratings), 200, {'Content-Type': 'application/json'}


@app.route("/avg-genre-ratings/<int:user_id>", methods=['GET'])
def get_avg_genre_user_ratings(user_id):
    avg_genre_user_ratings, _ = api_logic.compute_avg_genre_ratings_for_user(user_id)
    return json.dumps(avg_genre_user_ratings), 200, {'Content-Type': 'application/json'}


@app.route("/user-profile/<int:user_id>", methods=['GET'])
def get_user_profile(user_id):
    return json.dumps(api_logic.compute_user_profile(user_id)), 200, {'Content-Type': 'application/json'}


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=9875, debug=True)

    # example_rating = {"userID": 78, "movieID": 903, "rating": 5.0, "genre-Action": 0, "genre-Adventure": 0, "genre-Animation": 0,
    #                     "genre-Children": 0, "genre-Comedy": 0, "genre-Crime": 0, "genre-Documentary": 0, "genre-Drama": 1,
    #                     "genre-Fantasy": 0, "genre-Film-Noir": 0, "genre-Horror": 0, "genre-IMAX": 0, "genre-Musical": 0,
    #                     "genre-Mystery": 1, "genre-Romance": 1, "genre-Sci-Fi": 0, "genre-Short": 0, "genre-Thriller": 1, "genre-War":
    #                     0, "genre-Western": 0}

    # api_logic.add_rating(example_rating)
    # print(api_logic.list_rating())
    # api_logic.delete_ratings()
    # a, b = api_logic.compute_avg_genre_ratings()
    # print(a)
    # print(b)
    # c, d = api_logic.compute_avg_genre_ratings_for_user(75)
    # print(c)
    # profile =api_logic.compute_user_profile(78)
    # print(profile)

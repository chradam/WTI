from flask import Flask, jsonify, request
# from wtiproj03_ETL import *
import json
app = Flask(__name__)


# @app.route("/rating", methods=['POST'])
# def rating():
#     result = request.get_json()
#     return json.dumps(result), 200, {'Content-Type': 'application/json'}

# @app.route("/ratings", methods=['GET', 'DELETE'])
# def ratings():
#     if (request.method == 'GET'):
#         return json.dumps(get_rated_movies_with_genres(2)), 200, {'Content-Type': 'application/json'}
#     else:
#         return json.dumps({"http": "DELETE"})
#
# @app.route("/avg-genre-ratings/all-users")
# def avg():
#     # without 1 parameter - full list
#     return json.dumps(avg_reting_by_genre(200)), 200, {'Content-Type': 'application/json'}
#
# @app.route("/avg-genre-ratings/<int:num>")
# def user_avg(num):
#     # without 2 parameter - full list
#     return json.dumps(avg_reting_by_genre_by_user_id(num, 1500)), 200, {'Content-Type': 'application/json'}

#
if __name__ == "__main__":

    app.run(host='127.0.0.1', port=9875, debug=True)

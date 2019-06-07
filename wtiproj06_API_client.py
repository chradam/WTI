import requests
from time import sleep
import colorama
from colorama import Fore
colorama.init(strip=False)
# content_type_header = 'application/json'
# headers = {'Content-Type': content_type_header}


class ApiClient:
    def __init__(self, _url):
        self.url = _url

    def print_request(self, r):
        print('--------------------------------------')
        print(Fore.GREEN + 'request.url: {0}'.format(r.url))
        print(Fore.CYAN + 'request.status_code: {0}'.format(r.status_code))
        print(Fore.BLUE + 'request.headers: {0}'.format(r.headers))
        print(Fore.LIGHTBLUE_EX + 'request.request.headers: {0}'.format(r.request.headers))
        print(Fore.YELLOW + 'request.text: {0}'.format(r.text))
        print(Fore.LIGHTRED_EX + 'request.body: {0}'.format(r.request.body))

    def test_rating_post(self, payload):
        r = requests.post(
            url='{0}rating'.format(self.url),
            json=payload)
        self.print_request(r)

    def test_ratings_get(self):
        r = requests.get(url='{0}ratings'.format(self.url))
        self.print_request(r)

    def test_ratings_delete(self):
        r = requests.delete(url='{0}ratings'.format(self.url))
        self.print_request(r)

    def test_avg_all_users(self):
        r = requests.get(url='{0}avg-genre-ratings/all-users'.format(self.url))
        self.print_request(r)

    def test_avg_by_user(self, user_id):
        r = requests.get('{0}avg-genre-ratings/{1}'.format(self.url, user_id))
        self.print_request(r)

    def test_user_profile(self, user_id):
        r = requests.get('{0}user-profile/{1}'.format(self.url, user_id))
        self.print_request(r)


if __name__ == '__main__':
    HOST = '127.0.0.1'
    PORT = 9875
    BASE_URL = 'http://{0}:{1}/'.format(HOST, PORT)

    ac = ApiClient(BASE_URL)
    print("api client ready")

    payload_1 = {"userID": 78, "movieID": 903, "rating": 4.0, "genre-Action": 0, "genre-Adventure": 0,
                  "genre-Animation": 0, "genre-Children": 0, "genre-Comedy": 0, "genre-Crime": 0, "genre-Documentary": 0,
                  "genre-Drama": 1, "genre-Fantasy": 0, "genre-Film-Noir": 0, "genre-Horror": 0, "genre-IMAX": 0,
                  "genre-Musical": 0, "genre-Mystery": 1, "genre-Romance": 1, "genre-Sci-Fi": 0, "genre-Short": 0,
                  "genre-Thriller": 1, "genre-War": 0, "genre-Western": 0}

    payload_2 = {"userID": 75, "movieID": 904, "rating": 3.0, "genre-Action": 0, "genre-Adventure": 1,
                  "genre-Animation": 0, "genre-Children": 0, "genre-Comedy": 1, "genre-Crime": 0, "genre-Documentary": 0,
                  "genre-Drama": 1, "genre-Fantasy": 0, "genre-Film-Noir": 0, "genre-Horror": 0, "genre-IMAX": 0,
                  "genre-Musical": 0, "genre-Mystery": 1, "genre-Romance": 0, "genre-Sci-Fi": 0, "genre-Short": 0,
                  "genre-Thriller": 0, "genre-War": 0, "genre-Western": 0}

    # Test order
    # get_ratings > delete > posts > get_ratings > avg_all > avg_user > user_profile
    ac.test_ratings_delete()
    ac.test_ratings_get()
    ac.test_rating_post(payload_1)
    ac.test_avg_all_users()
    ac.test_avg_by_user(78)
    ac.test_user_profile(78)
    ac.test_rating_post(payload_2)
    ac.test_avg_all_users()
    ac.test_avg_by_user(75)
    ac.test_user_profile(78)

    # for i in range(20):
    #     ac.test_rating_post(payload_1)
    #     sleep(0.1)
    #     ac.test_rating_post(payload_2)
    #
    # ac.test_avg_all_users()
    # ac.test_avg_by_user(75)
    # ac.test_user_profile(75)

import random
import redis
import json
from wtiproj03_ETL import dict_list_to_df


class RedisClient:
    def __init__(self, host, port, db):
        self._redis = redis.StrictRedis(host=host, port=port, db=db)

    def get_dummy_randomized_dict(self, okreslony_int=0):
        dummy_randomized_dict = {}
        dummy_randomized_dict["jakis_string"] = "wartosc1"
        dummy_randomized_dict["jakis_float_jako_string"] = str(random.random())
        dummy_randomized_dict["jakis float"] = random.random()
        dummy_randomized_dict["jakis_int"] = random.randint(0, 44)
        dummy_randomized_dict["okreslony_int"] = okreslony_int

        return dummy_randomized_dict

    def clear_whole_db(self):
        self._redis.flushdb()

    def clear_queue(self, queue_name):
        queue_batch = self._redis.lrange(queue_name, 0, -1)
        self._redis.ltrim(queue_name, len(queue_batch), -1)

    def rpush(self, queue_name, _json_dict_list):
        self._redis.rpush(queue_name, json.dumps(_json_dict_list))

    def lrange(self, queue_name, _from, _to):
        return self._redis.lrange(queue_name, _from, _to)

    def ltrim(self, queue_name, _from, _to):
        return self._redis.ltrim(queue_name, _from, _to)

    def set(self, queue_name, payload):
        self._redis.set(queue_name, payload)

    def get(self, queue_name):
        self._redis.get(queue_name)

    def exists(self, queue_name):
        return self._redis.exists(queue_name)

    def pull_queue(self, queue_name):
        queue_as_list = []
        queue_batch = self.lrange(queue_name, 0, -1)
        for dict_value in queue_batch:
            value_read_from_queue_as_dict = json.loads(dict_value.decode())
            queue_as_list.append(value_read_from_queue_as_dict)

        # dict_list_to_df confuses column order, so movieID, rating, userID are the last ones
        genre_names = list(dict_list_to_df(queue_as_list))[:-3]

        return queue_as_list, genre_names

    def printout_queue(self, queue_name):
        queue_batch = self.lrange(queue_name, 0, -1)
        for dict_value in queue_batch:
            value_read_from_queue_as_dict = json.loads(dict_value.decode())
            print(value_read_from_queue_as_dict)


if __name__ == "__main__":
    r = RedisClient('localhost', 6379, 0)
    print("redis client ready")

    for i in range(3):
        dummy_dict = r.get_dummy_randomized_dict()
        r.rpush('dummy_dict', dummy_dict)

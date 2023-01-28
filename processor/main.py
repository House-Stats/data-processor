
import os
import time
from datetime import datetime
from pickle import loads
from typing import Dict

import psycopg2
from aggregations import Aggregator
from confluent_kafka import Consumer
from load_data import Loader
from pymongo import MongoClient


class Processor():
    def __init__(self):
        self._load_env()
        self._sql_conn = psycopg2.connect(f"postgresql://{self._SQL_USERNAME}:{self._SQL_PASSWORD}@{self._SQL_HOST}:5432/house_data")
        self._cur = self._sql_conn.cursor()
        self._mongo_conn = MongoClient(f"mongodb://{self._MONGO_USERNAME}:{self._MONGO_PASSWORD}@{self._MONGO_HOST}:27017/?authSource=house_data")
        self._mongo_db = self._mongo_conn["house_data"]
        self._consumer = Consumer({
                'bootstrap.servers': self._KAFKA,
                'group.id': 'PROCESSOR',
                'auto.offset.reset': 'earliest'
            })

    def _load_env(self):
        # Loads the enviroment variables
        self._DB = os.environ.get("DBNAME", "house_data")
        self._SQL_USERNAME = os.environ.get("POSTGRES_USER")
        self._SQL_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
        self._SQL_HOST = os.environ.get("POSTGRES_HOST")
        self._KAFKA = os.environ.get("KAFKA")
        self._MONGO_HOST = os.environ.get("MONGO_HOST")
        self._MONGO_USERNAME = os.environ.get("MONGO_USERNAME")
        self._MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")

    def main_loop(self) -> None:
        self._consumer.subscribe(["query_queue"])
        print("Waiting for queries")
        while True:
            msg = self._consumer.poll(1.0)  # Fetches the latest message from kafka
            if msg is None:  #Checks the message isnt empty
                continue
            if msg.error():  # Checks there are no errors
                print("Consumer error: {}".format(msg.error()))
                continue
            query: tuple = loads(msg.value()) # (area, area_type)
            query = tuple(map(lambda x: x.upper(), query)) # Makes all items upper case
                
            print(f"{time.time()} - {query[0]}({query[1]})")
            if not self._check_cache(*query):
                print(query, "- Aggregating data")
                self._get_stats(*query)
            else:
                print(query, "- Cache hit")
                continue

    def _check_cache(self, area, area_type) -> bool:
        query_id = self._calc_query_id(area, area_type)
        query = self._mongo_db.cache.find_one({"_id": query_id})
        if query is not None:
            last_updated = self._get_last_updated()
            if query["last_updated"] < last_updated:
                return False
            return True
        else:
            return False

    def _get_last_updated(self):
        self._cur.execute("SELECT * FROM settings WHERE name = 'last_updated'")
        last_updated = self._cur.fetchone()
        if last_updated == None:
            return datetime.fromtimestamp(0)
        else:
            if last_updated[1] is not None:
                return datetime.fromtimestamp(float(last_updated[1]))
            else:
                return datetime.fromtimestamp(0)


    def _get_stats(self, area: str, area_type: str) -> bool:
        load_start = time.time()
        data = self._get_area_data(area, area_type)
        load_time = time.time()-load_start
        if data is not None:
            start = time.time()
            stats = self._get_aggregation(data)
            time_taken = time.time()-start
            query_id = self._calc_query_id(area, area_type)
            self._cache_query(stats, query_id, area, area_type, time_taken, load_time)
            return True
        else:
            return False

    def _cache_query(self, stats: Dict, query_id: str, area: str, area_type: str, exe_time: float, load_time: float):
        query = self._mongo_db.cache.find_one({"_id": query_id})
        if query is not None:
            self._mongo_db.cache.update_one(
                {"_id": query_id}, 
                {"$set": {
                    "data": stats,
                    "last_updated": datetime.now(),
                    "exec_time": exe_time,
                    "load_time": load_time
                    }
                }
                )
        else:
            document = {
                "_id": query_id,
                "area": area,
                "area_type": area_type,
                "data": stats,
                "last_updated": datetime.now(),
                "exec_time": exe_time,
                "load_time": load_time
            }
            self._mongo_db.cache.insert_one(document)


    def _calc_query_id(self, area: str, area_type: str) -> str:
        query_id = (area + area_type).replace(" ", "")
        return query_id


    def _get_area_data(self, area: str, area_type: str):
        try:
            if area == "ALL" and area_type == "COUNTRY":
                lodr = Loader("", "", self._cur)
            else:
                lodr = Loader(area, area_type, self._cur)
            return lodr
        except Exception as e:
            pass # Store error in db with the query data

    def _get_aggregation(self, loader: Loader) -> Dict:
        agg = Aggregator(loader)
        data = agg.get_all_data()
        return data

if __name__ == "__main__":
    processor = Processor()
    processor.main_loop()
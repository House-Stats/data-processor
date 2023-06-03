from datetime import datetime
from typing import Dict, List, Tuple

import psycopg2
from pymongo import MongoClient
from worker.config import Config


class Valuation():
    def __init__(self) -> None:
        config = Config()
        self._sql_db = psycopg2.connect(f"postgresql://{config.SQL_USER}:{config.SQL_PASSWORD}@{config.SQL_HOST}:5432/house_data")
        self._mongo_db = MongoClient(f"mongodb://{config.MONGO_USER}:{config.MONGO_PASSWORD}@{config.MONGO_HOST}:27017/?authSource=house_data")
        self._cur = self._sql_db.cursor()
        self._mongo = self._mongo_db.house_data

    def check_house(self, houseid: str) -> bool:
        self._cur.execute("""SELECT h.houseid, h.paon, h.saon, h.postcode, h.type, p.town, p.district, p.county, p.area, p.outcode, p.sector
                              FROM houses AS h
                              INNER JOIN postcodes AS p ON h.postcode = p.postcode AND h.houseid = %s;""", 
                              (houseid,))
        self._house_info = self._cur.fetchone()
        if self._house_info is not None:
            return True
        else:
            return False

    def get_areas(self) -> List[Tuple[str,str]]:
        area_types = [("town", 5), ("county", 7), ("area", 8), ("outcode", 9), ("sector", 10)]
        areas = []
        for area_type in area_types:
            areas.append(
                (self._house_info[area_type[1]], area_type[0])
            )
        return areas

    def load_aggregations(self, areas: List[Tuple[str,str]]) -> List[Dict]:
        aggregations = []
        areas.append(("ALL", "COUNTRY"))
        for area in areas:
            _id = "".join(area).upper()
            agg = self._mongo.cache.find_one({"_id": _id})
            if agg is not None:
                aggregations.append(agg)
        for idx, agg in enumerate(aggregations):
            temp = {
                "area": agg["area"],
                "area_type": agg["area_type"],
                "monthly_qty": agg["stats"]["monthly_qty"],
                "monthly_perc": agg["stats"]["percentage_change"]
                }
            aggregations[idx] = temp
        return aggregations

    def _calc_biases(self, aggs, time_frame) ->List[Dict[str,float]]:
        monthly_volume = []
        biases: List[Dict[str, float]] = []
        for month in range(time_frame):
            month_biases = {}
            month_qty = 0
            for area in aggs[:-1]:
                idx = area["monthly_qty"]["type"].index(self._house_info[4].upper())
                month_qty += area["monthly_qty"]["qty"][idx][month]

            for area in aggs[:-1]:
                idx = area["monthly_qty"]["type"].index(self._house_info[4].upper())
                area_qty = area["monthly_qty"]["qty"][idx][month]
                month_biases[area["area_type"].upper()] = area_qty/month_qty
            biases.append(month_biases)

        return biases

    def find_monthly_averages(self, aggs) -> List[float]:
        perc_changes = []
        time_frame = len(aggs[-1]["monthly_perc"]["all"]["date"])
        biases = self._calc_biases(aggs, time_frame)
        for month in range(time_frame):
            local_average = 0
            for area in aggs[:-1]:
                local_average += area["monthly_perc"][self._house_info[4].upper()]["perc_change"][month] * biases[month][area["area_type"]]
            national_average = aggs[-1]["monthly_perc"][self._house_info[4].upper()]["perc_change"][month]
            average = (local_average + national_average) / 2
            perc_changes.append(average)

        return perc_changes

    def get_house_sales(self) -> List[Tuple[int, datetime]]:
        query = """SELECT s.price, s.date 
                FROM houses AS h 
                INNER JOIN sales AS s on s.houseid = h.houseid AND h.houseid = %s 
                WHERE s.freehold = true AND s.ppd_cat = 'A';"""
        self._cur.execute(query, (self._house_info[0],))
        sales = self._cur.fetchall()
        return sales

    def calc_latest_price(self, sales: List[Tuple[int, datetime]], percs: List[float]) -> List[List[datetime] | List[List[int]]]:
        sales_valuations = []
        for sale in sales:
            sales_date = sale[1]
            sale_month = (sales_date.year - 1996) * 12 + sales_date.month - 1
            house_value = []
            prev_month = sale[0]
            for month in percs[sale_month:]:
                house_value.append(prev_month)
                prev_month = prev_month * (1 + (month/100))
            padding = [None for i in range(sale_month)]
            house_value = list(map(lambda x: round(x,-2), house_value))
            house_value = padding + house_value
            sales_valuations.append(house_value)
        return [[i[1] for i in sales],sales_valuations]
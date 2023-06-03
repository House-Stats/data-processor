from datetime import datetime, timedelta
from typing import Dict

import polars as pl
import psycopg2
from dateutil.relativedelta import relativedelta
from polars import exceptions as pl_ex
from pymongo import MongoClient
from worker.config import Config
from worker.func_timer import Timer
from worker.loader import Loader


class Analyse():
    def __init__(self):
        config = Config()
        self._sql_uri = f"postgresql://{config.SQL_USER}:{config.SQL_PASSWORD}@{config.SQL_HOST}:5432/house_data"
        self._sql_db = psycopg2.connect(self._sql_uri)
        self._mongo_db = MongoClient(f"mongodb://{config.MONGO_USER}:{config.MONGO_PASSWORD}@{config.MONGO_HOST}:27017/?authSource=house_data")
        self._cur = self._sql_db.cursor()
        self._mongo = self._mongo_db.house_data

    @property
    def cursor(self):
        return self._cur

    def clean_up(self):
        self._sql_db.close()
        self._mongo_db.close()

    def run(self, area: str, area_type: str):
        area = area.upper()
        area_type = area_type.upper()
        self.timer = Timer()

        if (not self._check_cache(area + area_type) 
                and not self.load_data(area, area_type)):
    
            self.aggregate_data()

            timings = self.timer.get_times

            return_data = {
                "_id": area + area_type,
                "area": area,
                "area_type": area_type,
                "last_updated": datetime.now(),
                "timings": timings,
                "stats": self._stats
            }

            self._cache_results(return_data)

    def load_data(self, area, area_type):
        if area == "ALL" and area_type == "COUNTRY":
            area = ""
            area_type = ""

        self.timer.start("loader")

        try:
            self._loader = Loader(area, area_type, self._cur, self._sql_uri)
        except ValueError as e:
            print(e)
            return e
        self._data = self._loader.get_data()
        del self._loader

        self.timer.end("loader")


    def aggregate_data(self):
        self.timer.start("aggregate")
        self._stats = self.get_all_data()
        self.timer.end("aggregate")

    @property
    def stats(self):
        return self._stats

    def _cache_results(self, return_data: Dict) -> None:
        area_record = self._mongo.cache.find_one({
            "_id": return_data["_id"]
        })
        if area_record is not None:
            self._mongo.cache.update_one(
                {"_id": return_data["_id"]},
                {"$set":{
                        "last_updated": return_data["last_updated"],
                        "timings": return_data["timings"],
                        "stats": return_data["stats"]
                }})
        else:
            self._mongo.cache.insert_one(return_data)

    def _check_cache(self, area_id: str) -> bool:
        data = self._mongo.cache.find_one({"_id": area_id})
        if data is not None:
            last_updated = self.last_updated()
            if data["last_updated"] < last_updated:
                return False
            else:
                return True
        else:
            return False

    def last_updated(self):
        self._cur.execute("SELECT * FROM settings WHERE name = 'last_updated';")
        data = self._cur.fetchone()
        if data is not None:
            date = datetime.fromtimestamp(float(data[1]))
            return date
        else:
            return datetime.fromtimestamp(0)

    def _get_average_prices(self, period: str="1mo") -> Dict:
        self.timer.start("aggregate_average")
        df = self._data.partition_by("type", as_dict=True)
        house_types_means = {}
        for house_type in df:
            average_prices = self._calc_average_price(df[house_type],period)
            house_types_means[house_type] = self.pad_df(average_prices, period).to_dict(as_series=False)

        house_types_means["all"] = self.pad_df(self._calc_average_price(self._data, period), period).to_dict(as_series=False)
        data = {
            "type": [key for key in sorted(house_types_means)],
            "prices": [house_types_means[key]["price"] for key in sorted(house_types_means)],
            "dates": house_types_means["all"]["date"]
        }
        self.timer.end("aggregate_average")
        del df
        return data

    def _calc_average_price(self, df: pl.DataFrame, period: str) -> pl.DataFrame:
        df = df \
            .sort("date") \
            .groupby_dynamic("date", every=period) \
            .agg(pl.col("price").log().mean().exp())
        return df

    def _get_type_proportions(self) -> Dict:
        self.timer.start("aggregate_proportions")
        df = self._data \
            .unique(subset=["houseid"]) \
            .groupby("type") \
            .count() \
            .sort("type")
        data = df.to_dict(as_series=False)
        self.timer.end("aggregate_proportions")
        del df
        return data

    def _get_monthly_qtys(self, period: str="1mo") -> Dict:
        self.timer.start("aggregate_qty")
        df = self._data.partition_by("type", as_dict=True)
        monthly_quantity = {}
        for house_type in df:
            volume = self._calc_monthly_qty(df[house_type],period)
            monthly_quantity[house_type] = self.pad_df(volume, period).to_dict(as_series=False)

        monthly_quantity["all"] = self.pad_df(self._calc_monthly_qty(self._data, period), period).to_dict(as_series=False)

        data = {
            "type": [key for key in sorted(monthly_quantity)],
            "qty": [monthly_quantity[key]["qty"] for key in sorted(monthly_quantity)],
            "dates": monthly_quantity["all"]["date"]
        }
        del df
        self.timer.end("aggregate_qty")
        return data

    def _calc_monthly_qty(self, df: pl.DataFrame, period: str) -> pl.DataFrame:
        volume = df \
            .sort("date") \
            .groupby_dynamic("date", every=period) \
            .agg(pl.col("price").count().alias("qty"))
        return volume

    def _get_monthly_volumes(self, period: str = "1mo") -> Dict:
        self.timer.start("aggregate_vol")
        df = self._data.partition_by("type", as_dict=True)
        monthly_volume = {}
        for house_type in df:
            volume = self._calc_monthly_vol(df[house_type],period)
            monthly_volume[house_type] = self.pad_df(volume, period).to_dict(as_series=False)

        monthly_volume["all"] = self.pad_df(self._calc_monthly_vol(self._data,period), period).to_dict(as_series=False)

        data = {
            "type": [key for key in sorted(monthly_volume)],
            "volume": [monthly_volume[key]["volume"] for key in sorted(monthly_volume)],
            "dates": monthly_volume["all"]["date"]
        }
        del df
        self.timer.end("aggregate_vol")
        return data

    def _calc_monthly_vol(self, df: pl.DataFrame, period: str) -> pl.DataFrame:
        volume = df \
            .sort("date") \
            .groupby_dynamic("date", every=period) \
            .agg(pl.col("price").sum().alias("volume"))
        return volume

    def _get_percs(self, period: str = "1mo") -> Dict:
        self.timer.start("aggregate_perc")
        data = self._data.partition_by("type", as_dict=True)
        monthly_perc = {}
        for house_type in data:
            monthly_perc[house_type] = self.pad_df(self._calc_ind_perc(data[house_type], period), period).to_dict(as_series=False)

        monthly_perc["all"] = self.pad_df(self._calc_ind_perc(self._data, period), period).to_dict(as_series=False)

        del data
        self.timer.end("aggregate_perc")
        return monthly_perc

    def _calc_ind_perc(self, df: pl.DataFrame, period: str) -> pl.DataFrame:
        df = df.sort("date") \
            .groupby_dynamic("date", every=period) \
            .agg(pl.col("price").log().mean().exp().alias("avg_price"))

        df = df.with_columns([
            pl.col("date").dt.round(period).alias("period"),
        ])

        df = self._calc_period_perc(df, period)
        df = df \
            .drop(["prev_period", "avg_price","period"]) \
            .sort("date")
        return df

    def _calc_period_perc(self, df: pl.DataFrame, period) -> pl.DataFrame:
        df = df \
            .sort("date") \
            .with_columns(
                pl.col("avg_price").shift().alias("prev_period")
            ) \
            .filter(pl.col("prev_period").is_not_null())
        
        magic_numbers = {
            "1mo": 12,
            "3mo": 4,
            "6mo": 2,
            "12mo": 1
        } # It works and I dont know why

        df = df.with_columns(
            (((pl.col("avg_price")-pl.col("prev_period"))
                /pl.col("avg_price")
                    *100)
                        /magic_numbers[period]).alias("perc_change")
        )
        return df

    def average_tenancy(self):
        self.timer.start("aggregate_tenancy")
        data = self._data.partition_by("type", as_dict=True)
        tenancies = {}
        for house_type in data:
            df = self._get_ind_tenancy(data[house_type]) \
                .to_dict(as_series=False)["date"][0]
            if df is not None:
                try:
                    tenancies[house_type] = df.total_seconds()
                except AttributeError:
                    tenancies[house_type] = 0
            else:
                tenancies[house_type] = 0
        try:
            tenancies["all"] = self._get_ind_tenancy(self._data ) \
                    .to_dict(as_series=False)["date"][0].total_seconds()
        except AttributeError:
            tenancies["all"] = 0
        self.timer.end("aggregate_tenancy")
        return tenancies

    def _get_ind_tenancy(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df\
            .sort("date") \
            .groupby("houseid", maintain_order=True) \
            .agg([
                (pl.col("date") - pl.col("date").shift()),
            ]) \
            .explode("date")

        df = df \
            .filter(pl.col("date").is_not_null()) \
            .select(pl.col("date").mean())
        return df

    def _quick_stats(self, data) -> Dict[str, float]:

        current_month = data["average_price"]["dates"][-1]
        print(current_month)
        current_average = data["average_price"]["prices"][-1][-1]
        prev_average = data["average_price"]["prices"][-1][-2]
        try:
            average_change = round(100*(current_average-prev_average)/prev_average, 2)
        except ZeroDivisionError:
            average_change = None

        current_qty = data["monthly_qty"]["qty"][-1][-1]
        prev_qty = data["monthly_qty"]["qty"][-1][-2]
        try:
            qty_change = round(100*(current_qty-prev_qty)/prev_qty,2)
        except ZeroDivisionError:
            qty_change = None

        current_vol =  data["monthly_volume"]["volume"][-1][-1]
        prev_vol = data["monthly_volume"]["volume"][-1][-2]
        try:
            vol_change = round(100*(current_vol-prev_vol)/prev_vol,2)
        except ZeroDivisionError:
            vol_change = None

        try:
            expensive_sale = (self._data
                .filter(pl.col("date").is_between(current_month, current_month + timedelta(days=31)))
                .filter(pl.col("price") == pl.col("price").max())
                )[0,0]
        except:
            expensive_sale = 0

        quick_stats = {
            "current_time": current_month,
            "average_price": current_average,
            "average_change": average_change,
            "sales_qty": current_qty,
            "sales_qty_change": qty_change,
            "sales_volume": current_vol,
            "sales_volume_change": vol_change,
            "expensive_sale": expensive_sale
        }
        return quick_stats

    def get_all_data(self) -> Dict:
        data_period = {}            
        tenancy = self.average_tenancy()
        proportions = self._get_type_proportions()
        for i in ["1mo","3mo","6mo","12mo"]:
            average_prices = self._get_average_prices(period=i)
            quantities =  self._get_monthly_qtys(period=i)
            volume = self._get_monthly_volumes(period=i)
            perc = self._get_percs(period=i)
            data = {
                "average_price": average_prices,
                "type_proportions": proportions,
                "monthly_qty": quantities,
                "monthly_volume": volume,
                "percentage_change": perc,
                "average_tenancy": tenancy
            }

            data["quick_stats"] = self._quick_stats(data)
            data_period[i] = data

        return data_period

    def pad_df(self, df: pl.DataFrame, period: str) -> pl.DataFrame | None:
        latest_date = self.latest_date
        if latest_date is not None:
            dates = pl.date_range(datetime(1995,1,1), latest_date, period, eager=True)
            dates_df = pl.DataFrame(dates, schema=["date"])
            try:
                df = df.join(dates_df, on="date", how="outer")
                df = df.fill_null(0)
                df = df.filter(pl.col("date") < latest_date)
                return df
            except pl_ex.ComputeError:
                return df

    @property
    def latest_date(self) -> datetime | None:
        self._cur.execute("SELECT data FROM settings WHERE name = 'last_updated';")
        latest_date = self._cur.fetchone()
        if latest_date is not None:
            latest_date = datetime.fromtimestamp(float(latest_date[0]))
            if latest_date > (datetime.now() - timedelta(days=60)):
                start = datetime.now().replace(day=1).replace(hour=0,minute=0,second=0, microsecond=0)
                return start - relativedelta(months=2)
            else:
                return latest_date


if __name__ == "__main__":
    task = Analyse()
    print(task.latest_date)
    task.run("CH47","OUTCODE")

from typing import Dict

import polars as pl
from load_data import Loader
from datetime import timedelta

class Aggregator():
    def __init__(self, data: Loader) -> None:
        self._data = data.data
        self._latest_date = data.latest_date

    def _calc_average_price(self) -> Dict:
        df = self._data.partition_by("type", as_dict=True)
        house_types_means = {}
        for house_type in df:
            temp_df = df[house_type]
            house_types_means[house_type] = temp_df \
                                                .sort("date") \
                                                .groupby_dynamic("date", every="1mo") \
                                                .agg(pl.col("price").log().mean().exp()) \
                                                .to_dict(as_series=False)
        all_sales = self._data.lazy()
        std = all_sales.select(pl.col("price")).std().collect()[0, 0]
        mean = all_sales.select(pl.col("price")).mean().collect()[0, 0]
        temp_df = all_sales.filter((pl.col("price") < mean+(2*std)))
        house_types_means["all"] = all_sales \
                                    .sort("date") \
                                    .groupby_dynamic("date", every="1mo") \
                                    .agg(pl.col("price").log().mean().exp()) \
                                    .collect() \
                                    .to_dict(as_series=False)
        data = {
            "type": [key for key in sorted(house_types_means)],
            "prices": [house_types_means[key]["price"] for key in sorted(house_types_means)],
            "dates": house_types_means["all"]["date"]
        }
        return data

    def _remove_outliers(self, df: pl.DataFrame):
        std = df.select(pl.col("price")).std()[0, 0]
        mean = df.select(pl.col("price")).mean()[0, 0]
        df = df.filter(pl.col("price") < mean + (3*std))
        return df


    def _calc_type_proportions(self) -> Dict:
        df = self._data
        df = df.unique(subset=["houseid"])
        df = df.groupby("type").count()
        data = df.to_dict(as_series=False)
        return data

    def _calc_monthly_volume(self) -> Dict:
        df = self._data.partition_by("type", as_dict=True)
        monthly_volumes = {}
        for house_type in df:
            temp_df = df[house_type].lazy()
            volume = temp_df \
                .sort("date") \
                .groupby_dynamic("date", every="1mo") \
                .agg(pl.col("price").count().alias("volume")) \
                .collect() \
                .to_dict(as_series=False)
            monthly_volumes[house_type] = volume

        monthly_volumes["all"] = self._data.sort("date") \
                .groupby_dynamic("date", every="1mo") \
                .agg(pl.col("price").count().alias("volume")) \
                .to_dict(as_series=False)

        data = {
            "type": [key for key in sorted(monthly_volumes)],
            "volume": [monthly_volumes[key]["volume"] for key in sorted(monthly_volumes)],
            "dates": monthly_volumes["all"]["date"]
        }
        return data

    def _calc_monthly_price_volume(self) -> Dict:
        df = self._data.partition_by("type", as_dict=True)
        monthly_price_volume = {}
        for house_type in df:
            temp_df = df[house_type].lazy()
            volume = temp_df \
                .sort("date") \
                .groupby_dynamic("date", every="1mo") \
                .agg(pl.col("price").sum().alias("volume")) \
                .collect() \
                .to_dict(as_series=False)
            monthly_price_volume[house_type] = volume

        monthly_price_volume["all"] = self._data.sort("date") \
                .groupby_dynamic("date", every="1mo") \
                .agg(pl.col("price").sum().alias("volume")) \
                .to_dict(as_series=False)
        data = {
            "type": [key for key in sorted(monthly_price_volume)],
            "volume": [monthly_price_volume[key]["volume"] for key in sorted(monthly_price_volume)],
            "dates": monthly_price_volume["all"]["date"]
        }
        return data

    def _calc_all_perc(self) -> Dict:
        data = self._data.partition_by("type", as_dict=True)
        monthly_perc = {}
        for house_type in data:
            monthly_perc[house_type] = self._calc_ind_percentage(data[house_type]).to_dict(as_series=False)
        monthly_perc["all"] = self._calc_ind_percentage(self._data).to_dict(as_series=False)
        return monthly_perc


    def _calc_ind_percentage(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.sort("date") \
                .groupby_dynamic("date", every="1mo") \
                .agg(pl.col("price").log().mean().exp().alias("avg_price"))
        df = df.with_columns([
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.year().alias("year")
        ])
        df = df.groupby("month").apply(self._calc_percentages_months)
        df = df.drop(["year", "month", "prev_year", "avg_price"])
        df = df.sort("date")
        return df

    def _calc_percentages_months(self, data: pl.DataFrame):
        df = data.sort("date").with_columns(
            pl.col("avg_price").shift().alias("prev_year")
        )
        df = df.filter(pl.col("prev_year").is_not_null())
        df = df.with_columns(
            (((pl.col("avg_price")-pl.col("prev_year"))/pl.col("avg_price")*100)/12).alias("perc_change")
        )
        return df

    def _quick_stats(self, data) -> Dict[str, float]:
        try:
            current_month = data["average_price"]["dates"][-2]
            current_average = data["average_price"]["prices"][4][-2]
            prev_average = data["average_price"]["prices"][4][-3]
            current_average_change = round(100*(current_average-prev_average)/prev_average,2)
        except Exception:
            return {
                "current_month": 0,
                "average_price": 0,
                "average_change": 0,
                "current_sales_volume": 0,
                "sales_volume_change": 0,
                "current_price_volume": 0,
                "price_volume_change": 0,
                "expensive_sale": 0
            }

        try:
            current_sales_vol = data["monthly_sales_volume"]["volume"][4][-2]
            prev_sales_vol = data["monthly_sales_volume"]["volume"][4][-3]
            current_sales_vol_change = round(100*(current_sales_vol-prev_sales_vol)/prev_sales_vol,2)
        except IndexError:
            current_sales_vol = 0
            current_sales_vol_change = 0

        try:
            current_price_vol =  data["monthly_price_volume"]["volume"][4][-2]
            prev_price_vol = data["monthly_price_volume"]["volume"][4][-3]
            current_price_vol_change = round(100*(current_price_vol-prev_price_vol)/prev_price_vol,2)
        except IndexError:
            current_price_vol = 0
            current_price_vol_change = 0

        expensive_sale = (self._data
            .filter(pl.col("date").is_between(current_month, current_month + timedelta(days=31)))
            .filter(pl.col("price") == pl.col("price").max())
            )[0,0]

        quick_stats = {
            "current_month": current_month,
            "average_price": current_average,
            "average_change": current_average_change,
            "current_sales_volume": current_sales_vol,
            "sales_volume_change": current_sales_vol_change,
            "current_price_volume": current_price_vol,
            "price_volume_change": current_price_vol_change,
            "expensive_sale": expensive_sale
        }
        return quick_stats

    def get_all_data(self) -> Dict:
        data = {
            "average_price": self._calc_average_price(),
            "type_proportions": self._calc_type_proportions(),
            "monthly_sales_volume": self._calc_monthly_volume(),
            "monthly_price_volume": self._calc_monthly_price_volume(),
            "percentage_change": self._calc_all_perc()
        }
        data["quick_stats"] = self._quick_stats(data)
        return data

        

if __name__ == "__main__":
    import time
    import psycopg2

    start = time.time()
    conn = psycopg2.connect("postgresql://house_data:lriFahwbJwfv2388neiluOMI@192.168.4.30:5432/house_data")
    data_loader = Loader("CH", "area", conn.cursor())
    print(f"loaded_data - {time.time() - start}")
    agg = Aggregator(data_loader)
    data = agg.get_all_data()
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(data["percentage_change"])
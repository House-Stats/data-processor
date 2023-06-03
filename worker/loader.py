from datetime import datetime, timedelta
from typing import List, Tuple

import polars as pl
from dateutil.relativedelta import relativedelta


class Loader():
    def __init__(self, area: str, area_type: str, db_cur, sql_uri: str) -> None:
        self._sql_uri = sql_uri
        self._cur = db_cur
        self.area_type = area_type.lower()
        self.area = area.upper()
        if self.validate_areas():
            self.fetch_area_sales()
            self.format_df()

    def validate_areas(self) -> bool | None:
        self._areas = ["postcode", "street", "town", "district", "county", "outcode", "area", "sector", ""]
        if self.area_type not in self._areas:
            raise ValueError("Invalid area type")
        else:
            if self.area == "":
                return True
            elif self.verify_area():
                return True

    def verify_area(self):
        self._cur.execute(f"SELECT postcode FROM postcodes WHERE {self.area_type} = %s LIMIT 1;", (self.area,))
        if self._cur.fetchall() is not []:
            return True
        else:
            raise ValueError(f"Invalid {self.area_type} entered")

    def fetch_area_sales(self):
        query = f"""SELECT s.price, s.date, h.type, h.paon, h.saon, h.postcode, p.street, p.town, h.houseid
                FROM postcodes AS p
                INNER JOIN houses AS h ON p.postcode = h.postcode AND p.{self.area_type} = '{self.area}'
                INNER JOIN sales AS s ON h.houseid = s.houseid AND h.type != 'O'
                WHERE s.ppd_cat = 'A' AND s.date < '{self.latest_date}'
                """
        if self.area == "" and self.area_type == "":
            query = query.replace("AND p. = ''", "")
        self._data = pl.read_sql(query, self._sql_uri)


    def format_df(self):
        self._data = self._data.with_columns([
            pl.col('date').apply(lambda x: datetime(*x.timetuple()[:-4])).alias("dt") # type: ignore
        ])
        self._data = self._data \
            .drop("date") \
            .rename({"dt":"date"})
        self._data = self._data.with_columns([
            pl.col("price").cast(pl.Int64).alias("price")
        ])

    @property
    def latest_date(self) -> datetime | None:
        self._cur.execute("SELECT data FROM settings WHERE name = 'last_updated';")
        latest_date = self._cur.fetchone()[0]
        if latest_date is not None:
            latest_date = datetime.fromtimestamp(float(latest_date))
            if latest_date > (datetime.now() - timedelta(days=60)):
                start = datetime.now().replace(day=1).replace(hour=0,minute=0,second=0, microsecond=0)
                return start - relativedelta(months=2)
            else:
                return latest_date


    def get_data(self) -> pl.DataFrame:
        return self._data

if __name__ == "__main__":
    import psycopg2
    from config import Config

    config = Config()
    uri = f"postgresql://{config.SQL_USER}:{config.SQL_PASSWORD}@{config.SQL_HOST}:5432/house_data"
    conn = psycopg2.connect(uri)
    lodr = Loader("", "", conn.cursor(), uri)

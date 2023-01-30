from datetime import datetime, timedelta

import polars as pl
from typing import List

class Loader():
    def __init__(self, area: str, area_type: str, db_cur) -> None:
        self._cur = db_cur
        self.area_type = area_type.lower()
        self.area = area.upper()
        self._areas = ["postcode", "street", "town", "district", "county", "outcode", "area", "sector"]
        if self.area == "" and self.area_type == "":
            self._cur.execute("""SELECT s.price, s.date, h.type, h.paon, h.saon, h.postcode, p.street, p.town, h.houseid
                    FROM postcodes AS p
                    INNER JOIN houses AS h ON p.postcode = h.postcode
                    INNER JOIN sales AS s ON h.houseid = s.houseid AND h.type != 'O'
                    WHERE s.ppd_cat = 'A';""")
            data = self._cur.fetchall()
            self._format_df(data)
        else:
            if self.area_type not in self._areas:
                raise ValueError("Invalid area type")
            else:
                if self.verify_area():
                    data = self.fetch_area_sales()
                    self._format_df(data)

    def verify_area(self):
        self._cur.execute(f"SELECT postcode FROM postcodes WHERE {self.area_type} = %s LIMIT 1;", (self.area,))
        if self._cur.fetchall() is not []:
            return True
        else:
            raise ValueError(f"Invalid {self.area_type} entered")

    def fetch_area_sales(self) -> List:
        query = f"""SELECT s.price, s.date, h.type, h.paon, h.saon, h.postcode, p.street, p.town, h.houseid
                FROM postcodes AS p
                INNER JOIN houses AS h ON p.postcode = h.postcode AND p.{self.area_type} = %s
                INNER JOIN sales AS s ON h.houseid = s.houseid AND h.type != 'O'
                WHERE s.ppd_cat = 'A' AND s.date < %s;
                """
        self._cur.execute(query, (self.area, self.latest_date))
        data = self._cur.fetchall()
        if data == []:
            raise ValueError(f"No sales for area {self.area}")
        else:
            return data

    def _format_df(self, data):
        self._data = pl.DataFrame(data,
                                    columns=["price","date","type","paon","saon",
                                             "postcode","street","town","houseid"],
                                    orient="row")
        self._data = self._data.with_column(
            pl.col('date').apply(lambda x: datetime(*x.timetuple()[:-4])).alias("dt")
        )
        self._data = self._data.drop("date")
        self._data = self._data.with_column(
            pl.col("dt").alias("date")
        )
        self._data = self._data.drop("dt")

    @property
    def data(self) -> pl.DataFrame:
        return self._data

    @property
    def latest_date(self):
        self._cur.execute("SELECT date FROM sales ORDER BY date DESC LIMIT 1;")
        latest_date = self._cur.fetchone()
        if latest_date is not None:
            latest_date = datetime.combine(latest_date[0], datetime.min.time())
            if latest_date > (datetime.now() - timedelta(days=60)):
                return datetime.now() - timedelta(days=60)
            else:
                return latest_date[0]

if __name__ == "__main__":
    import psycopg2
    conn = psycopg2.connect("postgresql://house_data:lriFahwbJwfv2388neiluOMI@192.168.4.30:5432/house_data")
    loader = Loader("CH", "area", conn.cursor())
    print(loader.data.head())

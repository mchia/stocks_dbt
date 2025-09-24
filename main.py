import os
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
import snowflake.connector as sfc
from snowflake.connector.pandas_tools import write_pandas

from typing import (
    Any,
    Optional
)

load_dotenv()

# Use tuple to guarantee unique tickers and a cuase more memory efficient.
stock_tickers: tuple[str] = ('AMZN')

class pipeline:
    def __init__(self, tickers: tuple[str]) -> None:
        self.tickers: tuple[str] = tickers
        self.conn: Optional[sfc.SnowflakeConnection] = sfc.connect(
            user=os.getenv(key='user'),
            password=os.getenv(key='password'),
            account=os.getenv(key='account'),
            warehouse=os.getenv(key='warehouse'),
            database=os.getenv(key='database'),
            schema=os.getenv(key='schema'),
            role=os.getenv(key='role')
        )
        self.cursor: Optional[sfc.SnowflakeCursor] = self.conn.cursor()

    def create_table(self) -> None:
        # NUMBER(18, 4) -> Improve accuracy of float values (18 = Numbers before decimal, 4 = Numbers after decimal)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_data (
                date DATE,
                ticker STRING,
                open NUMBER(18,4),
                high NUMBER(18,4),
                low NUMBER(18,4),
                close NUMBER(18,4),
                adj_close NUMBER(18,4),
                volume BIGINT
            )
        """)

    def date_checker(self) -> pd.DataFrame:
        tickers_list: str = ', '.join([f"'{t}'" for t in self.tickers])
        query: str = f"""
            SELECT
                ticker,
                MAX(date) AS max_date
            FROM
                stock_data.historical_data.price_data
            WHERE
                ticker IN ({tickers_list})
            GROUP BY
                ticker
        """

        filter_df: pd.DataFrame = pd.read_sql(sql=query, con=self.conn)
        filter_df['MAX_DATE'] = pd.to_datetime(filter_df['MAX_DATE']).dt.date

        return filter_df

    def extract(self) -> pd.DataFrame:
        """
        Method to source all historical data available for tickers in self.tickers
        This method defaults to using the 1d interval.
        If other intervals are required, can use:
            yf.download and pass the desired interval as a parameter.
            Monkey path the history method to override 1d.
        """
        data: Optional[pd.DataFrame] = yf.download(
            tickers=self.tickers,
            period='max',
            group_by='ticker',
            auto_adjust=False
        )

        data: pd.DataFrame = data.stack(level=0).reset_index()
        data.columns = (
            data.columns
            .str.strip()
            .str.replace(r"^['\"]|['\"]$", "", regex=True)
            .str.upper()
            .str.replace(' ', '_')
        )
        data['DATE'] = pd.to_datetime(data['DATE']).dt.date

        return data

    def load(self) -> None:
        df: pd.DataFrame = self.extract()
        filter_df: pd.DataFrame = self.date_checker()

        merged: pd.DataFrame = df.merge(
            right=filter_df,
            on='TICKER',
            how='left'
        )

        merged: pd.DataFrame = merged[(merged['DATE'] > merged['MAX_DATE']) | (merged['MAX_DATE'].isna())]
        merged.drop(columns='MAX_DATE', inplace=True)

        if len(merged) > 0:
            write_pandas(
                conn=self.conn,
                df=df,
                table_name="PRICE_DATA",
                auto_create_table=True
            )
        else:
            print('No new rows to insert')

    def run(self) -> None:
        self.create_table()
        self.load()


d = pipeline(tickers=stock_tickers).load()

print(len(d))
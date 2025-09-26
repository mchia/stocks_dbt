import os
import yaml
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from typing import Any, Optional
import snowflake.connector as sfc
from snowflake.connector.pandas_tools import write_pandas
from concurrent.futures import ThreadPoolExecutor, as_completed


# Use tuple to guarantee unique tickers and cause more memory efficient.
stock_tickers: list[str] = ['NAB.AX', 'WBC.AX', 'ANZ.AX', 'BEN.AX', 'BOQ.AX', 'BFL.AX', 'JDO.AX', 'HGH.AX', 'MYS.AX', 'KSL.AX', 'BBC.AX']

class yf_data:
    def __init__(self, tickers: tuple[str]) -> None:
        self.tickers: tuple[str] = tickers

    def price_data(self) -> pd.DataFrame:
        """
        Method to source all historical data available for tickers in self.tickers
        This method defaults to using the 1d interval.
        Concurrency not required as yf.download already supports it.

        Returns
            data : pd.DataFrame
                A dataframe containing the cleaned data.
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

    @staticmethod
    def profile_data(ticker: str) -> dict[str, str]:
        """
        Retrieves profile data on tickers.
        Uses ticker not self.tickers (Tuple) as this method does not support concurrency.
        No loops/list comprehension as this method will be passed into ThreadingPool.

        Parameters:
            ticker : str -> Name of ticker to fetch data for

        Returns
            Dictionary containing (Ticker, Company Name, Industry, Sector, Country, QuoteType)
        """
        info: dict = yf.Ticker(ticker).info

        return {
        "TICKER": info.get("symbol"),
        "COMPANY_NAME": info.get("longName"),
        "INDUSTRY": info.get("industry", "Unavailable"),
        "SECTOR": info.get("sector", "Unavailable"),
        "COUNTRY":  info.get("country", "Unavailable"),
        "CLASS": info.get("quoteType", "Unavailable"),
        "MARKET_CAP": info.get("marketCap", None)
    }
    
    def threaded_profiling(self) -> pd.DataFrame:
        """
        Method that calls ThreadedPoolExecutor to process profiles concurrently instead of sequentially.
        Adjusts concurrent workers based on min of cpu_count() or number of tickers to evaluate.
        
        Returns
            pd.DataFrame
        """
        results: list = []
        with ThreadPoolExecutor(max_workers=min(os.cpu_count(), len(self.tickers))) as thread:
            future_ticker = {thread.submit(self.profile_data, t): t for t in self.tickers}
            for future in as_completed(future_ticker):
                results.append(future.result())
        return pd.DataFrame(data=results)

class pipeline:
    def __init__(self, tickers: tuple[str], db_schema: str) -> None:
        load_dotenv()
        self.tickers: list[tuple] = tickers
        self.db_schema: str = db_schema
        self.yf_data: yf_data = yf_data(tickers=self.tickers)
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

    def table_creator(self) -> None:
        """
        Method to read yaml file containing all required db table schemas and create them if they dont exist.
        YAML as easier to maintain and update manually.
        Datasets and columns unlikely to change, ALTER TABLE has been exlcuded in this pipeline.
        """
        with open(self.db_schema, "r") as f:
            schema: Any = yaml.safe_load(f)

        for table, columns in schema.items():
            cols: list[str] = [f"{col} {dtype}" for col, dtype in columns.items()]
            sql: str = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(cols)});"
            self.cursor.execute(sql)

    def date_checker(self) -> pd.DataFrame:
        """
        Method to query for all tickers currently in the price_data table to get their most recent dates.
        Might be easier to just make this a fact table in Snowflake.

        Returns
            filter_df : pd.DataFrame
                A dataframe containing each currently available ticker and their latest date values.
        """
        tickers_list: str = ', '.join([f"'{t}'" for t in self.tickers])
        query: str = f"""
            SELECT
                ticker,
                MAX(date) AS max_date
            FROM
                stock_data.historical_data.ticker_data
            WHERE
                ticker IN ({tickers_list})
            GROUP BY
                ticker
        """

        filter_df: pd.DataFrame = pd.read_sql_query(sql=query, con=self.conn)
        filter_df['MAX_DATE'] = pd.to_datetime(filter_df['MAX_DATE']).dt.date

        return filter_df

    def load_price_data(self) -> Optional[pd.Series]:
        """
        Method to compare sourced price data against what currently exists in Snowflake db.
        To avoid ingesting duplicate data, filter to only keep rows where:
            a) Ticker exists and rows are greater than the max date of that ticker in the database.
            b) Ticker does not currently exist in db.

        Returns
            pd.Series -> Count of rows per ticker.
        """
        df: pd.DataFrame = self.yf_data.price_data()
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
                df=merged,
                table_name="TICKER_DATA",
                auto_create_table=False
            )

            ticker_counts: pd.Series = merged.groupby('TICKER').size()

            return ticker_counts
        
        else:
            print("No valid rows to ingest")

    def load_ticker_profiles(self) -> int:
        """
        Method to compare tickers in the DB against newly sourced ones, and load only ones that don't already exist.
        SET has been applied to tickers in python before they are ingested hence:
            SELECT ticker FROM ticker_profiles does not require DISTINCT which would slow the query down.

        Returns
            int -> Count of new tickers added.
        """
        df: pd.DataFrame = self.yf_data.threaded_profiling()
        existing: pd.DataFrame = pd.read_sql_query(sql="""SELECT ticker FROM stock_profiles""", con=self.conn)
        existing_tickers: set[str] = set(existing["TICKER"].str.upper())
        df: pd.DataFrame = df[~df["TICKER"].str.upper().isin(existing_tickers)]

        if len(df) > 0:
            write_pandas(
                conn=self.conn,
                df=df,
                table_name="TICKER_PROFILES",
                auto_create_table=False
            )

            return len(df)
        
        else:
            return 0

    def run(self) -> None:
        self.table_creator()
        price: Optional[pd.Series] = self.load_price_data()
        profiles: int = self.load_ticker_profiles()

        print(price)
        print(f'{profiles} new profiles ingested')


pipeline(tickers=stock_tickers, db_schema='snowflake_schemas.yaml').run()
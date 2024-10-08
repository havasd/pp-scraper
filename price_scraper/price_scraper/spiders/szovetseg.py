"""
Scraper for Szovetseg PPF
"""

from io import BytesIO
from typing import Any
from pandas import ExcelFile
import scrapy
from scrapy.http import Response

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class SzovetsegPPFSpider(scrapy.Spider):
    """
    NOTE: The PPF is merged with Horizont therefore there will be no new data heres.
    I will implement this to be able to create historical prices for onces.

    Scrapes Szövetség PPF prices.

    URLs: https://szovetsegnyp.hu/arfolyam.xlsx

    It returns an excel file.
    """

    name = "szovetseg_nyugdij"
    start_urls = ["https://szovetsegnyp.hu/arfolyam.xlsx"]

    def parse(self, response: Response, **kwargs: Any):
        """
        Parses portfolios from the response which is a complete page.
        """

        excel_data = ExcelFile(BytesIO(response.body))
        dataframe = excel_data.parse(excel_data.sheet_names[0])
        portfolios = ["Klasszikus", "Kiegyensúlyozott", "Növekedési"]
        for row in dataframe.itertuples():
            # pylint: disable=protected-access
            date = row._1
            # invalid date
            if date == '0000-00-00':
                continue
            for idx, price in enumerate(row[2:5]):
                portfolio = portfolios[idx]
                yield PortfolioPerformanceHistoricalPrice(
                        file_name=portfolio.split(' ')[0],
                        date=date,
                        price=price,
                        security_name=f"Szövetség Nyugdíjpénztár {portfolio} portfólió",
                        currency="HUF",
                        ticker_symbol=f"SZÖMNYP_{portfolio[:5].upper()}"
                    )

"""
Scraper for Aranykor VPF
"""
import datetime
from typing import Any
import scrapy
from scrapy.http import Response

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class AranykorSpider(scrapy.Spider):
    """
    Scrapes Aranykor VPF portfolio prices

    Base URL: https://www.aranykornyp.hu/public/arfolyamok/archivum
    """

    name = "aranykor"

    def start_requests(self):
        today = datetime.date.today()
        req_year = 2014
        req_month = 7

        while datetime.date(req_year, req_month, 1) <= today:
            url = f"https://www.aranykornyp.hu/public/arfolyamok/archivum/{req_year}-{req_month}"
            yield scrapy.Request(url=url, callback=self.parse)
            req_month += 1
            if req_month > 12:
                req_month = 1
                req_year += 1

    def parse(self, response: Response, **kwargs: Any):
        header = response.css("tr")[0].css("th::text").getall()
        for line in response.css("tr")[1:]:
            prices = line.css("td::text").getall()
            date = prices[1]
            for idx, portfolio in enumerate(header, 2):
                yield PortfolioPerformanceHistoricalPrice(
                    file_name=portfolio,
                    date=date.replace('.', '-'),
                    price=float(prices[idx]),
                    security_name=f"Aranykor {portfolio}",
                    currency="HUF",
                    ticker_symbol=f"ARANY_{portfolio[:5].upper()}"
                )

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

    name = "aranykorv1"

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

class AranykorSpiderv2(scrapy.Spider):
    """
    Scrapes Aranykor VPF portfolio prices

    Base URL: https://www.aranykornyp.hu/public/arfolyamok/archivum
    """

    name = "aranykor"

    def start_requests(self):
        today = datetime.date.today()

        url = f"https://op-api.aranykornyp.hu/stock-rate/graph/2014-07-01/{today.strftime('%Y-%m-%d')}"
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response: Response, **kwargs: Any):
        data = response.json()
        for  day in data:
            date = datetime.datetime.fromisoformat(day["erteknap"]).date().strftime('%Y-%m-%d')
            # remove this element as we want to iterate over the portfolios
            del day["erteknap"]

            for key, price in day.items():
                # ignore zero price
                if price == 0.0:
                    continue
                portfolio = self.map_portfolio(key)
                if portfolio.startswith("Postás"):
                    ticker_symbol = portfolio[:10]
                else:
                    ticker_symbol = portfolio[:5]
                yield PortfolioPerformanceHistoricalPrice(
                    file_name=portfolio,
                    date=date,
                    price=price,
                    security_name=f"Aranykor {portfolio}",
                    currency="HUF",
                    ticker_symbol=f"ARANY_{ticker_symbol.upper()}"
                )

    @staticmethod
    def map_portfolio(portfolio):
        match portfolio:
            case "csendelet": return "Csendélet"
            case "klasszikus": return "Klasszikus"
            case "egyensuly": return "Egyensúly"
            case "lendulet": return "Lendület"
            case "esgDinamikus": return "ESG Dinamikus"
            case "postasBazis": return "Postás Bázis-Céldátum"
            case "postasX1": return "Postás X.1 Generáció 2027"
            case "postasX2": return "Postás X.2 Generáció 2037"
            case "postasY": return "Postás Y Generáció 2047"
            case _: raise RuntimeError(f"Unknown portfolio {portfolio}")

"""
Scraper for Budapest VPF and PPF
"""
import datetime
from typing import Any
import scrapy
from scrapy.http import Response

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class BudapestPFSpider(scrapy.Spider):
    """
    Scrapes Budapest VPF and PPF current portfolio prices

    URL: https://www.mbhbank.hu/apps/backend/exchange-rate/exchange-rate-voluntary?active=true&secure=true&fromDate=<from>&toDate=<to>
    """

    name = "budapest_nyugdij"

    def start_requests(self):
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=32)
        start_date = start_date.strftime("%Y%m%d")
        end_date = end_date.strftime("%Y%m%d")
        # pylint: disable=line-too-long
        url = f"https://www.mbhbank.hu/apps/backend/exchange-rate/exchange-rate-voluntary?active=true&secure=true&fromDate={start_date}&toDate={end_date}"
        print(f"{url}")
        yield scrapy.Request(url=url, callback=self.parse)
        # pylint: disable=line-too-long
        url = f"https://www.mbhbank.hu/apps/backend/exchange-rate/exchange-rate-personal?growth=true&balanced=true&classic=true&fromDate={start_date}&toDate={end_date}"
        print(f"{url}")
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response: Response, **kwargs: Any):
        data = response.json()
        for day in data:
            date = day['date']
            del day['date']
            for portfolio, price in day.items():
                (fund_long_name, fund_short_name) = BudapestPFSpider.get_fund_name(portfolio)
                portfolio = BudapestPFSpider.convert_portfolio(portfolio)
                yield PortfolioPerformanceHistoricalPrice(
                        file_name=portfolio,
                        date=date,
                        price=float(price),
                        security_name=f"{fund_long_name} {portfolio} portfólió",
                        currency="HUF",
                        ticker_symbol=f"{fund_short_name}_{portfolio[:5].upper()}"
                    )

    @staticmethod
    def get_fund_name(portfolio: str) -> tuple[str, str]:
        """
        Returns short and long name of the fund based on the portfolio
        """
        match portfolio:
            case "secure" | "active":
                return ("Budapest Önkéntes Nyugdíjpénztár", "BPÖNYP")
            case "growth" | "balanced" | "classic":
                return ("Budapest Magánnyugdíjpénztár", "BPMNYP")


    @staticmethod
    def convert_portfolio(portfolio: str) -> str:
        """
        Converts portfolio names from the rest response to accented official name
        """
        match portfolio:
            case "secure":
                return "Bebiztosító"
            case "active":
                return "Aktív"
            case "growth":
                return "Növekedési"
            case "balanced":
                return "Kiegyensúlyozott"
            case "classic":
                return "Klasszikus"

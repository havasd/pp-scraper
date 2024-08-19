"""
Scraper for MBH VPF
"""
from typing import Any
import scrapy

from scrapy.http import JsonRequest, Response
from price_scraper.items import PortfolioPerformanceHistoricalPrice

class MbhVPFSpider(scrapy.Spider):
    """
    Scrapes MBH Gondoskodás VPF current portfolio prices

    URLs:
    Method: POST
    Recent prices: https://webapi.mbhnyp.hu/publicapi/api/arfolyam/onyp/get_arfolyam
    Contains prices from 2010
    Historical prices: https://webapi.mbhnyp.hu/publicapi/api/arfolyam/onyp/get_arfolyam_ev
    request data: {"requestData":{"ev":"2017"}}
    """

    name = "mbh_nyugdij"
    def start_requests(self):
        url = "https://webapi.mbhnyp.hu/publicapi/api/arfolyam/onyp/get_arfolyam"
        data = {
            "requestData": {
            }
        }
        yield JsonRequest(url=url, callback=self.parse, method="POST", data=data)

    def parse(self, response: Response, **kwargs: Any):
        """
        Parses MBH api response for prices of portfolios.

        Sample data:
        [{
            "ARF_NAP":"2024.07.23",
            "ARF_KI":"2.581960",
            "ARF_KL":"1.809629",
            "ARF_NO":"2.624586",
            "ARF_SK":"1.728762",
            "ARF_Lend":"1.165257"
        }]
        """
        data = response.json()
        for day in data:
            date = day['ARF_NAP'].replace('.', '-')
            del day['ARF_NAP']
            for portfolio, price in day.items():
                portfolio = MbhVPFSpider.convert_portfolio(portfolio)
                yield PortfolioPerformanceHistoricalPrice(
                        file_name=portfolio,
                        date=date,
                        price=price,
                        security_name=f"MBH Gondoskodás Nyugdíjpénztár {portfolio} portfólió",
                        currency="HUF",
                        ticker_symbol=f"MBHNY_{portfolio[:5].upper()}"
                    )

    @staticmethod
    def convert_portfolio(portfolio: str) -> str:
        """
        Converts portfolio names from the rest response to accented official name
        """
        match portfolio:
            case "ARF_KI":
                return "Kiszámítható"
            case "ARF_KL":
                return "Klasszikus"
            case "ARF_NO":
                return "Kiegyensúlyozott"
            case "ARF_SK":
                return "Növekedési"
            case "ARF_Lend":
                return "Lendületes"

"""
Scraper for Pannónia VPF
"""

from typing import Any
import scrapy
from scrapy.http import Response

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class PannoniaVPFSpider(scrapy.Spider):
    """
    Scrapes Pannónia VPF portfolio prices

    URL: https://tagiportalpnyp.pannonianyp.hu/ingridportal/api/public/ugyfelszolgalat/getarfolyamok
    """

    name = "pannonia_nyugdij"

    def start_requests(self):
        # pylint: disable=line-too-long
        url = "https://tagiportalpnyp.pannonianyp.hu/ingridportal/api/public/ugyfelszolgalat/getarfolyamok"
        yield scrapy.Request(url=url, callback=self.parse, method="POST")

    def parse(self, response: Response, **kwargs: Any):
        data = response.json()
        for elem in data:
            portfolio = elem['label']
            for day in elem['data']:
                date = day['x'].split('T')[0]
                price = day['y']
                yield PortfolioPerformanceHistoricalPrice(
                    file_name=portfolio,
                    date=date,
                    price=price,
                    security_name=f"Pannónia Nyugdíjpénztár {portfolio} portfólió",
                    currency="HUF",
                    ticker_symbol=f"PANÖNYP_{portfolio[:5].upper()}".replace(' ', '')
                )

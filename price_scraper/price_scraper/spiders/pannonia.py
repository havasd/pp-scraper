import scrapy

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class PannoniaVPFSpider(scrapy.Spider):
    """
    Scrapes Pannónia VPF portfolio prices

    URL: https://www.mbhbank.hu/apps/backend/exchange-rate/exchange-rate-voluntary?active=true&secure=true&fromDate=<from>&toDate=<to>
    """

    name = "pannonia_nyugdij"

    def start_requests(self):
        url = "https://tagiportalpnyp.pannonianyp.hu/ingridportal/api/public/ugyfelszolgalat/getarfolyamok"
        yield scrapy.Request(url=url, callback=self.parse, method="POST")

    def parse(self, response):
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
                    name=f"Pannónia Nyugdíjpénztár {portfolio} portfólió",
                    currency="HUF",
                    symbol=f"PANÖNYP_{portfolio[:5].upper()}"
                )

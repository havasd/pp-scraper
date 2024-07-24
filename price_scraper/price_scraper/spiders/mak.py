import datetime
import scrapy

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class MakDailySpider(scrapy.Spider):
    """
    Scrapes Hungarian Goverment Bonds daily quotes

    URL to get the suffix of pricing path: https://www.allampapir.hu/api/network_rate/m/get_papers
    URL for prices: https://www.allampapir.hu/api/network_rate/m/get_prices/<security type>
    """

    name = "mak"
    start_urls = ["https://www.allampapir.hu/api/network_rate/m/get_papers"]

    def parse(self, response):
        """
        Scrapes the available bond types
        """
        content = response.json()
        for bond_type in content['data'].keys():
            yield scrapy.Request(
                f"https://www.allampapir.hu/api/network_rate/m/get_prices/{bond_type}",
                callback=self.parse_type
            )


    def parse_type(self, response):
        """
        Scrapes specific bond type
        """
        content = response.json()
        for product in content['data']['data']:
            bid_pct = product.get('bidPrice', '1').replace(',', '.')
            # in some cases they return empty string
            if not bid_pct:
                bid_pct = '1'
            bid_pct = float(bid_pct) / 100

            security_type = product['securityType']
            # simplified notation for this type
            if security_type == "MÁP Plusz":
                security_type = "MÁPP"

            # custom price calculation for bonds with market prices
            if security_type in ['KTV', 'DKJ']:
                price = bid_pct
            else:
                # 0 is represented as integer and non-zero interest is string
                accrued_interest = product['accruedInterest']
                if isinstance(accrued_interest, str):
                    accrued_interest = float(product['accruedInterest'].replace(',', '.'))
                price = bid_pct + accrued_interest / 100

            yield PortfolioPerformanceHistoricalPrice(
                name=f"{security_type}_{product['name']}",
                date=product['settleDate'].replace('.', '-'),
                price=price
            )
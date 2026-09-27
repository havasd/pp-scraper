"""
Scraper for Alfa VPF
"""

import datetime
import re
from typing import Any

import scrapy
from scrapy.http import JsonRequest, Response

from dateutil.relativedelta import relativedelta
from price_scraper.items import PortfolioPerformanceHistoricalPrice


class AlfaVPFSpider(scrapy.Spider):
    """
    Scrapes Alpha VPF current portfolio prices

    Flow:
    1. GET https://www.alfanyugdij.hu/arfolyamrajzolo/
       -> extract WP REST nonce from `var wpApiSettings = {..."nonce":"..."}`
    2. POST https://www.alfanyugdij.hu/wp-json/alfa/v1/rates
       Headers: Content-Type: application/json, X-WP-Nonce: <nonce>
       Body: {"bond": [...], "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}
    """

    name = 'alfa_nyugdij'
    page_url = 'https://www.alfanyugdij.hu/arfolyamrajzolo/'
    api_url = 'https://www.alfanyugdij.hu/wp-json/alfa/v1/rates'
    nonce_re = re.compile(r'wpApiSettings\s*=\s*\{.*?"nonce"\s*:\s*"([0-9a-f]+)"', re.S)
    bond_id_mapping = {
        '13': 'Klasszikus',
        '14': 'Kiegyensúlyozott',
        '16': 'Növekedési',
        '72': 'Szakértői abszolút hozam',
        '73': 'MegaTrend',
        '232': 'Pénzpiaci',
    }

    async def start(self):
        # Scrapy >= 2.13 entry point
        for request in self.start_requests():
            yield request

    def start_requests(self):
        # Scrapy < 2.13 entry point
        # the REST endpoint requires a WordPress nonce which is embedded in the page HTML
        yield scrapy.Request(url=self.page_url, callback=self.parse_nonce, dont_filter=True)

    def parse_nonce(self, response: Response, **kwargs: Any):
        match = self.nonce_re.search(response.text)
        if not match:
            self.logger.error('Could not find wpApiSettings nonce on %s', response.url)
            return
        nonce = match.group(1)

        end_date = datetime.date.today()
        # default scrape interval is current month-2 months just to be safe to not to miss anything
        curr_date = end_date - relativedelta(months=2)
        # this date range can be used for historical querying
        # curr_date = datetime.date(2015, 10, 1)
        yield JsonRequest(
            url=self.api_url,
            callback=self.parse,
            method='POST',
            headers={
                'X-WP-Nonce': nonce,
                'Referer': self.page_url,
            },
            data={
                'bond': list(self.bond_id_mapping.keys()),
                'start_date': curr_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
            },
            dont_filter=True,
        )

    def parse(self, response: Response, **kwargs: Any):
        """
        Parses portfolio prices from the JSON response.

        Sample response:
        {
            "success":true,
            "data":{
                "2024-08-01":[
                    {
                        "kotveny_id":"13",
                        "ertek":"1.204955",
                        "erteknap":"2024-08-01"
                    },
                ]
            }
        }
        """
        payload = response.json()
        if not payload.get('success'):
            self.logger.error('API returned unsuccessful response: %s', response.text[:500])
            return
        data = payload.get('data')
        if not data:
            return

        for date, prices in data.items():
            for price in prices:
                portfolio = self.bond_id_mapping.get(price['kotveny_id'])
                if portfolio is None:
                    self.logger.warning('Unknown kotveny_id: %s', price['kotveny_id'])
                    continue
                yield PortfolioPerformanceHistoricalPrice(
                    file_name=portfolio.split(' ')[0],
                    date=date,
                    price=float(price['ertek'].replace(',', '.')),
                    security_name=f'Alfa Nyugdíjpénztár {portfolio} portfólió',
                    currency='HUF',
                    ticker_symbol=f'ALFÖNYP_{portfolio[:5].upper()}',
                )

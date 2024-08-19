"""
Scraper for Alfa VPF
"""

import datetime
from typing import Any
import scrapy
from scrapy.http import Response

from dateutil.relativedelta import relativedelta
from price_scraper.items import PortfolioPerformanceHistoricalPrice

class AlfaVPFSpider(scrapy.Spider):
    """
    Scrapes Alpha VPF current portfolio prices

    URLs:
    Method: POST
    https://www.alfanyugdij.hu/wp-admin/admin-ajax.php
    """

    name = 'alfa_nyugdij'
    bond_id_mapping = {
        '13': 'Klasszikus',
        '14': 'Kiegyensúlyozott',
        '16': 'Növekedési',
        '72': 'Szakértői abszolút hozam',
        '73': 'MegaTrend',
        '232': 'Pénzpiaci',
    }

    def start_requests(self):
        end_date = datetime.date.today()
        # default scrape interval is current month-2 months just to be safe to not to miss anything
        curr_date = end_date - relativedelta(months=2)
        # this date range can be used for historical querying
        #curr_date = datetime.date(2015, 10, 1)
        yield scrapy.FormRequest(
            url='https://www.alfanyugdij.hu/wp-admin/admin-ajax.php',
            callback=self.parse,
            method='POST',
            formdata={
                'bond[]': ['13', '14', '16', '72', '73', '232'],
                'start_date': curr_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'action': 'aegon_rates',
            }
        )

    def parse(self, response: Response, **kwargs: Any):
        """
        Parses portfolios from the response which is a complete page.

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
        data = response.json()['data']
        if not data:
            return

        for date, prices in data.items():
            for price in prices:
                portfolio = self.bond_id_mapping[price['kotveny_id']]
                yield PortfolioPerformanceHistoricalPrice(
                        file_name=portfolio.split(' ')[0],
                        date=date,
                        price=float(price['ertek'].replace(',', '.')),
                        security_name=f'Alfa Nyugdíjpénztár {portfolio} portfólió',
                        currency='HUF',
                        ticker_symbol=f'ALFÖNYP_{portfolio[:5].upper()}'
                    )

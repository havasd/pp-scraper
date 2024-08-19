"""
Scraper for Horizont PPF
"""
import datetime
from typing import Any
import scrapy
from scrapy.http import Response

from dateutil.relativedelta import relativedelta
from price_scraper.items import PortfolioPerformanceHistoricalPrice

class MbhVPFSpider(scrapy.Spider):
    """
    Scrapes Horizont PPF current portfolio prices

    URLs:
    Method: POST
    https://horizontmagannyugdijpenztar.hu/arfolyamok request data: `date=2024-02`
    """

    name = 'horizont_nyugdij'

    def start_requests(self):
        end_date = datetime.date.today()
        # default scrape interval is current month-1 months just to be safe to not to miss anything
        curr_date = end_date - relativedelta(months=1)
        while curr_date <= end_date:
            yield scrapy.FormRequest(
                url='https://horizontmagannyugdijpenztar.hu/arfolyamok',
                callback=self.parse,
                method='POST',
                formdata={
                    'date': curr_date.strftime('%Y-%m')
                },
            )
            curr_date = curr_date + relativedelta(months=1)

    def parse(self, response: Response, **kwargs: Any):
        """
        Parses portfolios from the response which is a complete page.
        """
        table = response.css('div.rates-table').css('div.table-row')
        if not table:
            return None
        headers = table[0].css('div.column')
        portfolios = []
        for header in headers:
            portfolios.append(' '.join(header.css('::text').extract()))
        # strip the first element because that is not needed
        portfolios = portfolios[1:]

        for row in table[1:]:
            row_datas = row.css('div.column::text').extract()
            date = row_datas[0]
            for idx, data in enumerate(row_datas[1:]):
                portfolio = portfolios[idx]
                price = float(data.replace(',', '.'))
                yield PortfolioPerformanceHistoricalPrice(
                        file_name=portfolio.split(' ')[0],
                        date=date,
                        price=price,
                        security_name=f'Horizont Magánnyugdíjpénztár {portfolio}',
                        currency='HUF',
                        ticker_symbol=f'HORMNYP_{portfolio[:5].upper()}'
                    )

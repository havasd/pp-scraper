import datetime
import scrapy

from dateutil.relativedelta import relativedelta
from price_scraper.items import PortfolioPerformanceHistoricalPrice

class HonvedVPFSpider(scrapy.Spider):
    """
    Scrapes Honved VPF current portfolio prices

    URLs:
    Method: POST
    https://hnyp.hu/arfolyamok
    """

    name = 'honved_nyugdij'

    def start_requests(self):
        end_date = datetime.date.today()
        # default scrape interval is current month-2 months just to be safe to not to miss anything
        curr_date = end_date - relativedelta(months=2)
        # this date range can be used for historical querying
        # curr_date = datetime.date(2008, 1, 1)
        yield scrapy.FormRequest(
            url='https://hnyp.hu/arfolyamok',
            callback=self.parse,
            method='POST',
            formdata={
                'datum[tol]': curr_date.strftime('%Y/%m/%d'),
                'datum[ig]': end_date.strftime('%Y/%m/%d'),
                'portfolio[klasszikus]': '1',
                'portfolio[kiegyensulyozott]': '1',
                'portfolio[novekedesi]': '1',
                'portfolio[penzpiaci]': '1',
            }
        )

    def parse(self, response):
        """
        Parses portfolios from the response which is a complete page.
        """
        table = response.css('td.cikk').css('tr')
        if not table:
            return None
        # strip the first element because that is not needed
        portfolios = [r.css('::text').get() for r in table[0].css('td')][1:]

        for row in table[1:]:
            row_datas = [r.css('::text').get() for r in row.css('td')]
            if not row_datas:
                continue

            date = row_datas[0].replace('.', '-')
            for idx, data in enumerate(row_datas[1:]):
                portfolio = portfolios[idx]
                price = float(data.replace(',', '.'))
                yield PortfolioPerformanceHistoricalPrice(
                        file_name=portfolio.split(' ')[0],
                        date=date,
                        price=price,
                        security_name=f"Honvéd Közszolgálati Önkéntes Nyugdíjpénztár {portfolio}",
                        currency="HUF",
                        ticker_symbol=f"HONÖNYP_{portfolio[:5].upper()}"
                    )

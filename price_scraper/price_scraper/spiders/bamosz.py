"""
Scraper for Bamosz listed funds
"""

from functools import partial
from typing import Any
import scrapy
from scrapy.http import Response
import scrapy_splash

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class BamoszSpider(scrapy.Spider):
    """
    Scrapes Bamosz website for daily hungarian fund prices

    Base URL: https://www.bamosz.hu/legfrissebb-adatok
    """

    name = "bamosz"
    start_urls = ["https://www.bamosz.hu/legfrissebb-adatok"]

    def __init__(self, *args, scrape_historical_data=False, **kwargs):
        super(BamoszSpider, self).__init__(*args, **kwargs)
        self.scrape_historical_data = scrape_historical_data

    def parse(self, response: Response, **kwargs: Any):
        table = response.css('div[id="A6951:urlap:alapData_content"]')[0]
        fund_groups = table.css('table[class="dataTable2 alapokContainer specEvenOddTableGrey"]')

        instruments = []
        for group in fund_groups:
            rows = group.css('tr')
            for idx in range(2, len(rows), 2):
                name_row = rows[idx]
                data_row = rows[idx + 1]
                long_fund_name = name_row.css('a::text').getall()[0].strip()
                isin = extract_isin(name_row.css('td').getall()[1])
                fund_data = data_row.css('td::text').getall()
                fund_data = sanitize_columns(fund_data)
                currency = fund_data[1],
                yield PortfolioPerformanceHistoricalPrice(
                    security_name=long_fund_name,
                    file_name=isin,
                    # it has a trailing dot in the date but we shouldn't remove it
                    date=fund_data[3].replace('.', '-')[:-1],
                    price=float(fund_data[2].replace(',', '.')),
                    currency=currency,
                    isin=isin,
                )

                instruments.append({
                    'isin': isin,
                    'currency': currency,
                    'security_name': long_fund_name,
                })

        # you need to reduce the concurrent requests to 1
        # you need to start splash separately with
        # docker run -p 8050:8050 scrapinghub/splash
        if self.scrape_historical_data:
            for instrument in instruments:
                yield self.get_url_for_fund_page(instrument)

    def get_url_for_fund_page(self, instrument):
        """
        First query: https://www.bamosz.hu/legfrissebb-adatok
        then scrape ISINs and call https://www.bamosz.hu/alapoldal?isin=<isin>

        After that create a post request
        https://www.bamosz.hu/web/guest/alapoldal?_bamoszpublicalapoldal_WAR_bamoszpublicalapoldalportlet_INSTANCE_N4Uk__facesViewId=/view.xhtml&p_p_col_count=2&p_p_col_id=column-1&p_p_col_pos=1&p_p_id=bamoszpublicalapoldal_WAR_bamoszpublicalapoldalportlet_INSTANCE_N4Uk&p_p_lifecycle=2&p_p_mode=view&p_p_state=normal
        """
        url = f'https://www.bamosz.hu/alapoldal?isin={instrument["isin"]}'
        return scrapy_splash.SplashRequest(
            url=url,
            callback=partial(self.request_historical_data, instrument)
        )

    def request_historical_data(self, instrument, response):
        """
        Creates a post rquest which requests historical data for instrument
        """
        self.logger.info("Scrapgin data for ISIN: %s", instrument['isin'])
        request = scrapy_splash.SplashFormRequest.from_response(
            response,
            formid='A3225:j_idt8',
            formdata={
                'A3225:j_idt8:startDate_input': '2024.09.01',
            },
            callback=partial(self.parse_historical_data, instrument)
        )
        yield request

    def parse_historical_data(self, instrument, response):
        """
        Parses historical data for an instrument from the response
        """
        # we have 2 tables one is hidden because of ajax craziness
        table = response.css('table.dataTable2')[-1]
        rows = table.css('tr')
        # first line headers
        for row in rows[1:]:
            data = sanitize_columns(row.css('td::text').getall())
            # sometimes there is no price on certain dates
            if not data[1]:
                continue
            yield PortfolioPerformanceHistoricalPrice(
                security_name=instrument['security_name'],
                file_name=instrument['isin'],
                # it has a trailing dot in the date but we shouldn't remove it
                date=data[0].replace('.', '-')[:-1],
                price=float(data[1].replace(',', '.')),
                currency=instrument['currency'],
                isin=instrument['isin'],
            )

def sanitize_columns(columns):
    """
    Trims whitespaces and removes newlines
    """
    data = []
    for column in columns:
        column = column.strip()
        column = column.replace('\xa0', '')
        data.append(column)

    return data

def extract_isin(input_data: str):
    """
    Extracts ISIN number from the given string for HU instruments
    """
    idx = input_data.find("'HU") + 1
    idx_end = input_data.find("'", idx)
    return input_data[idx:idx_end]

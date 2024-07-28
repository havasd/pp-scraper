
import scrapy

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class BamoszSpider(scrapy.Spider):
    """
    Scrapes Bamosz website for daily hungarian fund prices

    Base URL: https://www.bamosz.hu/legfrissebb-adatok
    """

    name = "bamosz"
    start_urls = ["https://www.bamosz.hu/legfrissebb-adatok"]

    def parse(self, response):
        table = response.css('div[id="A6951:urlap:alapData_content"]')[0]
        fund_groups = table.css('table[class="dataTable2 alapokContainer specEvenOddTableGrey"]')
        for group in fund_groups:
            rows = group.css('tr')
            for idx in range(2, len(rows), 2):
                name_row = rows[idx]
                data_row = rows[idx + 1]
                long_fund_name = name_row.css('a::text').getall()[0].strip()
                isin = extract_isin(name_row.css('td').getall()[1])
                fund_data = data_row.css('td::text').getall()
                fund_data = sanitize_columns(fund_data)
                yield PortfolioPerformanceHistoricalPrice(
                    name=isin,
                    # it has a trailing dot in the date but we shouldn't remove it
                    date=fund_data[3].replace('.', '-')[:-1],
                    price=float(fund_data[2].replace(',', '.')),
                    currency=fund_data[1],
                    long_name=long_fund_name,
                )

def sanitize_columns(columns):
    """
    Trims whitespaces and removes newlines
    """
    data = []
    for column in columns:
        column = column.strip()
        data.append(column)

    return data

def extract_isin(input_data: str):
    """
    Extracts ISIN number from the given string for HU instruments
    """
    idx = input_data.find("'HU") + 1
    idx_end = input_data.find("'", idx)
    return input_data[idx:idx_end]

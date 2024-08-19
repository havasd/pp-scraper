import csv
import scrapy

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class ErsteVPFSpider(scrapy.Spider):
    """
    Scrapes Erste VPF current portfolio prices

    URLs: https://www.erstenyugdijpenztar.hu/tagiportal/hu/arfolyamok.html

    Returns data with redirect. It returns roughly 3 years of data up to today
    """

    name = "erste_nyugdij"
    start_urls = ["https://www.erstenyugdijpenztar.hu/tagiportal/hu/arfolyamok.html"]

    def parse(self, response):
        """
        Parses portfolios from the response which is a complete page.
        """

        # if you want to export csv data with this spider make sure to change this
        if False:
            for item in self.parse_csv('<replace with your path to csv>'):
                yield item
            return

        table = response.css('table.arfolyamTable')
        if not table:
            return None

        headers = table.css('thead').css('tr')[1].css('th')
        headers = [header.css('::text').get() for header in headers]
        portfolios = [header for header in headers if header]
        for row in table.css('tbody').css('tr'):
            prices = [r.css('::text').get() for r in row.css('td')]
            date = row.css('th::text').get()
            for idx, data in enumerate(prices):
                if not data:
                    continue
                portfolio = portfolios[idx]
                price = float(data.replace(',', '.'))
                yield PortfolioPerformanceHistoricalPrice(
                        file_name=portfolio.split(' ')[0],
                        date=date.replace('.', '-'),
                        price=price,
                        security_name=f"Erste Nyugdíjpénztár {portfolio} portfólió",
                        currency="HUF",
                        ticker_symbol=f"ERÖNYP_{portfolio[:5].upper()}"
                    )

    def parse_csv(self, path):
        """
        Parses table from csv file. Primarily used for historical price generation
        You can create such csv by copying the table from
        https://www.erstenyugdijpenztar.hu/tagiportal/hu/arfolyamok.html
        into an excel and export is as CSV with `;` separated list.
        """
        with open(path, 'r', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file, delimiter=';')
            portfolios = [portfolio for portfolio in next(reader)][1:]
            for row in reader:
                date = row[0].replace('.', '-')
                for idx, column in enumerate(row[1:]):
                    if not column:
                        continue
                    portfolio = portfolios[idx]
                    yield PortfolioPerformanceHistoricalPrice(
                            file_name=portfolio,
                            date=date,
                            price=float(column),
                            security_name=f"Erste Nyugdíjpénztár {portfolio} portfólió",
                            currency="HUF",
                            ticker_symbol=f"ERÖNYP_{portfolio[:5].upper()}"
                        )

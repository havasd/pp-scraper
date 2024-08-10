import datetime
import scrapy

from dateutil.relativedelta import relativedelta
from price_scraper.items import PortfolioPerformanceHistoricalPrice

class AllianzVPFSpider(scrapy.Spider):
    """
    Scrapes Allianz VPF current portfolio prices scraper

    URLs:
    https://penztar.allianz.hu/web_graf/Graf_tabla.php?kezdes=20080101&vege=20240718
    """

    name = "allianz_nyugdij"

    def start_requests(self):
        end_date = datetime.date.today()
        start_date = end_date - relativedelta(days=20)
        # for historical generation
        start_date = datetime.date(2008, 1, 1)
        url = f'https://penztar.allianz.hu/web_graf/Graf_tabla.php?kezdes={start_date.strftime("%Y%m%d")}&vege={end_date.strftime("%Y%m%d")}'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        Parses portfolios from the response which is a complete page.
        """
        table = response.css('tr')
        if not table:
            return None

        portfolios = [r.css('::text').get() for r in table[4].css('td')]
        portfolios = AllianzVPFSpider.sanitize_table_columns(portfolios)
        # strip the first element because that is not needed
        portfolios = portfolios[1:]

        for row in table[5:]:
            row_datas = [r.css('::text').get() for r in row.css('td')]
            row_datas = AllianzVPFSpider.sanitize_table_columns(row_datas)
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
                        name=f"Allianz Önkéntes Pénztárak {portfolio}",
                        currency="HUF",
                        symbol=f"ALLÖNYP_{portfolio[:5].upper()}"
                    )

    @staticmethod
    def sanitize_table_columns(columns):
        """
        Removes empty and \xa0 elements in the array.
        """
        return list(filter(lambda c: c and c != '\xa0', columns))

import csv
import datetime
import io
import scrapy

from functools import partial

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class OtpVPFSpider(scrapy.Spider):
    """
    Scrapes OTP VPF current portfolio prices

    URL: https://www.otpnyugdij.hu/api/arfolyam/aktualis
    """

    name = "otp_nyugdij"
    start_urls = ["https://www.otpnyugdij.hu/api/arfolyam/aktualis"]

    def parse(self, response):
        data = response.json()
        for day in data:
            date = day['pdate']
            date = f"{date[0:4]}-{date[4:6]}-{date[6:]}"
            del day['pdate']
            print(day)
            for portfolio, price in day.items():
                portfolio = OtpVPFSpider.convert_portfolio(portfolio)
                yield PortfolioPerformanceHistoricalPrice(
                        file_name=portfolio,
                        date=date.replace('.', '-'),
                        price=price,
                        name=f"OTP Önkéntes Nyugdíjpénztári {portfolio} portfólió",
                        currency="HUF",
                        symbol=f"OTPNY_{portfolio[:5].upper()}"
                    )

    @staticmethod
    def convert_portfolio(portfolio: str) -> str:
        """
        Converts portfolio names from the rest response to accented official name
        """
        match portfolio:
            case "novekedesiPortfolio":
                return "Növekedési"
            case "klasszkusPortfolio":
                return "Klasszikus"
            case "kockazatkeruloPortfolio":
                return "Kockázatkerülő"
            case "ovatosPortfolio":
                return "Óvatos"
            case "kiegyensulyozottPortfolio":
                return "Kiegyensúlyozott"
            case "dinamikusPortfolio":
                return "Dinamikus"

class OtpVPFHistoricalSpider(scrapy.Spider):
    """
    Scrapes OTP VPF historical portfolio prices

    URL:
    https://www.otpnyugdij.hu/api/arfolyam/letoltes?portfolios=<portfolio>&startDate=20080101&endDate=<today>
    """

    name = "otp_nyugdij_historical"

    def start_requests(self):
        today = datetime.date.today()
        today = today.strftime("%Y%m%d")
        portfolios = ["Kockázatkerülő", "Klasszikus", "Óvatos", "Kiegyensúlyozott", "Növekedési", "Dinamikus"]
        for portfolio in portfolios:
            url = f"https://www.otpnyugdij.hu/api/arfolyam/letoltes?portfolios={portfolio}&startDate=20080101&endDate={today}"
            yield scrapy.Request(url=url, callback=partial(self.parse, portfolio))

    def parse(self, portfolio, response):
        buffer = io.StringIO(response.text)
        reader = csv.reader(buffer, delimiter=';')

        # headers not needed
        next(reader)
        next(reader)
        for row in reader:
            if not row or not row[1]:
                continue
            # this means we don't need to process this data is it is obselete
            if row[2] == 'Archivált':
                continue
            yield PortfolioPerformanceHistoricalPrice(
                    file_name=portfolio,
                    date=row[0].strip()[:-1].replace('.', '-').replace(' ', ''),
                    price=float(row[1]),
                    name=f"OTP Önkéntes Nyugdíjpénztári {portfolio} Portfólió",
                    currency="HUF",
                    symbol=f"OTPNY_{portfolio[:5].upper()}"
                )

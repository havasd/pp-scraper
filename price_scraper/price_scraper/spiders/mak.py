"""
Scraper for MAK government bonds
"""
import datetime
import re
from functools import partial
from typing import Any
import scrapy
from scrapy.http import JsonRequest, Response
from pdf2image import convert_from_bytes
import pytesseract

from price_scraper.items import PortfolioPerformanceHistoricalPrice


class MakDailySpider(scrapy.Spider):
    """
    Scrapes Hungarian Goverment Bonds daily quotes

    URL to get the suffix of pricing path: https://www.allampapir.hu/api/network_rate/m/get_papers
    URL for prices: https://www.allampapir.hu/api/network_rate/m/get_prices/<security type>
    """

    name = "mak"
    start_urls = ["https://www.allampapir.hu/api/network_rate/m/get_papers"]

    def parse(self, response: Response, **kwargs: Any):
        """
        Scrapes the available bond types
        """
        content = response.json()
        for bond_type in content['data'].keys():
            yield scrapy.Request(
                f"https://www.allampapir.hu/api/network_rate///m/get_prices/{bond_type}",
                callback=self.parse_type
            )


    def parse_type(self, response):
        """
        Scrapes specific bond type
        """
        content = response.json()
        for product in content['data']['data']:
            days_until_expiry = product['maturityInDays_val']
            settlement_date = product['settleDate'].replace('.', '-')
            security_type = product['securityType']
            # simplified notation for this type
            if security_type == "MÁP Plusz":
                security_type = "MÁPP"

            bid_pct = product.get('bidPrice', '')
            if bid_pct:
                bid_pct = float(bid_pct.replace(',', '.'))
                # custom price calculation for bonds with market prices
                if security_type != 'DKJ':
                    # 0 is represented as integer and non-zero interest is string
                    accrued_interest = product['accruedInterest']
                    if isinstance(accrued_interest, str):
                        accrued_interest = float(product['accruedInterest'].replace(',', '.'))
                    bid_pct = bid_pct + accrued_interest
            # when we are close to expiry we will set it to 100 percent on the maturity date
            elif not bid_pct and days_until_expiry <= 5:
                bid_pct = 100
                settlement_date = product['maturityDate'].replace('.', '-')
            # when there is an interest payment or close to expiry there is no bidPrice
            elif not bid_pct:
                continue

            # convert to percentage in numeric
            price = bid_pct / 100

            long_name = security_type_to_long_name(security_type)
            product_name = product['name'].removesuffix('_BABA').removesuffix('_EUR')
            symbol = f"{security_type}_{product_name}"
            # more sensible naming for DKJ
            match security_type:
                case "DKJ":
                    product_name = product_name[1:]
                    symbol = f"{security_type}{product_name}"

            yield PortfolioPerformanceHistoricalPrice(
                file_name=f"{security_type}_{product['name']}",
                date=settlement_date,
                price=price,
                ticker_symbol=symbol,
                security_name=f"{long_name} {product_name}",
                start_date=product['issueDate'],
                currency=product['currency'],
            )

class MakHistoricalSpider(scrapy.Spider):
    """
    Scrapes Hungarian Goverment Bonds historical quotes from 2022.01.01

    We can get historical data by generating pdfs for every day and parse it.
    https://webkincstar.allamkincstar.gov.hu/report-service/report
    POST data: `{"clientCode":"all","reportName":"_20633_arfolyam_mak","language":"hu",
    "report_params":[{"name":"datum","value":"2023-01-02"}]}`
    """

    name = "mak_historical"

    report_names = [
        "_20631_arfolyam_dkj",
        "_20632_arfolyam_kkj",
        "_20633_arfolyam_mak",
        "_20638_arfolyam_pemak",
        "_20642_arfolyam_start",
        "_207461_arfolyam_mapp",
        # these are not useful
        #"_20644_arfolyam_belf_koz",
        #"_20664_arfolyam_omak",
    ]

    # regex which identifies the beginning of the lines inthe table
    line_matcher = re.compile(r"^((K|N)\d{4}\/)|(D\d{6})|(\d{4}\/)")

    def start_requests(self):
        date_ranges = [
            # start, end
            # (datetime.date(2022, 1, 1), datetime.date(2022, 7, 1)),
            # (datetime.date(2022, 7, 2), datetime.date(2023, 1, 1)),
            # (datetime.date(2023, 1, 2), datetime.date(2023, 7, 1)),
            # (datetime.date(2023, 7, 2), datetime.date(2024, 1, 1)),
            # (datetime.date(2024, 1, 2), datetime.date(2024, 8, 14)),
            (datetime.date(2024, 9, 1), datetime.date(2024, 9, 25)),
        ]
        # increment manually
        date_range = date_ranges[0]
        end_date = date_range[1]
        start_date = date_range[0]

        offset = datetime.timedelta(days=1)

        while start_date <= end_date:
            for report_name in self.report_names:
                body = {
                    "clientCode": "all",
                    "reportName":  report_name,
                    #"reportName":  self.report_names[1],
                    "language": "hu",
                    "report_params": [
                        {
                            "name": "datum",
                            "value": start_date.strftime("%Y-%m-%d")
                        }
                    ]
                }
                url = "https://webkincstar.allamkincstar.gov.hu/report-service/report"
                yield JsonRequest(url=url,
                    callback=partial(self.parse, curr_date=start_date, report_name=report_name),
                    data=body,
                    headers={
                        'Accept': '*/*'
                    }
                )

            start_date = start_date + offset


    def parse(self, response: Response, **kwargs: Any):
        """
        Parses daily quote prices for bonds from pdf
        """
        curr_date = kwargs['curr_date']
        report_name = kwargs['report_name']
        self.logger.info("Parsing data for date: %s, report type: %s",
            curr_date.strftime("%Y-%m-%d"),
            report_name
        )
        for item in self.parse_pdf(curr_date, response.body):
            yield item

    def parse_pdf(self, curr_date, data):
        """
        Converts the given PDF to images and then to string
        """
        images = convert_from_bytes(data)
        for image in images:
            text = pytesseract.image_to_string(image)
            for line in text.splitlines():
                if MakHistoricalSpider.line_matcher.search(line):
                    product = self.parse_data(curr_date, line)
                    if product is None:
                        continue
                    yield product

    def parse_data(self, curr_date, pdf_line):
        """
        Parses specific bond contract from pdf lines.
        This applies to MÁPP, MÁK, PMÁP, PEMÁP, 1MÁP, etc...
        """
        content = [i for i in pdf_line.split() if i != '%']
        name = self.sanitize_symbol(content[0])
        #expiry_date = content[-1]
        security_type = self.symbol_to_security_type(name)
        bid_pct = self.get_bid_pct(content, security_type)
        if bid_pct is None:
            return None

        long_name = security_type_to_long_name(security_type)

        return PortfolioPerformanceHistoricalPrice(
                file_name=f"{security_type}_{name}",
                date=curr_date,
                price=bid_pct,
                ticker_symbol=f"{security_type}_{name}",
                security_name=f"{long_name} {name}",
            )

    def get_bid_pct(self, content, security_type):
        """
        Extracts bid pct with the necessary conversions
        """
        try:
            data = content[self.get_bid_pct_index(security_type)].replace('%', '').replace(',', '.')
        except IndexError:
            self.logger.error("Not enough data: %s", content)
            return None
        # there is no pricing for this day
        if data == '-':
            return None
        if data.startswith('0'):
            data = '1' + data
        bid_pct = float(data) / 100
        return round(bid_pct, 10)


    def sanitize_symbol(self, symbol: str):
        """
        Corrects OCR errors
        """
        if symbol.endswith('/1') or symbol.endswith('/|') or symbol.endswith('/!'):
            symbol = symbol[:-1] + 'I'
        elif symbol.endswith('/0'):
            symbol = symbol[:-1] + 'O'
        elif symbol.endswith('/)') or symbol.endswith('/}'):
            symbol = symbol[:-1] + 'J'
        elif symbol.endswith('.') or symbol.endswith(','):
            symbol = symbol[:-1]
        elif symbol.endswith('/6'):
            symbol = symbol[:-1] + 'C'
        return symbol

    def get_bid_pct_index(self, security_type):
        """
        Returns the index in the line in which the bid pct + accrued interest is
        """
        match security_type:
            case 'DKJ':
                return 1
            case 'BABA':
                return 2
            case _:
                return 3

    def symbol_to_security_type(self, name: str):
        """
        Converts symbol to security_type.
        This is primarily the data from the PDF
        """
        match name:
            case name if name[0] == 'N':
                return 'MÁPP'
            case name if name[0] == 'N' and name[-2] in ['M']:
                return 'MÁPP_T'
            case name if name[0] == 'D':
                return 'DKJ'
            case name if name[0] == 'K':
                return '1MÁP'
            case name if name[-6:] == 'S_BABA':
                return 'BABA'
            case name if name[-5:] == 'U_EUR':
                return 'EMÁP'
            case name if name[-5:] in ['X_EUR', 'Y_EUR']:
                return 'PEMÁP'
            case name if name[-1] in ['I', 'J', 'K', 'L']:
                return 'PMÁP'
            case name if name[-2:] in ['Q1', 'Q2', 'Q3', 'Q4']:
                return 'FixMáp'
            case name if name[-1] in ['N', 'O', 'P', 'R']:
                return 'BMÁP'
            case _:
                return 'KTV'


def security_type_to_long_name(security_type: str):
    """
    Converts short bond security types to long names
    """
    match security_type:
        case "1MÁP":
            return "Egyéves Magyar Állampapír"
        case "BABA":
            return "Babakötvény"
        case "BMÁP":
            return "Bónusz Magyar Állampapír"
        case "DKJ":
            return "Diszkont Kincstárjegy"
        case "EMÁP":
            return "Euro Magyar Állampapír"
        case "FixMÁP":
            return "Fix Magyar Állampapír"
        case "KTV":
            return "Magyar Államkötvény"
        case "MÁPP" | "MÁPP_T":
            return "Magyar Állampapír Plusz"
        case "PEMÁP":
            return "Prémium Euró Magyar Állampapír"
        case "PMÁP":
            return "Prémium Magyar Állampapír"

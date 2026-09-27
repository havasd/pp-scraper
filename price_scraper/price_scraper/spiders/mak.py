"""
Scraper for MAK government bonds
"""
import datetime
import re
import subprocess
from typing import Any, Iterator
import scrapy
from scrapy.http import JsonRequest, Response
# Only needed for the OCR fallback (scanned / text-less PDFs)
from pdf2image import convert_from_bytes
import pytesseract

from price_scraper.items import PortfolioPerformanceHistoricalPrice

class MakDailySpider(scrapy.Spider):
    """
    Scrapes Hungarian Goverment Bonds daily quotes

    You must use a post request.

    URL to get available instruments: https://www.allampapir.hu/api/networkRate/get_papers_with_prices
    URL for prices: https://www.allampapir.hu/api/networkRate/get_prices
    """

    name = "mak"

    def start_requests(self):
        yield scrapy.Request(
            url='https://www.allampapir.hu/kincstari_arfolyamjegyzes',
            callback=self.parse_csrf_token
        )

    def parse_csrf_token(self, response: Response, **kwargs: Any):
        """
        Scrapes the available bond types
        """
        self.csrf_token = str(response.headers['Set-Cookie']).split(';')[0].split('=')[1]

        yield JsonRequest(
            url='https://www.allampapir.hu/api/networkRate/get_papers_with_prices',
            method='POST',
            headers={
                'Origin': 'https://www.allampapir.hu',
                'X-CSRF-TOKEN': self.csrf_token,
            },
            callback=self.parse_bond_types
        )

    def parse_bond_types(self, response: Response, **kwargs: Any):
        content = response.json()
        for bond_type in content['data']['papers'].keys():
            yield JsonRequest(
                url=f"https://www.allampapir.hu/api/networkRate/get_prices",
                method='POST',
                headers={
                    'Origin': 'https://www.allampapir.hu',
                    'X-CSRF-TOKEN': self.csrf_token,
                },
                data={
                    'paper': bond_type,
                },
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
                    symbol = f"{security_type}{product_name[1:]}"

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
    Scrapes Hungarian Government Bonds historical quotes.

    We can get historical data by generating PDFs for every day and parsing them.
    POST https://webkincstar.allamkincstar.gov.hu/report-service/report
    Body:
        {"clientCode":"all","reportName":"_20633_arfolyam_mak","language":"hu",
         "report_params":[{"name":"datum","value":"2026-09-25"}],
         "lang":"hu","channel":"WEB","channelId":2}

    NOTE: since the new webkincstar frontend the `lang` field is mandatory,
    without it the backend answers with HTTP 500 (JSON error body).

    The list of report names can be fetched from:
    GET https://webkincstar.allamkincstar.gov.hu/instrument-service/instrumentgroup/group?lang=hu
    (field: `priceReportName`)
    """

    name = "mak_historical"

    report_url = "https://webkincstar.allamkincstar.gov.hu/report-service/report"

    custom_settings = {
        # be nice with the state treasury backend
        "CONCURRENT_REQUESTS": 4,
        "DOWNLOAD_DELAY": 0.25,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408, 429],
    }

    report_names = [
        "_20631_arfolyam_dkj",      # Diszkont Kincstárjegy
        "_20632_arfolyam_kkj",      # Egyéves Magyar Állampapír
        "_20633_arfolyam_mak",      # MÁK/BMÁP/PMÁP/FixMÁP
        "_20638_arfolyam_pemak",    # EMÁP/PEMÁP
        "_20642_arfolyam_start",    # Babakötvény
        "_207461_arfolyam_mapp",    # Magyar Állampapír Plusz
        # these are not useful
        # "_20644_arfolyam_belf_koz",
        # "_20664_arfolyam_omak",
    ]

    # regex which identifies the beginning of the lines in the table
    # (the text layer can contain leading spaces, e.g. Babakötvény)
    line_matcher = re.compile(r"^\s*(?:[KN]\d{4}/|D\d{6}\b|\d{4}/)")

    async def start(self):
        # Scrapy >= 2.13 entry point; older versions use start_requests()
        for request in self.start_requests():
            yield request

    def start_requests(self):
        date_ranges = [
            # start, end
            # (datetime.date(2022, 1, 1), datetime.date(2022, 7, 1)),
            # (datetime.date(2022, 7, 2), datetime.date(2023, 1, 1)),
            # (datetime.date(2023, 1, 2), datetime.date(2023, 7, 1)),
            # (datetime.date(2023, 7, 2), datetime.date(2024, 1, 1)),
            # (datetime.date(2024, 1, 2), datetime.date(2024, 8, 14)),
            (datetime.date(2026, 9, 15), datetime.date(2026, 9, 25)),
        ]
        # increment manually
        start_date, end_date = date_ranges[0]
        offset = datetime.timedelta(days=1)

        while start_date <= end_date:
            for report_name in self.report_names:
                yield JsonRequest(
                    url=self.report_url,
                    data=self.build_body(report_name, start_date),
                    headers={"Accept": "application/pdf"},
                    callback=self.parse,
                    errback=self.on_error,
                    cb_kwargs={"curr_date": start_date, "report_name": report_name},
                    # the URL/body pair is unique, but be explicit
                    dont_filter=True,
                )
            start_date += offset

    @staticmethod
    def build_body(report_name: str, date: datetime.date) -> dict:
        return {
            "clientCode": "all",
            "reportName": report_name,
            "language": "hu",
            "report_params": [
                {"name": "datum", "value": date.strftime("%Y-%m-%d")},
            ],
            # new, required by the new frontend/backend
            "lang": "hu",
            "channel": "WEB",
            "channelId": 2,
        }

    def on_error(self, failure):
        request = failure.request
        self.logger.error(
            "Request failed for %s / %s: %r",
            request.cb_kwargs.get("curr_date"),
            request.cb_kwargs.get("report_name"),
            failure.value,
        )

    def parse(self, response: Response, **kwargs: Any):
        """
        Parses daily quote prices for bonds from pdf
        """
        curr_date: datetime.date = kwargs["curr_date"]
        report_name: str = kwargs["report_name"]

        content_type = response.headers.get("Content-Type", b"").decode().lower()
        if "application/pdf" not in content_type or not response.body.startswith(b"%PDF"):
            self.logger.error(
                "Not a PDF for date %s, report %s (status %s, type %s): %s",
                curr_date, report_name, response.status, content_type, response.text[:300],
            )
            return

        self.logger.info("Parsing data for date: %s, report type: %s",
                         curr_date.strftime("%Y-%m-%d"), report_name)

        yield from self.parse_pdf(curr_date, response.body)

    # ------------------------------------------------------------------ PDF

    def parse_pdf(self, curr_date: datetime.date, data: bytes) -> Iterator[Any]:
        """
        The new PDFs contain a real text layer, so we read that (fast and
        exact). OCR is kept only as a fallback if no text could be extracted.
        """
        text = self.pdf_to_text(data)
        ocr = False
        if not text.strip():
            self.logger.warning("No text layer in PDF for %s, falling back to OCR", curr_date)
            text = self.pdf_to_text_ocr(data)
            ocr = True

        for line in text.splitlines():
            if not self.line_matcher.search(line):
                continue
            product = self.parse_data(curr_date, line, ocr=ocr)
            if product is not None:
                yield product

    @staticmethod
    def pdf_to_text(data: bytes) -> str:
        """
        Uses poppler's pdftotext (already a dependency of pdf2image).
        `-layout` keeps the table columns on one line.
        """
        try:
            result = subprocess.run(
                ["pdftotext", "-layout", "-enc", "UTF-8", "-", "-"],
                input=data, capture_output=True, check=True, timeout=60,
            )
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
            return ""
        return result.stdout.decode("utf-8", errors="replace")

    @staticmethod
    def pdf_to_text_ocr(data: bytes) -> str:
        images = convert_from_bytes(data)
        return "\n".join(pytesseract.image_to_string(image) for image in images)

    # --------------------------------------------------------------- parsing

    def parse_data(self, curr_date: datetime.date, pdf_line: str, ocr: bool = False):
        """
        Parses specific bond contract from pdf lines.
        This applies to MÁPP, MÁK, PMÁP, PEMÁP, 1MÁP, etc...
        """
        content = [i for i in pdf_line.split() if i != "%"]
        if not content:
            return None
        name = self.sanitize_symbol(content[0]) if ocr else content[0]
        security_type = self.symbol_to_security_type(name)
        bid_pct = self.get_bid_pct(content, security_type, ocr=ocr)
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

    def get_bid_pct(self, content, security_type, ocr: bool = False):
        """
        Extracts bid pct (gross = net + accrued interest; for DKJ the net price)
        """
        try:
            data = content[self.get_bid_pct_index(security_type)]
        except IndexError:
            self.logger.error("Not enough data: %s", content)
            return None
        data = data.replace("%", "").replace(",", ".")
        # there is no pricing for this day
        if data in ("-", "") or not re.fullmatch(r"\d+(\.\d+)?", data):
            if data not in ("-", ""):
                self.logger.warning("Unexpected price value %r in %s", data, content)
            return None
        # OCR sometimes loses the leading '1' of e.g. 100.1234
        if ocr and data.startswith("0"):
            data = "1" + data
        return round(float(data) / 100, 10)

    def sanitize_symbol(self, symbol: str):
        """
        Corrects OCR errors (only used in OCR fallback mode)
        """
        if symbol.endswith(("/1", "/|", "/!")):
            symbol = symbol[:-1] + "I"
        elif symbol.endswith("/0"):
            symbol = symbol[:-1] + "O"
        elif symbol.endswith(("/)", "/}")):
            symbol = symbol[:-1] + "J"
        elif symbol.endswith((".", ",")):
            symbol = symbol[:-1]
        elif symbol.endswith("/6"):
            symbol = symbol[:-1] + "C"
        return symbol

    def get_bid_pct_index(self, security_type):
        """
        Returns the index in the line in which the bid pct + accrued interest is
        (after dropping the standalone '%' tokens)

        DKJ:   D260930  99.9148  6.22 ...                -> [1] vételi árfolyam
        BABA:  2032/S_BABA  100.0000  105.4518 ...       -> [2] vételi bruttó
        other: 2027/A  97.4623  2.7370  100.1993 ...     -> [3] vételi bruttó
        """
        match security_type:
            case "DKJ":
                return 1
            case "BABA":
                return 2
            case _:
                return 3

    # (regex, security type) - the first match wins, the order matters
    security_type_rules = [
        (re.compile(r"^N\d{4}/\d{2}$"), "MÁPP"),           # N2026/40
        (re.compile(r"^\d{4}/M\d{1,2}$"), "MÁPP_T"),        # 2030/M10
        (re.compile(r"^N\d{4}/M\d{1,2}$"), "MÁPP_T"),       # N2029/M1
        (re.compile(r"^D\d{6}$"), "DKJ"),                    # D260930
        (re.compile(r"^K\d{4}/"), "1MÁP"),                   # K2027/...
        (re.compile(r"^\d{4}/S_BABA$"), "BABA"),             # 2032/S_BABA
        (re.compile(r"^\d{4}/U_EUR$"), "EMÁP"),              # 2029/U_EUR
        (re.compile(r"^\d{4}/[XY]_EUR$"), "PEMÁP"),          # 2026/X_EUR
        (re.compile(r"^\d{4}/[IJKL]\d?$"), "PMÁP"),         # 2027/I, 2035/I1
        (re.compile(r"^\d{4}/Q\d{1,2}$"), "FixMÁP"),        # 2027/Q1, 2030/Q5
        (re.compile(r"^\d{4}/[NOPR]\d?$"), "BMÁP"),         # 2026/O, 2028/R1
        (re.compile(r"^\d{4}/[A-H]$"), "KTV"),               # 2026/D, 2027/A, 2032/G
    ]

    def symbol_to_security_type(self, name: str) -> str:
        """
        Converts symbol to security_type. Never returns None:
        unknown symbols are logged and treated as KTV (Magyar Államkötvény).
        """
        for pattern, security_type in self.security_type_rules:
            if pattern.match(name):
                return security_type
        self.logger.warning("Unknown symbol %r, treating as KTV", name)
        return "KTV"


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
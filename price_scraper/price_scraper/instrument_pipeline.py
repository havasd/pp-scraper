from collections import defaultdict
from pathlib import Path

# useful for handling different item types with a single interface
from scrapy.exporters import CsvItemExporter

from .util import sanitize_file_name

class InstrumentExporterPipeline:
    """
    Collects all instruments found by a scraper and generates them as an importable
    instrument CSV for Portfolio Performance.
    """

    def __init__(self):
        self.csv_output = None
        self.stored_instruments = defaultdict(set)
        self.csv_file = None
        self.csv_exporter = None

    def open_spider(self, spider):
        """
        Called upon creating the spider
        """
        filename = "mak" if spider.name == "mak_historical" else spider.name
        self.csv_output = Path(spider.base_dir) / "instruments" / f"{filename}.csv"
        self.csv_output.parent.mkdir(parents=True, exist_ok=True)
        # file_name => instruments
        self.csv_file = self.csv_output.open("wb")
        self.csv_file.write(b"ticker symbol;isin;security name;currency;note;\n")
        self.csv_exporter = CsvItemExporter(
            self.csv_file,
            include_headers_line=False,
            fields_to_export=["ticker_symbol", "isin", "security_name", "currency", "note"],
            delimiter=";"
        )

    def close_spider(self, _spider):
        """
        Called upon closing the spider
        """
        self.csv_exporter.finish_exporting()
        self.csv_file.close()

    def _mark_item_as_recorded(self, item, instruments_file):
        """
        Marks that the given instrument was written to the csv
        """
        self.stored_instruments[instruments_file].add(get_instrument_id(item))

    def _item_is_recorded(self, item, instruments_file):
        """
        Checks if the given instrument was written to the csv
        """
        return get_instrument_id(item) in self.stored_instruments[instruments_file]

    def process_item(self, item, spider):
        """
        Creates an instrument from the given items which is stored in csv
        """
        item['note'] = f"JSON feed url: {feed_url(spider.name, item['file_name'])}"
        if self._item_is_recorded(item, spider.name):
            spider.logger.debug("Ignored item, it is already recorded in instruments")
            return item
        self.csv_exporter.export_item(item)
        self._mark_item_as_recorded(item, spider.name)
        return item

def feed_url(output_dir, name):
    """
    Creates a statically URL based on the given arguments
    """
    name = sanitize_file_name(name)
    return f"https://cdn.statically.io/gh/havasd/pp-data/main/{output_dir}/{name}.json"

def get_instrument_id(item):
    """
    Returns the instrument id from the given item
    """
    if 'ticker_symbol' in item.keys():
        return item['ticker_symbol']
    else:
        return item['isin']

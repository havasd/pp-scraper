from pathlib import Path
from unidecode import unidecode

# useful for handling different item types with a single interface
from scrapy.exporters import CsvItemExporter

from .util import sanitize_file_name

class InstrumentExporterPipeline:
    """
    Collects all instruments found by a scraper and generates them an importable
    instrument CSV for Portfolio Performance.
    """

    def open_spider(self, spider):
        self.csv_output = Path(spider.base_dir) / f"{spider.name}_instruments.csv"
        self.csv_output.parent.mkdir(parents=True, exist_ok=True)
        self.instruments = {}
        self._load_instruments()
        self.csv_file = self.csv_output.open("wb")
        self.csv_file.write(b"ticker symbol;isin;security name;currency;note;\n")
        self.csv_exporter = CsvItemExporter(
            self.csv_file,
            include_headers_line=False,
            fields_to_export=["ticker_symbol", "isin", "security_name", "currency", "note"],
            delimiter=";"
        )

    def close_spider(self, spider):
        self.csv_file.close()

    def _load_instruments(self):
        pass

    def process_item(self, item, spider):
        item['note'] = f"JSON feed url: {feed_url(spider.name, item['file_name'])}"
        self.csv_exporter.export_item(item)
        return item

    @staticmethod
    def file_name(name):
        """Normalizes file names"""
        return unidecode(name.lower().replace(' ', '_').replace('/', '_'))

def feed_url(output_dir, name):
    """
    Creates a statically URL based on the given arguments
    """
    name = sanitize_file_name(name)
    return f"https://cdn.statically.io/gh/havasd/pp-data/main/{output_dir}/{name}.json"

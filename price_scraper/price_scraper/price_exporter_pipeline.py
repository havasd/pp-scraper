import json
from pathlib import Path
from unidecode import unidecode
from collections import defaultdict

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import JsonItemExporter
from scrapy.exceptions import DropItem

from .util import truncate_utf8_chars

class PriceExporterPipeline:
    """
    Groups the item by portfolio and writes them into a json file.

    Writing happens in an append only mode which means we load the files
    and skip already stored data.
    """

    def open_spider(self, spider):
        self.portfolio_to_exporter = {}
        self.stored_dates = defaultdict(set)
        self.base_directory = f"{spider.base_dir}/{spider.name}"

    def close_spider(self, spider):
        for exporter, json_file in self.portfolio_to_exporter.values():
            exporter.finish_exporting()
            json_file.close()

    def _exporter_for_item(self, adapter):
        name = self.file_name(adapter["file_name"])
        if name not in self.portfolio_to_exporter:
            self._create_exporter(name)
        return self.portfolio_to_exporter[name][0]

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        exporter = self._exporter_for_item(adapter)
        if adapter["date"] in self.stored_dates[self.file_name(adapter["file_name"])]:
            raise DropItem(
                f"Date {adapter['date']} is already stored for instrument {adapter['file_name']}"
            )
        exporter.export_item(item)
        return item

    def _create_exporter(self, name):
        """
        Creates an exporter. If the file already exists we will append to it.
        Otherwise we create a new one.
        """

        file_path = Path(f"{self.base_directory}/{name}.json")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = file_path.exists()
        if file_exists:
            self._load_file(name, file_path)
            truncate_utf8_chars(file_path, 2, ignore_newlines=False)
        json_file = file_path.open("ab" if file_exists else "wb")
        exporter = JsonItemExporter(json_file, indent=True, fields_to_export=['price', 'date', 'volume', 'day_low', 'day_high'])
        if file_exists:
            exporter.first_item = False
        else:
            exporter.start_exporting()
        self.portfolio_to_exporter[name] = (exporter, json_file)

    def _load_file(self, name, file_path):
        """Stores the dates for a particular file name and returns the loaded data"""
        if name in self.stored_dates:
            return
        if not file_path.exists():
            return

        with file_path.open("rb") as f:
            data = json.load(f)
            for elem in data:
                self.stored_dates[name].add(elem["date"])

    @staticmethod
    def file_name(name):
        """Normalizes file names"""
        return unidecode(name.lower().replace(' ', '_').replace('/', '_'))

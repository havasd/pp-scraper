# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import json
from pathlib import Path
from unidecode import unidecode
from collections import defaultdict

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import JsonItemExporter
from scrapy.exceptions import DropItem

from .util import truncate_utf8_chars

class PriceScraperPipeline:
    """
    Groups the item by portfolio and writes them into a json file.

    Writing happens in an append only mode which means we load the files
    and skip already stored data.
    """

    def open_spider(self, spider):
        self.portfolio_to_exporter = {}
        self.stored_dates = defaultdict(set)

    def close_spider(self, spider):
        for exporter, json_file in self.portfolio_to_exporter.values():
            exporter.finish_exporting()
            json_file.close()

    def _exporter_for_item(self, adapter):
        name = self.file_name(adapter["name"])
        if name not in self.portfolio_to_exporter:
            self._load_file(name)
            self._create_exporter(name)
        return self.portfolio_to_exporter[name][0]

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        exporter = self._exporter_for_item(adapter)
        if adapter["date"] in self.stored_dates[self.file_name(adapter["name"])]:
            raise DropItem(
                f"Date {adapter['date']} is already stored for instrument {adapter['name']}"
            )
        exporter.export_item(item)
        return item

    def _create_exporter(self, name):
        """
        Creates an exporter. If the file already exists we will append to it.
        Otherwise we create a new one.
        """
        file_name = f"{name}.json"
        file_exists = Path(file_name).exists()
        if file_exists:
            truncate_utf8_chars(file_name, 1)
        json_file = open(file_name, "ab" if file_exists else "wb")
        exporter = JsonItemExporter(json_file)
        if file_exists:
            exporter.first_item = False
        else:
            exporter.start_exporting()
        self.portfolio_to_exporter[name] = (exporter, json_file)

    def _load_file(self, name):
        """Stores the dates for a particular file name and returns the loaded data"""
        if name in self.stored_dates:
            return
        file_name = f"{name}.json"
        if not Path(file_name).exists():
            return

        with open(file_name, "rb") as f:
            data = json.load(f)
            for elem in data:
                self.stored_dates[name].add(elem["date"])

    @staticmethod
    def file_name(name):
        """Normalizes file names"""
        return unidecode(name.lower().replace(' ', '_'))

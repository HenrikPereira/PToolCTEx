# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os

import pandas as pd


class ParquetPipeline:
    def __init__(self, output_folder):
        self.items = []
        self.output_folder = output_folder
        self.output_file = None

    @classmethod
    def from_crawler(cls, crawler):
        # Read the output folder from settings; provide a default if not set.
        output_folder = crawler.settings.get('PARQUET_OUTPUT_FOLDER', '/path/to/your/output/folder')
        return cls(output_folder)

    def process_item(self, item, spider):
        # Append each item (converted to a dict) to the list
        self.items.append(dict(item))

        return item

    def close_spider(self, spider):
        df = pd.DataFrame(self.items).convert_dtypes()

        if spider.name == 'trials':
            self.output_file = os.path.join(self.output_folder, 'trials.parquet')

        if spider.name == 'ctis_eu':
            self.output_file = os.path.join(self.output_folder, 'ctis.parquet')

        if spider.name == 'pap_infarmed':
            self.output_file = os.path.join(self.output_folder, 'pap.parquet')

        df.to_parquet(self.output_file, index=False) if self.output_file is not None else None


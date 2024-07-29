# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PortfolioPerformanceHistoricalPrice(scrapy.Item):
    """
    Contains data for portfolio performance historical price data source
    """

    # name of the file to export the price data to
    file_name = scrapy.Field()
    # long name of the instrument
    name = scrapy.Field()
    # date of the price
    date = scrapy.Field()
    # last price
    price = scrapy.Field()
    # volume over the day
    volume = scrapy.Field()
    # lowest price during the day
    day_low = scrapy.Field()
    # highest price during the day
    day_high = scrapy.Field()
    # Currency of the fund
    currency = scrapy.Field()
    # Start date of the fund
    start_date = scrapy.Field()
    # ISIN if available
    isin = scrapy.Field()
    # symbol if available
    symbol = scrapy.Field()

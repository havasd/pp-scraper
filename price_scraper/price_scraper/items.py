# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PortfolioPerformanceHistoricalPrice(scrapy.Item):
    """
    Contains data for portfolio performance historical price data source
    """
    
    # name of the instrument can be ISIN or explicit name depending on the scraped website
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
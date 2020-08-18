import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from time import time
from pprint import pprint
import os
import re
from ORM import Operations
from datetime import date, datetime
import re
import json

class RootSpider(scrapy.Spider):
  name = "root"
  search_url = 'https://www.zillow.com/homes/{}_rb/'
  root_url = 'https://www.zillow.com/homes{}'
  listings = []
  errors = []

  def save_errors(self):
    for error in self.errors:
      Operations.SaveError(error)

  def save_listings(self):
    for listing in self.listings:
      Operations.SaveListing(listing)

    self.listings = []


  def start_requests(self):
    start_urls = [self.search_url.format(zip.Value) for zip in Operations.QueryZIP()]
    for url in start_urls[0:10]:
      yield scrapy.Request(url=url,
        callback=self.parse_urls,
        errback=self.errbacktest,
        meta={'root': url})

  def parse_urls(self, response):
    if response.status != 200:
      print(response.text)
      return

    urls = response.xpath("//a[@class='list-card-link']/@href").extract()

    if len(self.errors) < 10:
      for url in urls:
        yield scrapy.Request(url,
          callback=self.parse_listing,
          errback=self.errbacktest)

      # if there is a next page, scrape it
      next_page_enabled = response.xpath("//a[@rel='next']/@disabled").extract_first() == None
      if next_page_enabled:
        url = response.xpath("//a[@rel='next']/@href").extract_first()
        yield response.follow(url,
          callback=self.parse_urls,
          errback=self.errbacktest,
          meta={'root': response.meta.get('root')})


  def parse_listing(self, response):
    if response.status != 200:
      print(response.text)
      return

    try:
      self.get_fields(response)

    except Exception as e:
      error = {}
      error['url'] = response.url
      error['error'] = str(e)

      self.errors.append(error)

  def errbacktest(self, failiure):
    pass

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    return spider

  def spider_closed(self, spider):
    self.save_listings()
    self.save_errors()

  def get_fields(self, response):
    print(len(self.listings))
    result = {}

    result['_id'] = re.search(r'(.*?)_zpid', response.url).group(1).split('/')[-1]
    result['scrape_date'] = date.today()
    result['home_address'] = response.xpath("//h1[@class='ds-address-container']/span/text()").extract_first()
    result['zillow_url'] = response.url
    result['listed_price'] = response.xpath("//span[@class='ds-value']/text()").extract_first('').replace('$', '').replace(',','')

    result['z_estimate'] = response.xpath("//span[contains(text(), 'Zestimate')]/sup[contains(text(), '®')]")[0].xpath("../../../span/text()")[0].extract().replace("$",'').replace(',','')
    result['time_on_zillow'] = response.xpath("//div[contains(text(), 'Time on Zillow')]/following-sibling::*[1]/text()").extract_first().replace(' days', '')
    result['views'] = response.xpath("//button[contains(text(), 'Views')]/../following-sibling::*[1]/text()").extract_first(0).replace(',','')
    result['saves'] = response.xpath("//button[contains(text(), 'Saves')]/../following-sibling::*[1]/text()").extract_first(0)

    result['_type'] = response.xpath("//span[contains(text(), 'Type:')]/following-sibling::*[1]/text()").extract_first()

    result['year_build'] = response.xpath("//span[contains(text(), 'Year built:')]/following-sibling::*[1]/text()").extract_first()
    result['heating'] = response.xpath("//span[contains(text(), 'Heating:')]/following-sibling::*[1]/text()").extract_first()
    result['cooling'] = response.xpath("//span[contains(text(), 'Cooling:')]/following-sibling::*[1]/text()").extract_first()

    result['parking'] = response.xpath("//span[contains(text(), 'Parking:')]/following-sibling::*[1]/text()").extract_first().split(' ')[0]

    result['lot'] = response.xpath("//span[contains(text(), 'Lot:')]/following-sibling::*[1]/text()").extract_first('0').replace(' sqft', '').replace(',','')


    result['price_per_sqft'] = response.xpath("//span[contains(text(), 'Price/sqft')]/following-sibling::*[1]/text()").extract_first('0').replace('$', '').replace(',','')

    result['bedrooms'] = response.xpath("//span[contains(text(), 'Bedrooms:')]/text()[2]").extract_first(None)
    result['bathrooms'] = response.xpath("//span[contains(text(), 'Bathrooms:')]/text()[2]").extract_first(None)
    result['full_bathrooms'] = response.xpath("//span[contains(text(), 'Full bathrooms:')]/text()[2]").extract_first(None)
    result['3_4_bathrooms'] = response.xpath("//span[contains(text(), '3/4 bathrooms:')]/text()[2]").extract_first(None)
    result['1_2_bathrooms'] = response.xpath("//span[contains(text(), '1/2 bathrooms:')]/text()[2]").extract_first(None)
    result['1_4_bathrooms'] = response.xpath("//span[contains(text(), '1/4 bathrooms:')]/text()[2]").extract_first(None)

    result['flooring'] = response.xpath("//span[contains(text(), 'Flooring:')]/text()[2]").extract_first()
    result['heating'] = response.xpath("//span[contains(text(), 'Heating features:')]/text()[2]").extract_first()
    result['cooling'] = response.xpath("//span[contains(text(), 'Cooling features:')]/text()[2]").extract_first()
    result['appliances'] = ''.join(response.xpath("//span[contains(text(), 'Appliances')]/../ul/li/span/text()").extract())
    result['fireplace'] = response.xpath("//span[contains(text(), 'Fireplace:')]/text()[2]").extract_first() == 'Yes'
    result['parking_features'] = response.xpath("//span[contains(text(), 'Parking features:')]/text()[2]").extract_first()
    result['garage_spaces'] = response.xpath("//span[contains(text(), 'Garage spaces:')]/text()[2]").extract_first(None)
    result['spa'] = response.xpath("//span[contains(text(), 'Spa included:')]/text()[2]").extract_first() == 'Yes'
    result['view_description'] = response.xpath("//span[contains(text(), 'View description:')]/text()[2]").extract_first()
    result['on_waterfront'] = response.xpath("//span[contains(text(), 'On waterfront:')]/text()[2]").extract_first() == 'Yes'
    result['lot_size'] = response.xpath("//span[contains(text(), 'Lot size:')]/text()[2]").extract_first()
    result['home_type'] = response.xpath("//span[contains(text(), 'Home type:')]/text()[2]").extract_first()

    result['roof'] = response.xpath("//span[contains(text(), 'Roof:')]/text()[2]").extract_first()
    result['new_construction'] = response.xpath("//span[contains(text(), 'New construction:')]/text()[2]").extract_first() == 'Yes'
    result['year_build'] = response.xpath("//span[contains(text(), 'Year built:')]/text()[2]").extract_first()

    # schools
    schools = response.xpath("//ul[@class='ds-nearby-schools-list']/li")
    school_rates = [school.xpath(".//div/div/span/text()").extract_first() for school in schools]
    result['great_schools_rating'] = ','.join(school_rates)

    result['neighborhood'] = response.xpath("//span[@id='skip-link-neighborhood']/following-sibling::div/h4/text()").extract_first().replace("Neighborhood: ", '')

    # price  history
    price_history_string = re.search(r'priceHistory\\\":(.*?)}]', response.text).group(1)
    price_history_string = price_history_string.replace("\\", '')
    price_history_string += '}]'
    price_history = json.loads(price_history_string)
    last_sale_price, last_sale_sell_date = next(((item['price'], item['time']) for item in price_history if 'event' in item and item['event'] == 'Sold'), (None, None))
    result['last_sale_price'] = last_sale_price
    result['last_sale_sell_date'] = datetime.fromtimestamp(last_sale_sell_date/1000) if last_sale_sell_date != None else None

    self.listings.append(result)
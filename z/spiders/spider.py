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
    print("Saving errors")
    for error in self.errors:
      Operations.SaveError(error)

  def save_listings(self):
    print("Saving listings")
    listings_to_save = []
    while len(self.listings) > 0:
      listings_to_save.append(self.listings.pop())

    for listing in listings_to_save:
      Operations.SaveListing(listing)


  def start_requests(self):
    zip_codes = Operations.QueryZIP()
    for _zip in zip_codes[0:10]:
      yield scrapy.Request(url=self.search_url.format(_zip.Value),
        callback=self.parse_urls,
        errback=self.errbacktest,
        meta={'zip': _zip.Value})

  def parse_urls(self, response):
    if response.status != 200:
      print(response.text)
      return

    urls = response.xpath("//a[@class='list-card-link']/@href").extract()

    for url in urls:
      yield scrapy.Request(url,
        callback=self.parse_listing,
        errback=self.errbacktest,
        meta={'zip': response.meta.get('zip')})

    # if there is a next page, scrape it
    next_page_enabled = response.xpath("//a[@rel='next']/@disabled").extract_first() == None
    if next_page_enabled:
      url = response.xpath("//a[@rel='next']/@href").extract_first()
      yield response.follow(url,
        callback=self.parse_urls,
        errback=self.errbacktest,
        meta={'zip': response.meta.get('zip')})

  def parse_listing(self, response):

    if len(self.listings) > 10:
      self.save_listings()

    try:
      if response.status != 200:
        raise Exception(response.status)

      self.get_fields(response)

    except Exception as e:
      error = {}
      error['url'] = response.url
      error['error'] = str(e)

      self.errors.append(error)

  def errbacktest(self, failiure):
    error = {}
    error['url'] = response.url
    error['error'] = str(failiure)

    self.errors.append(error)

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    return spider

  def spider_closed(self, spider):
    self.save_listings()
    self.save_errors()

  def get_fields(self, response):
    result = {}

    result['_id'] = re.search(r'(.*?)_zpid', response.url).group(1).split('/')[-1]
    result['scrape_date'] = date.today()
    result['home_address'] = response.xpath("//h1[@class='ds-address-container']/span/text()").extract_first()
    result['zip'] = response.meta.get('zip')
    result['zillow_url'] = response.url
    result['listed_price'] = response.xpath("//span[@class='ds-value']/text()").extract_first('').replace('$', '').replace(',','')
    result['square_feet'] = response.xpath("//span[@class='ds-bed-bath-living-area']")[-1].xpath(".//span/text()").extract_first(0).replace(',','')

    try:
      result['z_estimate'] = response.xpath("//span[contains(text(), 'Zestimate')]/sup[contains(text(), '®')]")[0].xpath("../../../span/text()")[0].extract().replace("$",'').replace(',','')

    except Exception as e:

      try:
        data = re.search(r'VariantQuery(.*?)</script><script', response.text).group(1)
        data = data.replace("\\", '')
        result['z_estimate'] = re.search(r'zestimate\":(.*?),', data).group(1)

      except Exception as e:
        result['z_estimate'] = ''

    result['time_on_zillow'] = response.xpath("//div[contains(text(), 'Time on Zillow')]/following-sibling::*[1]/text()").extract_first().replace(' days', '')
    result['views'] = response.xpath("//button[contains(text(), 'Views')]/../following-sibling::*[1]/text()").extract_first(0).replace(',','')
    result['saves'] = response.xpath("//button[contains(text(), 'Saves')]/../following-sibling::*[1]/text()").extract_first(0)

    result['_type'] = response.xpath("//span[contains(text(), 'Type:')]/following-sibling::*[1]/text()").extract_first()

    result['year_build'] = response.xpath("//span[contains(text(), 'Year built:')]/following-sibling::*[1]/text()").extract_first()
    result['heating'] = response.xpath("//span[contains(text(), 'Heating:')]/following-sibling::*[1]/text()").extract_first()
    result['cooling'] = response.xpath("//span[contains(text(), 'Cooling:')]/following-sibling::*[1]/text()").extract_first()

    try:
      result['parking'] = response.xpath("//span[contains(text(), 'Parking:')]/following-sibling::*[1]/text()").extract_first().split(' ')[0]
    except Exception as e:
      result['parking'] = None

    result['lot'] = response.xpath("//span[contains(text(), 'Lot:')]/following-sibling::*[1]/text()").extract_first('0').replace(' sqft', '').replace(',','')


    result['price_per_sqft'] = response.xpath("//span[contains(text(), 'Price/sqft')]/following-sibling::*[1]/text()").extract_first('0').replace('$', '').replace(',','')

    result['bedrooms'] = response.xpath("//span[contains(text(), 'Bedrooms:')]/text()[2]").extract_first(None)
    result['bathrooms'] = response.xpath("//span[contains(text(), 'Bathrooms:')]/text()[2]").extract_first(None)
    result['full_bathrooms'] = response.xpath("//span[contains(text(), 'Full bathrooms:')]/text()[2]").extract_first(None)
    result['3_4_bathrooms'] = response.xpath("//span[contains(text(), '3/4 bathrooms:')]/text()[2]").extract_first(None)
    result['1_2_bathrooms'] = response.xpath("//span[contains(text(), '1/2 bathrooms:')]/text()[2]").extract_first(None)
    result['1_4_bathrooms'] = response.xpath("//span[contains(text(), '1/4 bathrooms:')]/text()[2]").extract_first(None)

    result['flooring'] = response.xpath("//span[contains(text(), 'Flooring:')]/text()[2]").extract_first()
    result['heating_features'] = response.xpath("//span[contains(text(), 'Heating features:')]/text()[2]").extract_first()
    result['cooling_features'] = response.xpath("//span[contains(text(), 'Cooling features:')]/text()[2]").extract_first()
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

    if price_history_string.startswith('[]'):
      result['last_sale_price'] = None
      result['last_sale_sell_date'] = None

    else:
      price_history = json.loads(price_history_string)
      last_sale_price, last_sale_sell_date = next(((item['price'], item['time']) for item in price_history if 'event' in item and item['event'] == 'Sold'), (None, None))
      result['last_sale_price'] = last_sale_price
      result['last_sale_sell_date'] = datetime.fromtimestamp(last_sale_sell_date/1000) if last_sale_sell_date != None else None

    result['agent'] = response.xpath("//span[@class='cf-listing-agent-display-name']/text()").extract_first('')

    self.listings.append(result)



import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from time import time
from pprint import pprint
import os
import re
from datetime import date, datetime
import re
import json
from ORM import Operations

class RootSpider(scrapy.Spider):
  name = "root"
  search_url = 'https://www.zillow.com/homes/{}_rb/{}_p/'
  rent_url = 'https://www.zillow.com/homes/{}_rb/?searchQueryState=%7B%22pagination%22%3A%7B%22currentPage%22%3A{}%7D%2C%22usersSearchTerm%22%3A%2294107%22%2C%22mapBounds%22%3A%7B%22west%22%3A-122.61161373974612%2C%22east%22%3A-122.2099261176758%2C%22south%22%3A37.59411242334494%2C%22north%22%3A37.81521727170439%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A97562%2C%22regionType%22%3A7%7D%5D%2C%22isMapVisible%22%3Atrue%2C%22mapZoom%22%3A12%2C%22filterState%22%3A%7B%22pmf%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22pf%22%3A%7B%22value%22%3Afalse%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%7D%2C%22isListVisible%22%3Atrue%7D'
  listings = []
  errors = []

  def create_error(self, response, error):
    error = {}
    error['url'] = response.url
    error['error'] = str(error)

    self.errors.append(error)

  def save_errors(self):
    print("Saving errors")
    while len(self.errors) > 0:
      Operations.SaveError(self.errors.pop())


  def save_listings(self):
    print("Saving listings")
    listings_to_save = []
    while len(self.listings) > 0:
      listings_to_save.append(self.listings.pop())

    for listing in listings_to_save:
      Operations.SaveListing(listing)


  def start_requests(self):

    self.scrape_type = getattr(self,'scrape_type', 0)
    zip_codes = Operations.QueryZIP()

    if self.scrape_type ==  0:
      zip_codes = [x for x in zip_codes if x.Value == '94107']
      self.search_url = self.rent_url

    for _zip in zip_codes:

      yield scrapy.Request(url=self.search_url.format(_zip.Value, 1),
        callback=self.parse_urls,
        errback=self.errbacktest,
        meta={'zip': _zip.Value, 'page': 1})


  def parse_urls(self, response):
    if response.status != 200:
      print(response.text)
      return

    urls = response.xpath("//a[@class='list-card-link']/@href").extract()

    # Some listings from the list view have malformed URLS we find those that are okai
    for url in [x for x in urls if '_zpid' in x]:
      if 'https://www.zillow.com' not in url:
        url = 'https://www.zillow.com' + url

      yield scrapy.Request(url,
        callback=self.parse_listing,
        errback=self.errbacktest,
        meta={'zip': response.meta.get('zip')})

    # Some listings from the list view have malformed URLS we find those that are bad
    # and request them and send them to get_better_url method
    for url in [x for x in urls if '_zpid' not in x]:
      if 'https://www.zillow.com' not in url:
        url = 'https://www.zillow.com' + url

      yield scrapy.Request(url,
        callback=self.get_better_url,
        errback=self.errbacktest,
        meta={'zip': response.meta.get('zip')})

    # if there is a next page, scrape it
    next_page_enabled = response.xpath("//a[@rel='next']/@disabled").extract_first() == None
    if next_page_enabled:
      url = response.xpath("//a[@rel='next']/@href").extract_first()

      if url == None: return

      yield scrapy.Request(self.search_url.format(response.meta.get('zip'), response.meta.get('page') + 1),
        callback=self.parse_urls,
        errback=self.errbacktest,
        meta={'zip': response.meta.get('zip'), 'page': response.meta.get('page') + 1})

  # This is to find a correct zillow URL from malformed URLS
  def get_better_url(self, response):

    try:
      new_url_group = re.search(r'bestMatchedUnit":(.*?),"carouselPhotos', response.text).group(1)
      new_url = json.loads(new_url_group)['hdpUrl']
      url = 'https://www.zillow.com' + new_url
      yield scrapy.Request(url,
        callback=self.parse_listing,
        errback=self.errbacktest,
        meta={'zip': response.meta.get('zip')})

    except Exception as e:
      self.create_error(response, e)


  def parse_listing(self, response):
    if len(self.listings) > 0 and len(self.listings)%10 == 0:
      self.save_listings()

    if len(self.errors) > 0 and len(self.errors)%10 == 0:
      print(len(self.errors))
      self.save_errors()

    try:
      if response.status != 200:
        raise Exception(response.status)

      self.get_fields(response)

    except Exception as e:
      self.create_error(response, e)

  def errbacktest(self, failiure):
    self.create_error(failure.value.response, failiure.value)

    error = {}
    error['url'] = str(failiure)
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
    result['rent'] = True if self.scrape_type == 0 else False

    result['scrape_date'] = date.today()
    result['home_address'] = response.xpath("//h1[@class='ds-address-container']/span/text()").extract_first()
    result['zip'] = response.meta.get('zip')
    result['zillow_url'] = response.url
    result['listed_price'] = response.xpath("//span[@class='ds-value']/text()").extract_first('').replace('$', '').replace(',','')

    try:
      result['square_feet'] = response.xpath("//span[@class='ds-bed-bath-living-area']")[-1].xpath(".//span/text()").extract_first(0).replace(',','')

    except Exception as e:
      result['square_feet'] = 0


    try:
      result['z_estimate'] = response.xpath("//span[contains(text(), 'Zestimate')]/sup[contains(text(), '®')]")[0].xpath("../../../span/text()")[0].extract().replace("$",'').replace(',','')

    except Exception as e:

      try:
        data = re.search(r'VariantQuery(.*?)</script><script', response.text).group(1)
        data = data.replace("\\", '')
        result['z_estimate'] = re.search(r'zestimate\":(.*?),', data).group(1)

      except Exception as e:
        result['z_estimate'] = ''

    try:
      result['time_on_zillow'] = response.xpath("//div[contains(text(), 'Time on Zillow')]/following-sibling::*[1]/text()").extract_first().replace(' days', '')

    except Exception as e:
      result['time_on_zillow'] = 0

    try:
      result['views'] = response.xpath("//button[contains(text(), 'Views')]/../following-sibling::*[1]/text()").extract_first(0).replace(',','')

    except Exception as e:
      result['views'] = 0

    result['saves'] = response.xpath("//button[contains(text(), 'Saves')]/../following-sibling::*[1]/text()").extract_first(0)

    result['_type'] = response.xpath("//span[contains(text(), 'Type:')]/following-sibling::*[1]/text()").extract_first()

    result['year_build'] = response.xpath("//span[contains(text(), 'Year built:')]/following-sibling::*[1]/text()").extract_first()
    result['heating'] = response.xpath("//span[contains(text(), 'Heating:')]/following-sibling::*[1]/text()").extract_first()
    result['cooling'] = response.xpath("//span[contains(text(), 'Cooling:')]/following-sibling::*[1]/text()").extract_first()

    try:
      result['parking'] = response.xpath("//span[contains(text(), 'Parking:')]/following-sibling::*[1]/text()").extract_first().split(' ')[0]
    except Exception as e:
      result['parking'] = None

    try:
      result['lot'] = response.xpath("//span[contains(text(), 'Lot:')]/following-sibling::*[1]/text()").extract_first('0').replace(' sqft', '').replace(',','')

    except Exception as e:
      result['lot'] = 0


    try:
      result['price_per_sqft'] = response.xpath("//span[contains(text(), 'Price/sqft')]/following-sibling::*[1]/text()").extract_first('0').replace('$', '').replace(',','')

    except Exception as e:
      result['price_per_sqft'] = 0

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
    try:
      price_history_string = re.search(r'priceHistory\\\":(.*?)}]', response.text).group(1)
      price_history_string = price_history_string.replace("\\", '')
      price_history_string += '}]'

    except Exception as e:
      price_history_string = '[]'

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



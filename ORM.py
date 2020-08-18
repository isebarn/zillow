import os
import json
from datetime import datetime
from sqlalchemy import ForeignKey, desc, create_engine, func, Column, BigInteger, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pprint import pprint

engine = create_engine(os.environ.get('ZILLOW_DATABASE'), echo=False)
Base = declarative_base()

def read_file(filename):
  file = open(filename, "r")
  return file.read().splitlines()

def safe_int_cast(value, tracer, listing):
  try:
    if isinstance(value, str):
      return int(value) if value != 'No' and value != None and value.isdigit() else None

  except Exception as e:
    print("{}: {}: {}: {}".format(value, tracer, listing, e))
    return None

class ZIP(Base):
  __tablename__ = 'zip'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String, unique=True)

  def __init__(self, zip_code):
    self.Value = zip_code

class Listing(Base):
  __tablename__ = 'listing'

  Id = Column('id', Integer, primary_key=True)

  Bathrooms = Column('bathrooms', Integer)
  Bedrooms = Column('bedrooms', Integer)
  FullBathrooms = Column('full_bathrooms', Integer)
  HalfBathrooms = Column('1_2_bathrooms', Integer)
  QuarterBathrooms = Column('1_4_bathrooms', Integer)
  ThreeQuarterBathrooms = Column('3_4_bathrooms', Integer)

  GarageSpaces = Column('garage_spaces', Integer)
  LastSalePrice = Column('last_sale_price', Integer)
  ListedPrice = Column('listed_price', Integer)
  PricePerSqft = Column('price_per_sqft', Integer)
  Saves = Column('saves', Integer)
  TimeOnZillow = Column('time_on_zillow', Integer)
  Views = Column('views', Integer)
  YearBuild = Column('year_build', Integer)
  ZEstimate = Column('z_estimate', Integer)

  Appliances = Column('appliances', String)
  Cooling = Column('cooling', String)
  Cooling = Column('cooling', String)
  Fireplace = Column('fireplace', Boolean)
  Flooring = Column('flooring', String)
  GreatSchoolsRating = Column('great_schools_rating', String)
  Heating = Column('heating', String)
  Heating = Column('heating', String)
  HomeAddress = Column('home_address', String)
  HomeType = Column('home_type', String)
  LastSaleSellDate = Column('last_sale_sell_date', DateTime)
  Lot = Column('lot', String)
  LotSize = Column('lot_size', String)
  Neighborhood = Column('neighborhood', String)
  Parking = Column('parking', String)
  ParkingFeatures = Column('parking_features', String)
  PropertyType = Column('property_type', String)
  Roof = Column('roof', String)
  ScrapeDate = Column('scrape_date', DateTime)
  ViewDescription = Column('view_description', String)
  ZillowUrl = Column('zillow_url', String)

  NewConstruction = Column('new_construction', Boolean)
  OnWaterfront = Column('on_waterfront', Boolean)
  Spa = Column('spa', Boolean)


  def __init__(self, data):
    Id = data['_id']
    Appliances = data['appliances']
    Cooling = data['cooling']
    Cooling = data['cooling']
    Fireplace = data['fireplace']
    Flooring = data['flooring']
    GreatSchoolsRating = data['great_schools_rating']
    Heating = data['heating']
    Heating = data['heating']
    HomeAddress = data['home_address']
    HomeType = data['home_type']
    LastSaleSellDate = data['last_sale_sell_date']
    LotSize = data['lot_size']
    Neighborhood = data['neighborhood']
    ParkingFeatures = data['parking_features']
    PropertyType = data['_type']
    Roof = data['roof']
    ScrapeDate = data['scrape_date']
    ViewDescription = data['view_description']
    ZillowUrl = data['zillow_url']

    NewConstruction = data['new_construction']
    OnWaterfront = data['on_waterfront']
    Spa = data['spa']

    Bathrooms = safe_int_cast(data['bathrooms'], 'bathrooms', self.Id)
    Bedrooms = safe_int_cast(data['bedrooms'], 'bedrooms', self.Id)
    FullBathrooms = safe_int_cast(data['full_bathrooms'], 'full_bathrooms', self.Id)
    HalfBathrooms = safe_int_cast(data['1_2_bathrooms'], '1_2_bathrooms', self.Id)
    QuarterBathrooms = safe_int_cast(data['1_4_bathrooms'], '1_4_bathrooms', self.Id)
    ThreeQuarterBathrooms = safe_int_cast(data['3_4_bathrooms'], '3_4_bathrooms', self.Id)

    GarageSpaces = safe_int_cast(data['garage_spaces'], 'garage_spaces', self.Id)
    LastSalePrice = data['last_sale_price']
    ListedPrice = safe_int_cast(data['listed_price'], 'listed_price', self.Id)
    Lot = safe_int_cast(data['lot'], 'lot', self.Id)
    Parking = safe_int_cast(data['parking'], 'parking', self.Id)
    PricePerSqft = safe_int_cast(data['price_per_sqft'], 'price_per_sqft', self.Id)
    Saves = safe_int_cast(data['saves'], 'saves', self.Id)
    TimeOnZillow = safe_int_cast(data['time_on_zillow'], 'time_on_zillow', self.Id)
    Views = safe_int_cast(data['views'], 'views', self.Id)
    YearBuild = safe_int_cast(data['year_build'], 'year_build', self.Id)
    ZEstimate = safe_int_cast(data['z_estimate'], 'z_estimate', self.Id)


Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

class Operations:

  def SaveZIP(data):
    session.add(data)
    session.commit()

  def QueryZIP():
    return session.query(ZIP).all()

  def SaveListing(data):
    if session.query(Listing.Id).filter_by(Id=data['_id']).scalar() == None:
      session.add(Listing(data))
      session.commit()


  def init_db():
    zip_codes = [ZIP(zip_code) for zip_code in read_file('zipcodes.txt')]
    for zip_code in zip_codes:
      Operations.SaveZIP(zip_code)


if __name__ == "__main__":
  pass

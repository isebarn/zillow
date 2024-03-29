import os
import json
from datetime import datetime
from sqlalchemy import ForeignKey, desc, create_engine, func, Column, BigInteger, Integer, Float, String, Boolean, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pprint import pprint

engine = create_engine('postgresql://zillow:zillow123@192.168.1.35:5433/zillow', echo=False)
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

class Error(Base):
  __tablename__ = 'error'

  Id = Column('id', Integer, primary_key=True)
  Error = Column('error', JSON)

  def __init__(self, data):
    self.Error = data

class ZIP(Base):
  __tablename__ = 'zip'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String, unique=True)
  RunKey = Column('run_key', Integer)


  def __init__(self, zip_code):
    self.Value = zip_code

class Run(Base):
  __tablename__ = 'run'

  Id = Column('id', Integer, primary_key=True)
  Run = Column('run', Integer)
  Seconds = Column('seconds', Integer)

  def __init__(self, run_key):
    self.Run = run_key

  def json(self):
    data = json_object(self)
    return data

class Listing(Base):
  __tablename__ = 'listing'

  Id = Column('id', Integer, primary_key=True)

  Bathrooms = Column('bathrooms', Integer)
  Bedrooms = Column('bedrooms', Integer)
  FullBathrooms = Column('full_bathrooms', Integer)
  HalfBathrooms = Column('1_2_bathrooms', Integer)
  QuarterBathrooms = Column('1_4_bathrooms', Integer)
  ThreeQuarterBathrooms = Column('3_4_bathrooms', Integer)
  ZIP = Column('zip', Integer)

  GarageSpaces = Column('garage_spaces', Integer)
  LastSalePrice = Column('last_sale_price', Integer)
  ListedPrice = Column('listed_price', Integer)
  PricePerSqft = Column('price_per_sqft', Integer)
  Saves = Column('saves', Integer)
  SquareFeet = Column('square_feet', Integer)
  TimeOnZillow = Column('time_on_zillow', Integer)
  Views = Column('views', Integer)
  YearBuild = Column('year_build', Integer)
  ZEstimate = Column('z_estimate', Integer)

  Agent = Column('agent', String)
  Appliances = Column('appliances', String)
  Cooling = Column('cooling', String)
  CoolingFeatures = Column('cooling_features', String)
  Fireplace = Column('fireplace', Boolean)
  Flooring = Column('flooring', String)
  GreatSchoolsRating = Column('great_schools_rating', String)
  Heating = Column('heating', String)
  HeatingFeatures = Column('heating_features', String)
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
  Rent = Column('rent', Boolean)
  Run = Column('run', Integer, ForeignKey('run.id'))


  def __init__(self, data):
    self.Id = data['_id']
    self.Agent = data['agent']
    self.Appliances = data['appliances']
    self.Cooling = data['cooling']
    self.CoolingFeatures = data['cooling_features']
    self.Fireplace = data['fireplace']
    self.Flooring = data['flooring']
    self.GreatSchoolsRating = data['great_schools_rating']
    self.Heating = data['heating']
    self.HeatingFeatures = data['heating_features']
    self.HomeAddress = data['home_address']
    self.HomeType = data['home_type']
    self.LastSaleSellDate = data['last_sale_sell_date']
    self.LotSize = data['lot_size']
    self.Neighborhood = data['neighborhood']
    self.ParkingFeatures = data['parking_features']
    self.PropertyType = data['_type']
    self.Roof = data['roof']
    self.ScrapeDate = data['scrape_date']
    self.ViewDescription = data['view_description']
    self.ZillowUrl = data['zillow_url']

    self.NewConstruction = data['new_construction']
    self.OnWaterfront = data['on_waterfront']
    self.Spa = data['spa']

    self.Bathrooms = safe_int_cast(data['bathrooms'], 'bathrooms', self.Id)
    self.Bedrooms = safe_int_cast(data['bedrooms'], 'bedrooms', self.Id)
    self.FullBathrooms = safe_int_cast(data['full_bathrooms'], 'full_bathrooms', self.Id)
    self.HalfBathrooms = safe_int_cast(data['1_2_bathrooms'], '1_2_bathrooms', self.Id)
    self.QuarterBathrooms = safe_int_cast(data['1_4_bathrooms'], '1_4_bathrooms', self.Id)
    self.ThreeQuarterBathrooms = safe_int_cast(data['3_4_bathrooms'], '3_4_bathrooms', self.Id)
    self.ZIP = safe_int_cast(data['zip'], 'zip', self.ZIP)

    self.GarageSpaces = safe_int_cast(data['garage_spaces'], 'garage_spaces', self.Id)
    self.LastSalePrice = data['last_sale_price']
    self.ListedPrice = safe_int_cast(data['listed_price'], 'listed_price', self.Id)
    self.Lot = safe_int_cast(data['lot'], 'lot', self.Id)
    self.Parking = safe_int_cast(data['parking'], 'parking', self.Id)
    self.PricePerSqft = safe_int_cast(data['price_per_sqft'], 'price_per_sqft', self.Id)
    self.Saves = safe_int_cast(data['saves'], 'saves', self.Id)
    self.SquareFeet = safe_int_cast(data['square_feet'], 'square_feet', self.Id)
    self.TimeOnZillow = safe_int_cast(data['time_on_zillow'], 'time_on_zillow', self.Id)
    self.Views = safe_int_cast(data['views'], 'views', self.Id)
    self.YearBuild = safe_int_cast(data['year_build'], 'year_build', self.Id)
    self.ZEstimate = safe_int_cast(data['z_estimate'], 'z_estimate', self.Id)

    self.Rent = data['rent']
    self.Run = data['run']


Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

class Operations:

  def SaveRun():
    max_run_key = session.query(ZIP).order_by(desc(ZIP.RunKey)).limit(1).one().RunKey
    last_run_key = session.query(Run).order_by(desc(Run.Id)).limit(1).one().Run

    new_run_key = last_run_key + 1

    if max_run_key < new_run_key:
      new_run_key = 1


    run = Run(new_run_key)
    session.add(run)
    session.commit()
    return run

  def CommitAll():
    session.commit()

  def SaveZIP(data):
    session.add(data)
    session.commit()

  def QueryZIP():
    return session.query(ZIP).all()

  def SaveListing(data):
    if session.query(Listing.Id).filter_by(Id=data['_id']).scalar() != None:
      session.query(Listing).filter_by(Id=data['_id']).delete()
      session.commit()

    session.add(Listing(data))
    session.commit()

  def SaveError(data):
    session.add(Error(data))
    session.commit()

  def init_db():
    zip_codes = [ZIP(zip_code) for zip_code in read_file('zipcodes.txt')]
    for zip_code in zip_codes:
      Operations.SaveZIP(zip_code)


if __name__ == '__main__':
  #Operations.init_db()
  max_run_key = session.query(ZIP).order_by(desc(ZIP.RunKey)).limit(1).one().RunKey
  last_run_key = session.query(Run).order_by(desc(Run.Id)).limit(1).one().Run
  print(max_run_key)
  print(last_run_key)
  new_run_key = last_run_key + 1

  if max_run_key < new_run_key:
    new_run_key = 1
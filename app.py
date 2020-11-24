# Import required modules
import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################

# Create engine
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect database
Base = automap_base()
Base.prepare(engine, reflect=True)

# View all of the classes that automap found
print(Base.classes.keys())

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################

#Flask setup
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    # List all available routes on Home Page
    return (
        f"<h1>Welcome to my 'Home' page!</h1>"
        f"<h3>Available routes</h3>"
        f"/api/v1.0/precipitation ====> Perceipitation Data<br/>"
        f"/api/v1.0/stations ====> List of Stations<br/>"
        f"/api/v1.0/tobs ====> temperature observations (TOBS) for the previous year<br/>"
        f"/api/v1.0/yyyy-mm-dd ====> Min, Max & Avg temperature for given start date<br/>"
        f"/api/v1.0/yyyy-mm-dd/yyyy-mm-dd ====> Min, Max & Avg temperature for given start and end date range<br/>"
    )


@app.route("/api/v1.0/precipitation")
def perc_data():
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Get the earliest & last date
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    print(f"Last data point in Measurement is for date : {last_date[0]}")

    # Calculate the date 1 year ago from the last data point in the database
    date_year_back = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    print("-"*80)
    print(f"Date 1 year ago from last data point in Measurement is : {date_year_back}")

    # Perform a query to retrieve the data and precipitation scores 
    sel = [Measurement.date, Measurement.prcp]
    prcp_data = session.query(*sel).filter(Measurement.date >= date_year_back).all()

    session.close()

    prcp_dta_list = []
    for date, percp in prcp_data:
        prcp_data_dict = {}
        prcp_data_dict["Date"] = date
        prcp_data_dict["Percipitation"] = percp
        prcp_dta_list.append(prcp_data_dict)
    
    return jsonify(prcp_dta_list)

@app.route("/api/v1.0/stations")
def station_list():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    sel = [Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation]
    
    results = session.query(*sel).all()

    session.close()

    station_list = []
    for station, name, lat, lng, elevation in results:
        station_dict = {}
        station_dict["Station"] = station
        station_dict["Name"] = name
        station_dict["Latitude"] = lat
        station_dict["Longitude"] = lng
        station_dict["Elevation"] = elevation
        station_list.append(station_dict)

    return jsonify(station_list)


#run flask server
if __name__ == '__main__':
    app.run(debug=True)
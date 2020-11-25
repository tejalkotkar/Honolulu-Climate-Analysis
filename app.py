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
# Methods 
#################################################
"""
Method to get earliest & last dates 
"""
def get_early_last_date():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    #dates = []
    earliest_date = session.query(Measurement.date).order_by(Measurement.date).first()
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    return(earliest_date[0], last_date[0])

"""
Method to validate if input value is valid date string
"""
def validate(date_text):
    flag = 0
    try:
        dt.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        flag = 1
    finally:
        return flag

"""
Below method is created to handle sessions & run query.
Calling this method, will create a session , run query for appropriate route, close session & return query result.
Arguments:
route - Name of Route 
date1 - Start Date if needs to filter a query on start_date
date2 - End Date if needs to filter a query on end_date
"""
def run_query(route, date1=None, date2=None):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Check for route and run query
    if route == 'Percipitation':
        sel = [Measurement.date, Measurement.prcp]
        result = session.query(*sel).all()  

    elif route == 'Stations':
        sel = [Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation]
        result = session.query(*sel).all()

    elif route == 'tobs':
        #Get the last date
        start_last_date = get_early_last_date()

        #retrive date, month & year for last_date
        lastdate=dt.datetime.strptime(start_last_date[1], "%Y-%m-%d")
        year = lastdate.year
        month = lastdate.month
        day = lastdate.day

        # Calculate the date 1 year ago from the last data point in the database
        date_year_back = dt.date(year, month, day) - dt.timedelta(days=365)

        sel = [Measurement.station, Station.name, func.count(Measurement.station)]
        active_station = session.query(*sel).filter(Measurement.station == Station.station).\
                  group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
        
        # Perform a query to retrieve the date and temperature observation
        sel = [Measurement.date, Measurement.tobs]
        result = session.query(*sel).filter(Measurement.date >= date_year_back).\
                    filter(Measurement.station == active_station[0]).all()

    elif route == 'start':
        sel=[func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)]
        result = session.query(*sel).filter(Measurement.date >= date1).all()

    elif route == 'range':
        sel=[func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)]
        result = session.query(*sel).filter(Measurement.date >= date1).\
                filter(Measurement.date <= date2).all()
                       
    # Close session
    session.close()

    # Return query result
    return result


#################################################
# Flask Routes
#################################################

"""
Error handlinfg : If incorrect route has provided
"""
@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404


"""
Defining a route for home page which will list all available routes
"""
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


"""
Defining a route which will display date & percipitation score
"""
@app.route("/api/v1.0/precipitation")
def perc_data():

    # Call run_query function    
    prcp_data = run_query('Percipitation')

    # Create an empty list to store percipitation data
    prcp_dta_list = []

    # Iterate through prcp_date
    for date, percp in prcp_data:
        # Define an empty dict & store date & percipitation score from each record in that
        prcp_data_dict = {}
        prcp_data_dict["Date"] = date
        prcp_data_dict["Percipitation"] = percp

        # append dict to list
        prcp_dta_list.append(prcp_data_dict)
    
    # Return jsonify data
    return jsonify(prcp_dta_list)


"""
Defining a route which will provide stations list
"""
@app.route("/api/v1.0/stations")
def station_list():
   
   # Call run_query function
    results = run_query('Stations')

    # Create an empty list to store stations data
    station_list = []

    # Iterate through result
    for station, name, lat, lng, elevation in results:
        # Define an empty dict & store station details from each record in that
        station_dict = {}
        station_dict["Station"] = station
        station_dict["Name"] = name
        station_dict["Latitude"] = lat
        station_dict["Longitude"] = lng
        station_dict["Elevation"] = elevation

        # append dict to list
        station_list.append(station_dict)

    # Return jsonify data
    return jsonify(station_list)

"""
Defining a route which will provide dates and temperature observations of the most active station for the last year of data
"""
@app.route("/api/v1.0/tobs")
def temperature_observations():
   
    # Call run_query function
    results = run_query('tobs')

    # Create an empty list to store temp data
    temp_data = []

    # Iterate through prcp_date
    for date, tobs in results:
        # Define an empty dict & store date & percipitation score from each record in that
        tobs_data_dict = {}
        tobs_data_dict["Date"] = date
        tobs_data_dict["Temp_Observation"] = tobs

        # append dict to list
        temp_data.append(tobs_data_dict)

    # Return jsonify data
    return jsonify(temp_data)
    
"""
Defining a route which will provide min, max & average temperature for all dates greater than and equal to the start date.
"""
@app.route("/api/v1.0/<startdate>")
def temp_start(startdate):
    
    val = validate(startdate)
    if val == 1:
        return("Incorrect url provided OR if date is provided then its an incorrect date format, should be YYYY-MM-DD")

    # Get the earliest & latest date from dataset
    start_last_date = get_early_last_date()
    
    if startdate >= start_last_date[0] and startdate <= start_last_date[1]:

        # Call run_query function
        results = run_query('start',startdate)

        # Return jsonify data
        return (
            f"Below are min, max & average temperatures for all dates greater than and equal to {startdate}</br>"
            f"Min Temp -- {results[0][0]}</br>"
            f"Max Temp -- {results[0][1]}</br>"
            f"Average Temp -- {round(results[0][2],2)}</br>"
            )
    else:
        return("Date Entered is outside of data set")

"""
Defining a route which will provide min, max & average temperature for a given start or start-end range.
"""
@app.route("/api/v1.0/<startdate>/<enddate>")
def start_end(startdate, enddate):

    val1 = validate(startdate)
    val2 = validate(enddate)
    if val1 == 1 or val2==1:
        return("Incorrect url provided OR if date is provided then its an incorrect date format, should be YYYY-MM-DD")

    if enddate <= startdate:
        err = f"Please enter the route as /api/v1.0/startdate/enddate and startdate should be less than enddate"   
        return jsonify(error=err)

    
    # Get the earliest & latest date from dataset
    start_last_date = get_early_last_date()
    
    if (startdate >= start_last_date[0] and startdate <= start_last_date[1]) and (enddate >= start_last_date[0] and enddate <= start_last_date[1]):

        # Call run_query function
        results = run_query('range',startdate, enddate)

        # Return jsonify data
        return (
            f"Below are min, max & average temperatures for all dates in range of {startdate} and {enddate}</br>"
            f"Min Temp -- {results[0][0]}</br>"
            f"Max Temp -- {results[0][1]}</br>"
            f"Average Temp -- {round(results[0][2],2)}</br>"
            )
    else:
        return jsonify(error="Date Entered is outside of data set")


#run flask server
if __name__ == '__main__':
    app.run(debug=True)
import io
import pandas as pd
import requests
from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()
import json
import duckdb

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test






@data_loader
def load_data_from_api():
    df_arrivals = pd.DataFrame()
    df_departures = pd.DataFrame()

    for i in range(-10,0):
        airport = fr_api.get_airport_details(code='EDDB',page=i)
        arrivals = pd.json_normalize(airport['airport']['pluginData']['schedule']['arrivals']['data'])
        df_arrivals = df_arrivals.append(arrivals)
      
    
    df_arrivals = df_arrivals.drop_duplicates(subset=['flight.identification.row'], keep='last')
    
    
    return df_arrivals







@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

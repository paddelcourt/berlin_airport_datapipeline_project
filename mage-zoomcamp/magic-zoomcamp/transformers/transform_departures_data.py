
import duckdb
import pandas as pd



if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test







@transformer
def departures_transform(data, *args, **kwargs):
    
    departures = duckdb.sql("""select 
    "flight.identification.id" as flight_id,
    "flight.identification.number.default" as flight_identification_number,
    "flight.status.generic.status.type" as flight_status,
    "flight.status.generic.status.color" as flight_status_color,
    "flight.aircraft.model.text" as flight_aircraft_model,
    "flight.airline.name" as flight_airline_name,
    'Berlin Brandenburg Airport'  as airport_arrival_name,
    'Germany' as airport_arrival_country,
    'Berlin'as airport_arrival_city,
    CAST('CET' as string) as airport_arrival_timezone,
    "flight.airport.destination.name" as airport_destination_name,
    "flight.airport.origin.timezone.abbr" as airport_destination_timezone,
    "flight.airport.destination.position.country.name" as airport_destination_country,
    "flight.airport.destination.position.region.city" as airport_destination_city,
    date_trunc('second', to_timestamp("flight.time.scheduled.departure")) as flight_time_scheduled_departure,
    date_trunc('second', to_timestamp("flight.time.scheduled.arrival")) as flight_time_scheduled_arrival,
    date_trunc('second', to_timestamp("flight.time.real.departure")) as flight_time_real_departure,
    date_trunc('second', to_timestamp("flight.time.real.arrival")) as flight_time_real_arrival,
    date_diff('minute',to_timestamp("flight.time.real.departure"),to_timestamp("flight.time.real.arrival")) AS flight_time_duration_minutes
    
    

    from data
    WHERE 
    flight_time_real_departure IS NOT NULL
    AND flight_time_real_departure between (current_date - interval '1' day)::timestamp and (current_date)::timestamp
    ORDER BY 
    flight_time_scheduled_departure ASC """).df()

    departures['flight_time_scheduled_departure'] = pd.to_datetime(departures['flight_time_scheduled_departure'], unit='ms')
    departures['flight_time_scheduled_arrival'] = pd.to_datetime(departures['flight_time_scheduled_arrival'], unit='ms')
    departures['flight_time_real_departure'] = pd.to_datetime(departures['flight_time_real_departure'], unit='ms')
    departures['flight_time_real_arrival'] = pd.to_datetime(departures['flight_time_real_arrival'], unit='ms')
    
    departures.reset_index(drop=True, inplace=True)

    return departures






@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

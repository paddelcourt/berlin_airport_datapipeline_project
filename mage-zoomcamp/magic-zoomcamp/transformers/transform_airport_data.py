
import duckdb
import pandas as pd


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test







@transformer
def arrivals_transform(data, *args, **kwargs):
    arrivals = duckdb.sql("""select 
    "flight.identification.id" as flight_id,
    "flight.identification.number.default" as flight_identification_number,
    "flight.status.generic.status.type" as flight_status,
    "flight.status.generic.status.color" as flight_status_color,
    "flight.aircraft.model.text" as flight_aircraft_model,
    "flight.airline.name" as flight_airline_name,
    "flight.airport.origin.name" as airport_arrival_name,
    "flight.airport.origin.position.country.name" as airport_arrival_country,
    "flight.airport.origin.position.region.city" as airport_arrival_city,
    "flight.airport.origin.timezone.abbr" as airport_arrival_timezone,
    'Berlin Brandenburg Airport' as airport_destination_name,
    CAST('CET' as string) as airport_destination_timezone,
    'Germany' as airport_destination_country,
    'Berlin' as airport_destination_city,
    date_trunc('second', to_timestamp("flight.time.scheduled.departure")) as flight_time_scheduled_departure,
    date_trunc('second',to_timestamp("flight.time.scheduled.arrival")) as flight_time_scheduled_arrival,
    date_trunc('second',to_timestamp("flight.time.real.departure")) as flight_time_real_departure,
    date_trunc('second',to_timestamp("flight.time.real.arrival")) as flight_time_real_arrival,
    CAST("flight.time.other.duration"/60 as long) as flight_time_duration_minutes

    from data
    where flight_time_real_arrival is not null
    and flight_time_real_arrival between (current_date - interval '1' day)::timestamp and (current_date)::timestamp
    order by flight_time_scheduled_arrival asc """).df()

    

    arrivals['flight_time_scheduled_departure'] = pd.to_datetime(arrivals['flight_time_scheduled_departure'], unit='ms')
    arrivals['flight_time_scheduled_arrival'] = pd.to_datetime(arrivals['flight_time_scheduled_arrival'], unit='ms')
    arrivals['flight_time_real_departure'] = pd.to_datetime(arrivals['flight_time_real_departure'], unit='ms')
    arrivals['flight_time_real_arrival'] = pd.to_datetime(arrivals['flight_time_real_arrival'], unit='ms')

    
    
    return arrivals





@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

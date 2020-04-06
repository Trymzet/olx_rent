import requests
import pendulum
from typing import List, Dict, Tuple, Any
import pandas as pd
from utils import olx_rent_logger
from prefect import task, Flow
from prefect.engine.executors import DaskExecutor
from time import sleep
import prefect
import os

os.putenv("DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING", "False")

logger = olx_rent_logger

OVERPASS_API_ENDPOINT = "http://overpass-api.de/api/interpreter"


def query_overpass(query: str, endpoint: str = OVERPASS_API_ENDPOINT) -> Dict[str, Any]:
    """Query the Overpass API
    
    Parameters
    ----------
    query : str
        Query in Overpass QL (see https://wiki.openstreetmap.org/wiki/Overpass_API)
    
    Returns
    -------
    dict
        Result of the query in JSON format
    """
    query = "[out: json];" + query
    response = requests.get(OVERPASS_API_ENDPOINT, params={"data": query})
    while response.status_code == 429:
        sleep(1)
        response = requests.get(OVERPASS_API_ENDPOINT, params={"data": query})
    data = response.json()
    return data


def get_overpass_timestamp(response: dict) -> str:
    """Extract a single timestamp from an Overpass API response
    Representing the least recent update
    
    Parameters
    ----------
    response : dict
        response form a query in JSON format
    
    Returns
    -------
    str
        timezone-aware string representation of the timestamp
    """
    timestamp_osm_base = pendulum.parse(response["osm3s"]["timestamp_osm_base"])
    timestamp_areas_base = pendulum.parse(response["osm3s"]["timestamp_areas_base"])
    timestamp = min([timestamp_osm_base, timestamp_areas_base])
    return str(timestamp)


@task(name="cities")
def get_cities(country_code: str) -> List[str]:
    """Get the list of cities of a given country using Overpass API
    
    Parameters
    ----------
    country_code : str
        ISO-formatted country code
    
    Returns
    -------
    List[str]
        The list of streets in local language
    """
    query = f"""
    area["ISO3166-1"="{country_code}"]["admin_level"="2"];
    rel(area)["admin_level"="8"];
    out;
    """
    response = query_overpass(query)
    cities = list(set([city["tags"]["name"] for city in response["elements"]]))
    return cities


@task(name="streets")
def get_streets(city: str) -> Tuple[str, List[str]]:
    """Get the list of streets of a given city using Overpass API
    
    Parameters
    ----------
    city : str
        The name of the city in local language
    """
    query = f"""
    area[name="{city}"];
    way(area)[highway][name];
    out;
    """
    logger = prefect.context.get("logger")
    logger.info(f"Retrieving streets for {city}...")
    response = query_overpass(query)
    streets = list(set([street["tags"]["name"] for street in response["elements"]]))
    logger.info(f"Retrieved {len(streets)} streets for {city}...")
    return city, streets


@task(name="get_chunk")
def get_chunk(_list: List[Any], chunk_start: int, chunk_end: int) -> List[Any]:
    return _list[chunk_start:chunk_end]


@task(name="make_dict")
def make_dict(cities_and_streets: List[Tuple[str, List[str]]]) -> Dict[str, str]:
    data = {}
    for city, streets in cities_and_streets:
        data[city] = streets
    return data


@task(name="merge")
def merge_dicts(dicts: List[Dict[Any, Any]]) -> Dict[Any, Any]:
    merged = {}
    for d in dicts:
        merged.update(d)
    return merged


@task(name="to_df")
def to_df(cities_and_streets: Dict[str, str]) -> pd.DataFrame:
    df = (
        pd.DataFrame()
        .from_dict(cities_and_streets, orient="index")
        .transpose()
        .melt(var_name="city", value_name="street")
        .drop_duplicates()
    )
    return df


@task(name="csv")
def to_csv(df: pd.DataFrame, chunk_start: int, chunk_end: int) -> None:
    df.to_csv(f"parts_{chunk_start}-{chunk_end}.csv", index=False, chunksize=100000)


with Flow("List of Polish cities and streets") as flow:
    chunksize = 100
    chunk_start, chunk_end = 1100, 1100 + chunksize
    limit = 3000
    cities = get_cities(country_code="PL")
    dict_chunks = []
    i = 1100
    while True:
        if i > limit:
            break
        cities_chunk = get_chunk(cities, chunk_start=chunk_start, chunk_end=chunk_end)
        streets_chunk = get_streets.map(cities_chunk)
        dict_chunk = make_dict(streets_chunk)
        # dict_chunks.append(dict_chunk)
        df = to_df(dict_chunk)
        csv = to_csv(df, chunk_start=chunk_start, chunk_end=chunk_end)

        i += chunksize
        chunk_start += chunksize
        chunk_end += chunksize

    # merged = merge_dicts(dict_chunks)
    # df = to_df(merged)
    # csv = to_csv(df)

flow.run()

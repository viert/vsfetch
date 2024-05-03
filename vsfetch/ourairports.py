import requests
from typing import Optional, Dict
from pydantic import BaseModel
from vsfetch.config import get_config


RUNWAY_MAP_URL = "https://raw.githubusercontent.com/viert/ourairports-json/main/output/runway_split_map.json"


class Runway(BaseModel):
    airport_ref: int
    airport_ident: str
    length_ft: Optional[int]
    width_ft: Optional[int]
    surface: str
    lighted: bool
    closed: bool
    ident: str
    latitude_deg: Optional[float]
    longitude_deg: Optional[float]
    elevation_ft: Optional[int]
    heading_degT: Optional[int]
    displaced_threshold_ft: Optional[int]


_data: Optional[Dict[str, Dict[str, Runway]]] = None


def reload():
    global _data
    resp = requests.get(RUNWAY_MAP_URL, timeout=get_config().external.timeout)
    runway_map = {}
    for key, r_map in resp.json().items():
        runway_map[key] = {}
        for ident, rwy in r_map.items():
            runway_map[key][ident] = Runway(**rwy)
    _data = runway_map


def get_data() -> Dict[str, Dict[str, Runway]]:
    global _data
    if _data is None:
        reload()
    return _data


def find_airport_runways(icao: str) -> Optional[Dict[str, Runway]]:
    data = get_data()
    return data.get(icao)

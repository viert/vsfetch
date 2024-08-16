from typing import Optional, Dict
from pydantic import BaseModel
from vsfetch.http import get_json


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


async def reload():
    global _data
    data = await get_json(RUNWAY_MAP_URL)
    runway_map = {}
    for key, r_map in data.items():
        runway_map[key] = {}
        for ident, rwy in r_map.items():
            runway_map[key][ident] = Runway(**rwy)
    _data = runway_map


async def get_data() -> Dict[str, Dict[str, Runway]]:
    global _data
    if _data is None:
        await reload()
    return _data


async def find_airport_runways(icao: str) -> Optional[Dict[str, Runway]]:
    data = await get_data()
    return data.get(icao)

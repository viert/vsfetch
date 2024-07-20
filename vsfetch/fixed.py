import time
import requests
from typing import Self, Optional, List, Dict, DefaultDict, Annotated, TYPE_CHECKING, Any
from collections import defaultdict

import shapely
from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator
from shapely.geometry import shape
from vsfetch.log import log
from vsfetch.config import get_config

if TYPE_CHECKING:
    from .dynamic import Controller

FIXED_DATA_URL = "https://raw.githubusercontent.com/vatsimnetwork/vatspy-data-project/master/VATSpy.dat"
BOUNDARIES_DATA_URL = "https://raw.githubusercontent.com/vatsimnetwork/vatspy-data-project/master/Boundaries.geojson"


class Point(BaseModel):
    lat: float
    lng: float


class BoundingBox(BaseModel):
    min: Point
    max: Point


class Boundaries(BaseModel):
    geometry: Dict[str, Any]
    bbox: BoundingBox
    center: Point


_boundaries: Optional[Dict[str, Boundaries]] = None


def boundaries() -> Dict[str, Boundaries]:
    global _boundaries
    if _boundaries is None:
        log.debug("loading and parsing boundaries from %s", BOUNDARIES_DATA_URL)
        t1 = time.time()
        resp = requests.get(BOUNDARIES_DATA_URL, timeout=get_config().external.timeout)
        data = resp.json()
        bounds = {}
        for feature in data["features"]:
            icao = feature["properties"]["id"]
            s = shape(feature["geometry"])
            center = shapely.centroid(s)
            bounds[icao] = Boundaries(
                geometry=feature["geometry"],
                bbox=BoundingBox(
                    min=Point(lng=s.bounds[0], lat=s.bounds[1]),
                    max=Point(lng=s.bounds[2], lat=s.bounds[3]),
                ),
                center=Point(lng=center.x, lat=center.y)
            )
        _boundaries = bounds
        t2 = time.time()
        log.debug("boundaries parsed in %.3fs", t2 - t1)
    return _boundaries


class ParseError(Exception):
    pass


class Country(BaseModel):
    name: str
    code: str
    custom_control_name: Annotated[Optional[str], BeforeValidator(lambda x: x if x else None)]

    @classmethod
    def parse(cls, line: str) -> Self:
        tokens = line.strip().split("|")
        if len(tokens) != 3:
            raise ParseError(f"invalid country line: {line}")
        return cls(
            name=tokens[0],
            code=tokens[1],
            custom_control_name=tokens[2]
        )


class Airport(BaseModel):
    icao: str
    name: str
    latitude: Annotated[float, BeforeValidator(lambda x: float(x))]
    longitude: Annotated[float, BeforeValidator(lambda x: float(x))]
    iata: Annotated[Optional[str], BeforeValidator(lambda x: x if x else None)]
    fir: str
    is_pseudo: Annotated[bool, BeforeValidator(lambda x: x == "1")]

    @classmethod
    def parse(cls, line: str) -> Self:
        tokens = line.strip().split("|")
        if len(tokens) != 7:
            raise ParseError(f"invalid airport line: {line}")
        return cls(
            icao=tokens[0],
            name=tokens[1],
            latitude=tokens[2],
            longitude=tokens[3],
            iata=tokens[4],
            fir=tokens[5],
            is_pseudo=tokens[6],
        )


class FIR(BaseModel):
    icao: str
    name: str
    prefix: str
    boundaries: Optional[Boundaries] = None

    @classmethod
    def parse(cls, line: str) -> Self:
        tokens = line.strip().split("|")
        if len(tokens) != 4:
            raise ParseError(f"invalid FIR line: {line}")

        bds = boundaries().get(tokens[3], boundaries().get(tokens[0]))

        if bds is None:
            log.error(f"can't find boundaries for fir {tokens[0]} {tokens[2]}")

        return cls(
            icao=tokens[0],
            name=tokens[1],
            prefix=tokens[2],
            boundaries=bds
        )


class UIR(BaseModel):
    icao: str
    name: str
    fir_ids: Annotated[List[str], BeforeValidator(lambda x: x.strip().split(","))]

    @classmethod
    def parse(cls, line: str) -> Self:
        tokens = line.strip().split("|")
        if len(tokens) != 3:
            raise ParseError(f"invalid UIR line: {line}")
        return cls(
            icao=tokens[0],
            name=tokens[1],
            fir_ids=tokens[2],
        )


class Data:

    _countries: List[Country]
    _airports: List[Airport]
    _firs: List[FIR]
    _uirs: List[UIR]

    _country_idx: Dict[str, int]
    _airport_icao_idx: DefaultDict[str, List[int]]
    _airport_iata_idx: DefaultDict[str, List[int]]
    _fir_icao_idx: DefaultDict[str, List[int]]
    _fir_prefix_idx: Dict[str, int]
    _uir_icao_idx: Dict[str, int]
    _uir_fir_idx: Dict[str, int]

    def __init__(self,
                 countries: List[Country],
                 airports: List[Airport],
                 firs: List[FIR],
                 uirs: List[UIR],
                 ):
        self._countries = countries
        self._airports = airports
        self._firs = firs
        self._uirs = uirs

        self.build_indexes()

    @classmethod
    def load(cls) -> Self:
        log.debug("loading fixed data from %s", FIXED_DATA_URL)
        resp = requests.get(FIXED_DATA_URL, timeout=get_config().external.timeout)
        data = resp.text
        return cls.parse(data)

    @classmethod
    def parse(cls, text: str) -> Self:
        t1 = time.time()
        log.debug("parsing fixed data")
        c_section = None

        countries = []
        airports = []
        firs = []
        uirs = []

        for line in text.split("\n"):
            line = line.strip()
            if not line or line.startswith(";"):
                continue

            if line.startswith("[") and line.endswith("]"):
                c_section = line[1:-1].lower()
                continue

            match c_section:
                case "countries":
                    countries.append(Country.parse(line))
                case "airports":
                    airports.append(Airport.parse(line))
                case "firs":
                    firs.append(FIR.parse(line))
                case "uirs":
                    uirs.append(UIR.parse(line))

        t2 = time.time()
        log.debug("fixed data parsed in %.3fs", t2 - t1)
        return cls(countries, airports, firs, uirs)

    def find_airport_by_ctrl(self, ctrl: "Controller") -> Optional[Airport]:
        code = ctrl.callsign.split("_")[0]
        if len(code) < 4:
            idxs = self._airport_iata_idx.get(code)
            if not idxs:
                return None
            return self._airports[idxs[0]]

        idxs = self._airport_icao_idx.get(code, self._airport_iata_idx.get(code))
        if not idxs:
            return None
        return self._airports[idxs[0]]

    def find_fir_by_ctrl(self, ctrl: "Controller") -> Optional[FIR]:
        code = ctrl.callsign.split("_")[0]
        idxs = self._fir_icao_idx.get(code)
        if idxs:
            return self._firs[idxs[0]]

        for i in range(len(ctrl.callsign), 4, -1):
            code = ctrl.callsign[:i]
            idx = self._fir_prefix_idx.get(code)
            if idx is not None:
                return self._firs[idx]

    def find_country_by_icao(self, icao: str) -> Optional[Country]:
        idx = self._country_idx.get(icao[:2])
        if idx:
            return self._countries[idx]
        return None

    def build_indexes(self):
        log.debug("building fixed data indexes")
        t1 = time.time()

        self._country_idx = {}
        for i, c in enumerate(self._countries):
            self._country_idx[c.code] = i

        self._airport_icao_idx = defaultdict(list)
        self._airport_iata_idx = defaultdict(list)

        for i, a in enumerate(self._airports):
            self._airport_icao_idx[a.icao].append(i)
            if a.iata is not None:
                self._airport_iata_idx[a.iata].append(i)

        self._fir_icao_idx = defaultdict(list)
        self._fir_prefix_idx = {}

        for i, f in enumerate(self._firs):
            self._fir_icao_idx[f.icao].append(i)
            self._fir_prefix_idx[f.prefix] = i

        self._uir_icao_idx = {}
        self._uir_fir_idx = {}

        for i, u in enumerate(self._uirs):
            self._uir_icao_idx[u.icao] = i
            for fir_id in u.fir_ids:
                self._uir_fir_idx[fir_id] = i

        t2 = time.time()
        log.debug("fixed data indexes built in %.3fs", t2 - t1)


_data: Optional[Data] = None


def reload():
    global _data
    _data = Data.load()


def get_data() -> Data:
    global _data
    if _data is None:
        reload()
    return _data

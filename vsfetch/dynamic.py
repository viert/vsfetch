import time
import requests
from typing import Optional, List, Annotated, Dict, TypeVar
from datetime import datetime
from pydantic import BaseModel, Field
from pydantic.functional_validators import BeforeValidator
from vsfetch.log import log
from vsfetch.fixed import Airport as FixedAirport, FIR as FixedFIR, data as fixed_data


VATSIM_DATA_URL = "https://data.vatsim.net/v3/vatsim-data.json"
VERSIONED_BASE_URL = "http://localhost:9440"
TRACKED_BASE_URL = "http://localhost:9441"

TBaseModel = TypeVar("TBaseModel", bound=BaseModel)


def parse_vatsim_date_str(date_str: str) -> datetime:
    try:
        dt = datetime.strptime(date_str[:26], "%Y-%m-%dT%H:%M:%S.%f")
        return dt
    except ValueError as e:
        log.error(f"error parsing datetime %s: %s", date_str, e)
        dt = datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S")
        return dt


def parse_vatsim_date_str_ts_ms(date_str: str) -> int:
    dt = parse_vatsim_date_str(date_str)
    return round(dt.timestamp() * 1000)


def join_if_exists(data: Optional[List[str]]) -> Optional[str]:
    return "\n".join(data) if data else None


class TrackPoint(BaseModel):
    ts: int
    lat: float
    lng: float
    hdg: int
    alt: int
    gs: int


class TrackObject(BaseModel):
    track_id: str
    point: TrackPoint


class VersionedPoint(BaseModel):
    lat: float
    lng: float


class VersionedRect(BaseModel):
    min: VersionedPoint
    max: VersionedPoint


class Controller(BaseModel):
    cid: int
    name: str
    callsign: str
    frequency: str
    facility: int
    visual_range: int
    text_atis: Annotated[Optional[str], BeforeValidator(join_if_exists)] = None
    logon_time: str


class AirportControllerSet(BaseModel):
    atis: Optional[Controller] = None
    delivery: Optional[Controller] = None
    ground: Optional[Controller] = None
    tower: Optional[Controller] = None
    approach: Optional[Controller] = None

    @property
    def is_empty(self) -> bool:
        return (
            self.atis is None and
            self.delivery is None and
            self.ground is None and
            self.tower is None and
            self.approach is None
        )


class Airport(FixedAirport):
    controllers: AirportControllerSet = Field(default_factory=AirportControllerSet)

    def versioned_object(self, version: int) -> "VersionedObject":
        return VersionedObject(
            data=self,
            point=VersionedPoint(lat=self.latitude, lng=self.longitude),
            version=version
        )

    @property
    def is_empty(self) -> bool:
        return self.controllers.is_empty


class FIR(FixedFIR):
    controller: Optional[Controller] = None

    @property
    def is_empty(self) -> bool:
        return self.controller is None

    def versioned_object(self, version: int) -> "VersionedObject":
        return VersionedObject(
            data=self,
            rect=VersionedRect(
                min=VersionedPoint(lng=self.boundaries.bbox.min.lng, lat=self.boundaries.bbox.min.lat),
                max=VersionedPoint(lng=self.boundaries.bbox.max.lng, lat=self.boundaries.bbox.max.lat),
            ),
            version=version
        )


class FlightPlan(BaseModel):
    flight_rules: str
    aircraft: str
    aircraft_faa: str
    aircraft_short: str
    departure: str
    arrival: str
    alternate: str
    cruise_tas: str
    altitude: str
    deptime: str
    enroute_time: str
    fuel_time: str
    remarks: str
    route: str
    revision_id: int
    assigned_transponder: str


class Pilot(BaseModel):
    cid: int
    name: str
    callsign: str
    latitude: float
    longitude: float
    altitude: int
    groundspeed: int
    transponder: str
    heading: int
    qnh_i_hg: float
    qnh_mb: int
    flight_plan: Optional[FlightPlan]
    logon_time: str

    def track_object(self, ts: int) -> TrackObject:
        track_id = f"{self.callsign}.{self.cid}.{self.logon_time}"
        return TrackObject(
            track_id=track_id,
            point=TrackPoint(
                ts=ts,
                lat=self.latitude,
                lng=self.longitude,
                hdg=self.heading,
                alt=self.altitude,
                gs=self.groundspeed
            )
        )

    def versioned_object(self, version: int) -> "VersionedObject":
        return VersionedObject(
            data=self,
            point=VersionedPoint(lat=self.latitude, lng=self.longitude),
            version=version
        )


class VersionedObject(BaseModel):
    data: TBaseModel
    point: Optional[VersionedPoint] = None
    rect: Optional[VersionedRect] = None
    version: int


def store_track(pilots: List[Pilot], version: int):
    t1 = time.time()
    objects = [pilot.track_object(version).model_dump() for pilot in pilots]
    req = {"data": objects}
    url = f"{TRACKED_BASE_URL}/api/v1/tracks/"
    log.debug(f"storing track data to %s", url)
    resp = requests.post(url, json=req)
    data = resp.json()
    t2 = time.time()
    log.info("track data stored in %.3fs status: %s", t2-t1, data["status"])


def store_pilots(pilots: List[Pilot], version: int):
    t1 = time.time()
    object_map = {
        f"pilot:{pilot.callsign}": pilot.versioned_object(version).model_dump(exclude_none=True) for pilot in pilots
    }
    req = {"data": object_map}
    url = f"{VERSIONED_BASE_URL}/api/v1/objects/"
    log.debug(f"storing pilots data to %s", url)
    resp = requests.post(url, json=req)
    data = resp.json()
    t2 = time.time()
    log.info("versioned pilots stored in %.3fs status: %s", t2-t1, data["status"])

    t1 = time.time()
    log.debug("collecting existing pilot keys from versioned db")
    url = f"{VERSIONED_BASE_URL}/api/v1/keys/?prefix=pilot:"
    resp = requests.get(url)
    data = resp.json()

    keys = set(data["keys"])
    new_keys = set(object_map.keys())
    keys_to_remove = keys.difference(new_keys)

    log.debug("keys in db %d, number of keys to remove %d", len(keys), len(keys_to_remove))

    if keys_to_remove:
        req = {
            "data": [{"key": key} for key in keys_to_remove],
            "version": version
        }

        url = f"{VERSIONED_BASE_URL}/api/v1/objects/"
        resp = requests.delete(url, json=req)
        data = resp.json()
        t2 = time.time()
        log.debug(f"%s in %.3fs", data["status"], t2-t1)


def store_controllers(ctrls: List[Controller], atis: List[Controller], version: int):

    t1 = time.time()
    airports: Dict[str, Airport] = {}
    firs: Dict[str, FIR] = {}

    for ctrl in ctrls:
        if 2 <= ctrl.facility <= 5:
            f_arpt = fixed_data.find_airport_by_ctrl(ctrl)
            if f_arpt is None:
                log.debug("can't find airport by callsign %s", ctrl.callsign)
                continue

            arpt = airports.get(f_arpt.icao, Airport(**f_arpt.model_dump()))

            match ctrl.facility:
                case 2:
                    arpt.controllers.delivery = ctrl
                case 3:
                    arpt.controllers.ground = ctrl
                case 4:
                    arpt.controllers.tower = ctrl
                case 5:
                    arpt.controllers.approach = ctrl
            airports[arpt.icao] = arpt
        elif ctrl.facility == 6:
            f_fir = fixed_data.find_fir_by_ctrl(ctrl)
            if f_fir is None:
                log.debug("can't find FIR by callsign %s", ctrl.callsign)
                continue

            fir = firs.get(f_fir.icao, FIR(**f_fir.model_dump()))
            fir.controller = ctrl
            firs[fir.icao] = fir
        else:
            continue

    for ctrl in atis:
        f_arpt = fixed_data.find_airport_by_ctrl(ctrl)
        if f_arpt is None:
            log.debug("can't find airport by callsign %s", ctrl.callsign)
            continue

        arpt = airports.get(f_arpt.icao, Airport(**f_arpt.model_dump()))
        arpt.controllers.atis = ctrl
        airports[arpt.icao] = arpt

    airport_map = {
        f"airport:{arpt.icao}": arpt.versioned_object(version).model_dump(exclude_none=True) for arpt in airports.values()
    }

    fir_map = {
        f"fir:{fir.icao}": fir.versioned_object(version).model_dump(exclude_none=True) for fir in firs.values()
    }

    url = f"{VERSIONED_BASE_URL}/api/v1/objects/"

    req = {"data": airport_map}
    log.debug(f"storing airport data to %s", url)
    resp = requests.post(url, json=req)
    airport_data = resp.json()

    req = {"data": fir_map}
    log.debug(f"storing fir data to %s", url)
    resp = requests.post(url, json=req)
    fir_data = resp.json()
    t2 = time.time()

    log.info("versioned airports stored in %.3fs", t2-t1)
    log.debug("airport store status: %s", airport_data["status"])
    log.debug("fir store status: %s", fir_data["status"])

    t1 = time.time()
    log.debug("collecting existing airport keys from versioned db")
    url = f"{VERSIONED_BASE_URL}/api/v1/keys/?prefix=airport:"
    resp = requests.get(url)
    data = resp.json()

    keys = set(data["keys"])
    new_keys = set(airport_map.keys())
    keys_to_remove = keys.difference(new_keys)

    log.debug("keys in db %d, number of keys to remove %d", len(keys), len(keys_to_remove))

    if keys_to_remove:
        req = {
            "data": [{"key": key} for key in keys_to_remove],
            "version": version
        }

        url = f"{VERSIONED_BASE_URL}/api/v1/objects/"
        resp = requests.delete(url, json=req)
        data = resp.json()
        t2 = time.time()
        log.debug(f"%s in %.3fs", data["status"], t2-t1)

    t1 = time.time()
    log.debug("collecting existing fir keys from versioned db")
    url = f"{VERSIONED_BASE_URL}/api/v1/keys/?prefix=fir:"
    resp = requests.get(url)
    data = resp.json()

    keys = set(data["keys"])
    new_keys = set(fir_map.keys())
    keys_to_remove = keys.difference(new_keys)

    log.debug("keys in db %d, number of keys to remove %d", len(keys), len(keys_to_remove))

    if keys_to_remove:
        req = {
            "data": [{"key": key} for key in keys_to_remove],
            "version": version
        }

        url = f"{VERSIONED_BASE_URL}/api/v1/objects/"
        resp = requests.delete(url, json=req)
        data = resp.json()
        t2 = time.time()
        log.debug(f"%s in %.3fs", data["status"], t2-t1)


def process(prev_version: Optional[int] = None) -> int:
    log.debug("fetching data from %s", VATSIM_DATA_URL)
    resp = requests.get(VATSIM_DATA_URL)
    data = resp.json()

    version = parse_vatsim_date_str_ts_ms(data["general"]["update_timestamp"])
    if prev_version and version <= prev_version:
        log.debug("previous data version is the same or fresher, skipping")
        return prev_version

    # pilots = [Pilot(**pilot) for pilot in data["pilots"]]
    #
    # store_track(pilots, version)
    # store_pilots(pilots, version)

    ctrls = [Controller(**ctrl) for ctrl in data["controllers"]]
    atis = [Controller(**ctrl) for ctrl in data["atis"]]

    store_controllers(ctrls, atis, version)

    return version


def loop():
    version = None
    while True:
        new_version = process(version)
        if new_version == version:
            log.debug("no new data, sleeping for 3 seconds")
            time.sleep(3)
        else:
            version = new_version
            log.debug("data processed, sleeping for 10 seconds")
            time.sleep(10)

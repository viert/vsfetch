import time
import requests
from typing import Optional, List, Annotated, Dict, Set, TypeVar
from datetime import datetime
from dateutil.parser import parse
from pydantic import BaseModel, Field
from pydantic.functional_validators import BeforeValidator
from vsfetch.log import log
from vsfetch.config import get_config
from vsfetch.fixed import Airport as FixedAirport, FIR as FixedFIR, get_data as get_fixed_data
from vsfetch.ourairports import find_airport_runways


VATSIM_DATA_URL = "https://data.vatsim.net/v3/vatsim-data.json"

TBaseModel = TypeVar("TBaseModel", bound=BaseModel)


def parse_vatsim_date_str(date_str: str) -> datetime:
    return parse(date_str)


def parse_vatsim_date_str_ts_ms(date_str: str) -> int:
    dt = parse_vatsim_date_str(date_str)
    return round(dt.timestamp() * 1000)


def join_if_exists(data: Optional[List[str] | str]) -> Optional[str]:
    if isinstance(data, list):
        return "\n".join(data)
    else:
        return data


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
    human_readable: Optional[str] = None


class StoredController(Controller):
    position: VersionedPoint

    def versioned_object(self, version: int) -> "VersionedObject":
        return VersionedObject(
            data=self,
            version=version
        )


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


class Runway(BaseModel):
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
    active_to: bool = False
    active_lnd: bool = False


class Airport(FixedAirport):
    controllers: AirportControllerSet = Field(default_factory=AirportControllerSet)
    type: str = "airport"
    runways: Dict[str, Runway] = Field(default_factory=dict)

    def versioned_object(self, version: int) -> "VersionedObject":
        return VersionedObject(
            data=self,
            version=version
        )

    @property
    def is_empty(self) -> bool:
        return self.controllers.is_empty


class FIR(FixedFIR):
    controllers: Dict[str, Controller] = Field(default_factory=dict)
    type: str = "fir"

    @property
    def is_empty(self) -> bool:
        return self.controller is None

    def versioned_object(self, version: int) -> "VersionedObject":
        return VersionedObject(
            data=self,
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
    logon_time: Annotated[int, BeforeValidator(parse_vatsim_date_str_ts_ms)]
    type: str = "pilot"

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
    cfg = get_config()
    t1 = time.time()
    objects = [pilot.track_object(version).model_dump() for pilot in pilots]
    req = {"data": objects}
    url = f"{cfg.tracked.base_url}/api/v1/tracks/"
    log.debug(f"storing track data to %s", url)
    resp = requests.post(url, json=req, timeout=cfg.tracked.timeout)
    if resp.status_code >= 300:
        log.error(f"unsuccessful status code, response is {resp.text}")
    data = resp.json()
    t2 = time.time()
    log.info("track data stored in %.3fs status: %s", t2-t1, data["status"])


def store_pilots(pilots: List[Pilot], version: int):
    cfg = get_config()
    t1 = time.time()
    object_map = {
        f"pilot:{pilot.callsign}": pilot.versioned_object(version).model_dump(exclude_none=True) for pilot in pilots
    }
    req = {"data": object_map}
    url = f"{cfg.versioned.base_url}/api/v1/objects/"
    log.debug(f"storing pilots data to %s", url)
    resp = requests.post(url, json=req, timeout=cfg.versioned.timeout)
    data = resp.json()
    t2 = time.time()
    log.info("versioned pilots stored in %.3fs status: %s", t2-t1, data["status"])
    delete_old_keys("pilot:", set(object_map.keys()), version)


def delete_old_keys(prefix: str, new_keys: Set[str], version: int):
    cfg = get_config()
    t1 = time.time()
    log.debug("collecting existing keys with prefix \"%s\" from versioned db", prefix)
    url = f"{cfg.versioned.base_url}/api/v1/keys/?prefix={prefix}"
    resp = requests.get(url, timeout=cfg.versioned.timeout)
    data = resp.json()

    keys = set(data["keys"])
    keys_to_remove = keys.difference(new_keys)

    log.debug("keys in db %d, number of keys to remove %d", len(keys), len(keys_to_remove))

    if keys_to_remove:
        req = {
            "data": [{"key": key} for key in keys_to_remove],
            "version": version
        }

        url = f"{cfg.versioned.base_url}/api/v1/objects/"
        resp = requests.delete(url, json=req, timeout=cfg.versioned.timeout)
        data = resp.json()
        t2 = time.time()
        log.debug(f"%s in %.3fs", data["status"], t2-t1)


def store_controllers(ctrls: List[Controller], atis: List[Controller], version: int):
    cfg = get_config()

    t1 = time.time()
    airports: Dict[str, Airport] = {}
    firs: Dict[str, FIR] = {}
    pure_ctrls: Dict[str, StoredController] = {}

    for ctrl in ctrls:
        if 2 <= ctrl.facility <= 5:
            f_arpt = get_fixed_data().find_airport_by_ctrl(ctrl)
            if f_arpt is None:
                log.debug("can't find airport by callsign %s", ctrl.callsign)
                continue

            arpt = airports.get(f_arpt.icao, Airport(**f_arpt.model_dump()))
            runways = find_airport_runways(arpt.icao)
            if runways:
                runways = {k: Runway(**rwy.model_dump()) for k, rwy in runways.items()}
                arpt.runways = runways

            match ctrl.facility:
                case 2:
                    ctrl.human_readable = f"{arpt.name} Delivery"
                    arpt.controllers.delivery = ctrl
                case 3:
                    ctrl.human_readable = f"{arpt.name} Ground"
                    arpt.controllers.ground = ctrl
                case 4:
                    ctrl.human_readable = f"{arpt.name} Tower"
                    arpt.controllers.tower = ctrl
                case 5:
                    ctrl.human_readable = f"{arpt.name} Approach"
                    arpt.controllers.approach = ctrl
            airports[arpt.icao] = arpt

            stored_ctrl = {
                **ctrl.model_dump(),
                "position": VersionedPoint(lat=arpt.latitude, lng=arpt.longitude)
            }
            pure_ctrls[ctrl.callsign] = StoredController(**stored_ctrl)

        elif ctrl.facility == 6:
            f_fir = get_fixed_data().find_fir_by_ctrl(ctrl)
            if f_fir is None:
                log.debug("can't find FIR by callsign %s", ctrl.callsign)
                continue

            fir = firs.get(f_fir.icao, FIR(**f_fir.model_dump()))
            control_name = "Radar"

            country = get_fixed_data().find_country_by_icao(fir.icao)
            if country:
                if country.custom_control_name:
                    control_name = country.custom_control_name

            ctrl.human_readable = f"{fir.name} {control_name}"

            fir.controllers[ctrl.callsign] = ctrl
            firs[fir.icao] = fir
            stored_ctrl = {
                **ctrl.model_dump(),
                "position": VersionedPoint(lat=fir.boundaries.center.lat, lng=fir.boundaries.center.lng)
            }
            pure_ctrls[ctrl.callsign] = StoredController(**stored_ctrl)
        else:
            continue

    for ctrl in atis:
        ctrl.facility = 1
        f_arpt = get_fixed_data().find_airport_by_ctrl(ctrl)
        if f_arpt is None:
            log.debug("can't find airport by callsign %s", ctrl.callsign)
            continue

        arpt = airports.get(f_arpt.icao, Airport(**f_arpt.model_dump()))
        ctrl.human_readable = f"{arpt.name} ATIS"
        arpt.controllers.atis = ctrl
        airports[arpt.icao] = arpt
        stored_ctrl = {
            **ctrl.model_dump(),
            "position": VersionedPoint(lat=arpt.latitude, lng=arpt.longitude)
        }
        pure_ctrls[ctrl.callsign] = StoredController(**stored_ctrl)

    airport_map = {
        f"airport:{arpt.icao}": arpt.versioned_object(version).model_dump(exclude_none=True)
        for arpt in airports.values()
    }

    fir_map = {
        f"fir:{fir.icao}": fir.versioned_object(version).model_dump(exclude_none=True)
        for fir in firs.values()
    }

    pure_ctrl_map = {
        f"ctrl:{ctrl.callsign}": ctrl.versioned_object(version).model_dump(exclude_none=True)
        for ctrl in pure_ctrls.values()
    }

    url = f"{cfg.versioned.base_url}/api/v1/objects/"

    req = {"data": airport_map}
    log.debug("storing airport data to %s", url)
    resp = requests.post(url, json=req, timeout=cfg.versioned.timeout)
    airport_data = resp.json()

    req = {"data": fir_map}
    log.debug("storing fir data to %s", url)
    resp = requests.post(url, json=req, timeout=cfg.versioned.timeout)
    fir_data = resp.json()

    req = {"data": pure_ctrl_map}
    log.debug("storing pure controllers to %s", url)
    resp = requests.post(url, json=req, timeout=cfg.versioned.timeout)
    ctrl_data = resp.json()
    t2 = time.time()

    log.info("versioned airports stored in %.3fs", t2-t1)
    log.debug("airport store status: %s", airport_data["status"])
    log.debug("fir store status: %s", fir_data["status"])
    log.debug("pure ctrl store status: %s", ctrl_data["status"])

    delete_old_keys("airport:", set(airport_map.keys()), version)
    delete_old_keys("fir:", set(fir_map.keys()), version)
    delete_old_keys("ctrl:", set(pure_ctrl_map.keys()), version)


def process(prev_version: Optional[int] = None) -> int:
    log.debug("fetching data from %s", VATSIM_DATA_URL)

    resp = requests.get(VATSIM_DATA_URL, timeout=get_config().external.timeout)
    data = resp.json()

    version = parse_vatsim_date_str_ts_ms(data["general"]["update_timestamp"])
    if prev_version and version <= prev_version:
        log.debug("previous data version is the same or fresher, skipping")
        return prev_version

    pilots = [Pilot(**pilot) for pilot in data["pilots"]]

    store_track(pilots, version)
    store_pilots(pilots, version)

    ctrls = [Controller(**ctrl) for ctrl in data["controllers"]]
    atis = [Controller(**ctrl) for ctrl in data["atis"]]

    store_controllers(ctrls, atis, version)

    return version


def loop():
    version = None
    while True:
        try:
            new_version = process(version)
        except Exception as e:
            log.error(f"error processing version {version}: {e}, sleeping for 10 seconds")
            time.sleep(10)
            continue

        if new_version == version:
            log.debug("no new data, sleeping for 3 seconds")
            time.sleep(3)
        else:
            version = new_version
            log.debug("data processed, sleeping for 10 seconds")
            time.sleep(10)

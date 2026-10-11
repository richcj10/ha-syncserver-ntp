"""SyncServer NTP Monitor.

Polls a Symmetricom SyncServer (S2xx) GPS time server over SNMP and NTP, checks which
network devices follow it, and publishes everything to Home Assistant via MQTT discovery.
"""
import json
import logging
import os
import re
import socket
import struct
import subprocess
import time
import urllib.request
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("s250_ntp")

OPTIONS_PATH = "/data/options.json"
BASE = "s250_ntp"
NTP_EPOCH = 2208988800

# SYMMETRICOM syncserver.version subtree (1.3.6.1.4.1.9070.1.2.3.1.5.1)
SNMP_BASE = "1.3.6.1.4.1.9070.1.2.3.1.5.1"
SNMP_OIDS = {
    "freq": "1.11", "ntp_error": "1.12",
    "timing_status": "2.1", "timing_source": "2.2", "fly_period": "2.5",
    "utc_offset": "3.2", "gps_health": "3.3", "satlist": "3.4", "gps_mode": "3.5",
    "firmware": "6.1", "serial": "6.2", "model": "6.3",
}
GPS_HEALTH = {0: "Receiver down", 1: "Unknown mode", 2: "Acquiring signal", 3: "Bad geometry",
              4: "Propagate mode", 5: "2D solution", 6: "3D solution", 7: "Position hold", 8: "Time valid"}
LEAP = {0: "none", 1: "+1 s pending", 2: "-1 s pending", 3: "unsynchronised"}

# (component, object_id, name, state key, extra discovery fields)
ENTITIES = [
    ("sensor", "status", "Status", "status", {"icon": "mdi:clock-check-outline", "json_attributes_topic": None}),
    ("binary_sensor", "gps_locked", "GPS Locked", "locked",
     {"payload_on": "on", "payload_off": "off", "icon": "mdi:crosshairs-gps"}),
    ("binary_sensor", "ntp_reachable", "NTP Reachable", "reachable",
     {"payload_on": "on", "payload_off": "off", "device_class": "connectivity"}),
    ("binary_sensor", "holdover", "Holdover", "holdover",
     {"payload_on": "on", "payload_off": "off", "device_class": "problem"}),
    ("sensor", "gps_receiver", "GPS Receiver", "gps_receiver", {"icon": "mdi:satellite-variant"}),
    ("sensor", "receiver_mode", "Receiver Mode", "receiver_mode", {"icon": "mdi:map-marker-check"}),
    ("sensor", "timing_status", "Timing Status", "timing_status", {"icon": "mdi:timer-check-outline"}),
    ("sensor", "holdover_time", "Holdover Time", "holdover_s",
     {"unit_of_measurement": "s", "device_class": "duration", "state_class": "measurement"}),
    ("sensor", "satellites", "Satellites", "satellites", {"icon": "mdi:satellite-uplink"}),
    ("sensor", "stratum", "Stratum", "stratum", {"state_class": "measurement", "icon": "mdi:layers"}),
    ("sensor", "reference", "Reference", "refid", {"icon": "mdi:clock-check-outline"}),
    ("sensor", "leap_second", "Leap Second", "leap", {"icon": "mdi:clock-plus-outline"}),
    ("sensor", "oscillator_offset", "Oscillator Offset", "osc_ppm",
     {"unit_of_measurement": "ppm", "state_class": "measurement", "icon": "mdi:sine-wave"}),
    ("sensor", "ha_clock_offset", "HA Clock Offset", "ha_offset_us",
     {"unit_of_measurement": "µs", "state_class": "measurement", "icon": "mdi:clock-fast"}),
    ("sensor", "devices_on_gps", "Devices on GPS Time", "clients_ok",
     {"state_class": "measurement", "icon": "mdi:lan-check", "json_attributes_topic": f"{BASE}/clients"}),
    ("sensor", "gps_utc_offset", "GPS-UTC Offset", "utc_offset",
     {"unit_of_measurement": "s", "icon": "mdi:timer-sand", "entity_category": "diagnostic"}),
    ("sensor", "root_dispersion", "Root Dispersion", "root_disp_ms",
     {"unit_of_measurement": "ms", "state_class": "measurement", "entity_category": "diagnostic"}),
    ("sensor", "ntp_error", "NTP Error", "ntp_error", {"entity_category": "diagnostic"}),
    ("sensor", "gps_health_code", "GPS Health Code", "gps_health_code",
     {"state_class": "measurement", "entity_category": "diagnostic"}),
    ("sensor", "firmware", "Firmware", "firmware", {"entity_category": "diagnostic"}),
]


def load_options() -> dict:
    with open(OPTIONS_PATH) as f:
        return json.load(f)


def supervisor_token() -> str:
    """The token is in the environment, or (s6-overlay, init: false) only in the s6 container env dir."""
    for name in ("SUPERVISOR_TOKEN", "HASSIO_TOKEN"):
        if os.environ.get(name):
            return os.environ[name]
        try:
            with open(f"/run/s6/container_environment/{name}") as f:
                tok = f.read().strip()
                if tok:
                    return tok
        except OSError:
            pass
    return ""


def mqtt_settings(opts: dict) -> Optional[dict]:
    """Explicit options win; otherwise ask the Supervisor for the Mosquitto service login."""
    if opts.get("mqtt_host"):
        log.info("MQTT settings from add-on options: %s", opts["mqtt_host"])
        return dict(host=opts["mqtt_host"], port=int(opts.get("mqtt_port") or 1883),
                    username=opts.get("mqtt_user", ""), password=opts.get("mqtt_password", ""))
    tok = supervisor_token()
    if not tok:
        log.error("No Supervisor token available; set mqtt_host/mqtt_user/mqtt_password in the add-on options")
        return None
    req = urllib.request.Request("http://supervisor/services/mqtt", headers={"Authorization": "Bearer " + tok})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.load(r)["data"]
    except Exception as e:
        log.error("Supervisor MQTT service lookup failed (%s). Is the Mosquitto add-on installed? "
                  "Otherwise set mqtt_host/mqtt_user/mqtt_password in the add-on options.", e)
        return None
    log.info("MQTT settings from Supervisor: %s:%s user=%s", d["host"], d["port"], d.get("username"))
    return dict(host=d["host"], port=int(d["port"]), username=d.get("username", ""), password=d.get("password", ""))


def ntp_query(host: str, timeout: float = 2.0) -> Dict[str, Any]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        t1 = time.time()
        pkt = b"\x23" + 39 * b"\0" + struct.pack("!II", int(t1 + NTP_EPOCH), int((t1 % 1) * 2 ** 32))
        sock.sendto(pkt, (host, 123))
        d, _ = sock.recvfrom(512)
        t4 = time.time()
    finally:
        sock.close()
    if len(d) < 48:
        raise ValueError("short NTP reply")

    def ts(off):
        a, b = struct.unpack("!II", d[off:off + 8])
        return a - NTP_EPOCH + b / 2 ** 32 if a else None
    stratum = d[1]
    ref = d[12:16]
    refid = ref.decode("ascii", "replace").strip("\0") if stratum <= 1 else ".".join(map(str, ref))
    rd, rdisp = struct.unpack("!II", d[4:12])
    t2, t3 = ts(32), ts(40)
    return dict(leap=d[0] >> 6, stratum=stratum, precision=struct.unpack("!b", d[3:4])[0],
                root_delay=rd / 65536, root_disp=rdisp / 65536, refid=refid,
                offset=((t2 - t1) + (t3 - t4)) / 2, delay=(t4 - t1) - (t3 - t2))


def snmp_get(host: str, community: str) -> Optional[Dict[str, Any]]:
    keys = list(SNMP_OIDS)
    oids = [f"{SNMP_BASE}.{SNMP_OIDS[k]}.0" for k in keys] + ["1.3.6.1.2.1.1.3.0"]
    try:
        out = subprocess.run(["snmpget", "-v2c", "-c", community, "-t", "3", "-r", "1", "-Oqv", host] + oids,
                             capture_output=True, text=True, timeout=15)
    except Exception as e:
        log.warning("snmpget failed: %s", e)
        return None
    vals = [v.strip().strip('"') for v in out.stdout.splitlines()]
    if out.returncode != 0 or len(vals) != len(oids):
        log.warning("SNMP no/short reply (rc=%s): %s", out.returncode, out.stderr.strip()[:120])
        return None
    d: Dict[str, Any] = dict(zip(keys, vals))
    d["uptime"] = vals[-1]
    m = re.search(r"(\d+)", d.get("gps_health", ""))
    d["health_code"] = int(m.group(1)) if m else None
    d["mode"] = d.get("gps_mode", "").split(":")[-1].strip()
    try:
        d["fly"] = int(d.get("fly_period") or 0)
    except ValueError:
        d["fly"] = 0
    # gpsSatlist: "N,sv,flag,dBW,..."  (C = used in the timing solution, T = tracked only)
    toks = [t for t in re.split(r"[,\s]+", d.get("satlist", "")) if t]
    sats = []
    if toks and toks[0].isdigit():
        d["sat_count"] = int(toks[0])
        rest = toks[1:]
        for i in range(0, len(rest) - 2, 3):
            grp = rest[i:i + 3]
            sv = next((g for g in grp if g.isdigit()), None)
            flag = next((g for g in grp if g in ("C", "T")), "")
            sig = next((float(g) for g in grp if re.fullmatch(r"-?\d+(\.\d+)?", g) and g != sv), None)
            sats.append(dict(sv=sv, used=flag == "C", dbw=sig))
    else:
        d["sat_count"] = None
    d["sats"] = sats
    return d


def lan_ports(host: str) -> list:
    try:
        with urllib.request.urlopen(f"http://{host}/", timeout=5) as r:
            return re.findall(r'id="status\d+">(\w+)', r.read().decode("latin-1"))
    except Exception:
        return []


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class Monitor:
    def __init__(self, opts: dict):
        self.opts = opts
        self.host = opts["host"]
        self.fails = 0
        self.ntp = None
        self.snmp = None
        self.clients: Dict[str, Any] = {}
        self.ports: list = []

    def poll(self, full: bool) -> None:
        try:
            self.ntp, self.fails = ntp_query(self.host), 0
        except Exception as e:
            self.fails += 1
            log.warning("NTP query to %s failed (%d): %s", self.host, self.fails, e)
        snmp = snmp_get(self.host, self.opts["snmp_community"])
        if snmp:
            self.snmp = snmp
        if full:
            for c in self.opts.get("clients", []):
                try:
                    r = ntp_query(c["host"], 1.5)
                    self.clients[c["name"]] = dict(host=c["host"], stratum=r["stratum"], refid=r["refid"],
                                                   state="on GPS" if r["refid"] == self.host else f"via {r['refid']}")
                except Exception:
                    self.clients[c["name"]] = dict(host=c["host"], stratum=None, refid=None, state="no reply")
            self.ports = lan_ports(self.host)

    def state(self) -> Dict[str, Any]:
        srv, sn = self.ntp, self.snmp
        reachable = srv is not None and self.fails < 2
        locked = reachable and srv["stratum"] == 1 and srv["leap"] != 3 and (sn is None or sn["health_code"] == 8)
        st: Dict[str, Any] = dict(reachable="on" if reachable else "off", locked="on" if locked else "off",
                                  host=self.host, lan_ports=self.ports)
        if sn:
            hc = sn["health_code"]
            st.update(gps_receiver=GPS_HEALTH.get(hc, sn.get("gps_health")), gps_health_code=hc,
                      receiver_mode={"Hold": "Position hold", "Survey": "Surveying"}.get(sn["mode"], sn["mode"]),
                      timing_status=sn.get("timing_status"), holdover_s=sn["fly"],
                      holdover="on" if sn["fly"] else "off",
                      satellites=sn["sat_count"] if sn["sat_count"] else ("not reported" if hc == 8 else 0),
                      sats=sn["sats"], utc_offset=num(sn.get("utc_offset")), ntp_error=sn.get("ntp_error"),
                      osc_ppm=num((sn.get("freq") or "").split()[0]) if sn.get("freq") else None,
                      firmware=sn.get("firmware"), model=sn.get("model"), serial=sn.get("serial"),
                      uptime=sn.get("uptime"))
        if srv:
            st.update(stratum=srv["stratum"], refid=srv["refid"], leap=LEAP[srv["leap"]], leap_code=srv["leap"],
                      precision_us=round(2.0 ** srv["precision"] * 1e6, 2),
                      root_disp_ms=round(srv["root_disp"] * 1000, 3))
            if reachable:
                st["ha_offset_us"] = round(srv["offset"] * 1e6, 1)
        if not reachable:
            st["status"] = "Unreachable"
        elif sn and sn["fly"]:
            st["status"] = "Holdover"
        elif locked:
            st["status"] = "Locked"
        else:
            st["status"] = "Not locked"
        st["clients_ok"] = sum(1 for c in self.clients.values() if c["state"] == "on GPS")
        st["clients"] = self.clients
        return st


def publish_discovery(client: mqtt.Client, host: str, sn: Optional[dict]) -> None:
    uid = "s250_" + host.replace(".", "_")
    dev = {"identifiers": [uid], "name": "SyncServer S250", "manufacturer": "Symmetricom",
           "model": "SyncServer S250", "configuration_url": f"http://{host}/"}
    if sn:
        dev["serial_number"] = sn.get("serial")
        dev["sw_version"] = sn.get("firmware")
    for comp, oid, name, key, extra in ENTITIES:
        cfg = {"name": name, "unique_id": f"{uid}_{oid}", "device": dev,
               "state_topic": f"{BASE}/state", "value_template": "{{ value_json.%s }}" % key,
               "availability_topic": f"{BASE}/status"}
        for k, v in extra.items():
            cfg[k] = v if v is not None else f"{BASE}/state"   # None = use the full state as attributes
        client.publish(f"homeassistant/{comp}/{uid}/{oid}/config", json.dumps(cfg), retain=True)
    log.info("Published discovery for %d entities (%s)", len(ENTITIES), uid)


def main() -> None:
    opts = load_options()
    logging.getLogger().setLevel(getattr(logging, str(opts.get("log_level", "INFO")).upper(), logging.INFO))
    interval = int(opts.get("poll_interval", 30))
    mon = Monitor(opts)

    ms = mqtt_settings(opts)
    while ms is None:
        time.sleep(60)
        ms = mqtt_settings(opts)
    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="s250_ntp_monitor")
    if ms["username"]:
        client.username_pw_set(ms["username"], ms["password"])
    client.will_set(f"{BASE}/status", "offline", retain=True)

    def on_connect(c, userdata, flags, reason_code, properties=None):
        log.info("MQTT connected: %s", reason_code)
        c.publish(f"{BASE}/status", "online", retain=True)

    def on_disconnect(c, userdata, flags, reason_code, properties=None):
        log.warning("MQTT disconnected: %s", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(ms["host"], ms["port"], 60)
    client.loop_start()

    have_device_info = False
    n = 0
    while True:
        mon.poll(full=(n % max(1, 60 // interval) == 0))
        if n == 0 or (mon.snmp and not have_device_info):
            publish_discovery(client, mon.host, mon.snmp)
            have_device_info = mon.snmp is not None
        st = mon.state()
        client.publish(f"{BASE}/state", json.dumps(st), retain=True)
        client.publish(f"{BASE}/clients", json.dumps(st["clients"]), retain=True)
        log.debug("state: %s", st)
        n += 1
        time.sleep(interval)


if __name__ == "__main__":
    main()

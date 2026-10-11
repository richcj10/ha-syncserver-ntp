# SyncServer NTP Monitor

Polls a Symmetricom SyncServer (S250/S2xx) GPS time server and publishes it to Home
Assistant as the **SyncServer S250** MQTT device.

- **SNMP (v2c, read community)**: GPS receiver health, receiver mode, timing status,
  holdover, satellites (when reported), GPS-UTC offset, oscillator offset, firmware.
- **NTP**: reachability, stratum, reference, leap second, root dispersion, and HA's own
  clock offset against the GPS clock.
- **Clients**: every minute, each listed device is queried over NTP to confirm it follows
  the GPS clock (`Devices on GPS Time`, with per-device detail as attributes).

MQTT credentials come from the Mosquitto add-on automatically (`services: mqtt:need`);
set `mqtt_host`/`mqtt_user`/`mqtt_password` only to use a different broker.

The `Status` sensor carries the full state as attributes (used by the RackView display).
The GPS antenna position is deliberately not published.

## Install

1. Home Assistant: **Settings → Add-ons → Add-on Store → ⋮ → Repositories**, add
   `https://github.com/richcj10/ha-syncserver-ntp`.
2. Install **SyncServer NTP Monitor**, set its options on the **Configuration** tab, and start it.
3. Requires the Mosquitto broker add-on (MQTT credentials are taken from it automatically);
   to use another broker, set `mqtt_host`, `mqtt_user` and `mqtt_password`.

Part of [RackViewer](https://github.com/richcj10/RackViewer), a rack status display, which
reads this add-on's entities from Home Assistant.

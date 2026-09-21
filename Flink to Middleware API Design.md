# Flink to Middleware API Design

This document shows how our Flink SQL pipeline can talk to the middleware.

```
safecast-measurements -> Flink SQL -> safecast-map-points -> middleware -> frontend
```

## Topic for the Middleware

The middleware should consume this Kafka topic:

```
safecast-map-points
```

This topic is written with Flink's `upsert-kafka` connector. That means the
message key matters: when a new message arrives with the same key, it represents
an update for the same map marker.

## Message Key

The key is the marker id:

```
{
  "marker_id": "sensor-108:cell-51.981:9.235"
}
```

The middleware should use this value as the id of the marker on the map. If
another message arrives with the same `marker_id`, the existing marker should be
updated instead of adding a second marker.

`sensor_id` is still included in the message value, but it is the original
Safecast device/sensor identity. `marker_id` is the update key for the map.

## Message Value

The value looks like this:

```
{
  "marker_id": "sensor-108:cell-51.981:9.235",
  "sensor_id": "108",
  "captured_at": "2026-04-01 13:07:11",
  "uploaded_at": "2026-04-01 13:08:12.59642",
  "location": {
    "name": "Bad Pyrmont, DE",
    "lat": 51.9807,
    "lon": 9.2345
  },
  "cpm": 15.0,
  "status": "safe",
  "color": "#2e7d32",
  "action": "upsert"
}
```

Above is only an example. The same structure is used for every
sensor.

The important fields for the middleware are:

| Field | Meaning |
| --- | --- |
| `marker_id` | Current marker/update key. |
| `sensor_id` | Original Safecast device/sensor id. |
| `captured_at` | Time when the measurement was taken. |
| `uploaded_at` | Time when Safecast uploaded the measurement. |
| `location` | Name, latitude, and longitude for the marker. |
| `cpm` | Radiation value in counts per minute. |
| `status` | Flink classification: `safe`, `warning`, or `critical`. |
| `color` | Marker color chosen by Flink. |
| `action` | Currently always `upsert`. |

## Middleware Usage

The middleware should:

- consume `safecast-map-points`;
- decode the JSON key and value;
- forward the value to the frontend through the WebSocket;
- use `marker_id` as the marker key;
- `action = "upsert"` means "update this marker".

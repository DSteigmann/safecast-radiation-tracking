// All filtering/classification/
// Aggregation happens server-side (middleware or Flink).

const WARNING_CPM = 100;
const CRITICAL_CPM = 200;

// Configure via VITE_WS_URL. For local testing point this at the CSV replay
// stream (`/testing`); deployment uses the live Kafka stream (`/ws`).
const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8080/ws";
const PRODUCER_API_URL =
  import.meta.env.VITE_PRODUCER_API_URL || "http://localhost:8042/producer";
const DEFAULT_PRODUCER_SPEED = 1000;

document.addEventListener("DOMContentLoaded", () => {
  // We center on Europe: the testing CSV streams European measurements first, so
  // points appear immediately. The data is global, just zoom to explore
  const map = new ol.Map({
    target: "map",
    layers: [
      new ol.layer.Tile({
        source: new ol.source.OSM(), // OpenStreetMap tiles
      }),
    ],
    view: new ol.View({
      center: ol.proj.fromLonLat([10.0, 50.0]), // central Europe
      zoom: 5,
    }),
  });

  function fallbackColorForCpm(value) {
    if (value > CRITICAL_CPM) return "#c62828";
    if (value >= WARNING_CPM) return "#f9a825";
    return "#2e7d32";
  }

  // Marker size for scaling with the zoom.
  // The blobs size depends on zoom now: 
  // between 6 and 16, depending on the zoom level.
  function radiusForZoom(zoom) {
    return Math.max(6, Math.min(16, 19 - zoom));
  }

  // Which H3 resolution the middleware serves for a given zoom. 
  // This is a mirror of the ZoomToH3Resolution in the Go middleware (internal/utility/viewport.go)
  // It detect when a zoom switches to a different set of cells, so that we can remove them all.
  function resolutionForZoom(zoom) {
    if (zoom < 6) return 2;
    if (zoom < 10) return 4;
    if (zoom < 14) return 6;
    if (zoom < 17) return 8;
    return 12;
  }

  const vectorSource = new ol.source.Vector();
  const vectorLayer = new ol.layer.Vector({
    source: vectorSource,
    style: function (feature) {
      // The middleware/Flink classifies the point into (green/amber/red).
      // We simply render that color here that we have.
      let color = feature.get("serverColor");
      // but if the server has not done it yet, then we simply want to do it ourselves.
      if (!color) {
        color = fallbackColorForCpm(Number(feature.get("value")) || 0);
      }
      // Make the bubbles transparent so overlapping bubbles stay readable,
      // with a solid outline
      // ol.color.asArray turns "#2e7d32" into the array[r, g, b, a]
      // and lower the alpha to ~0.6, which makes it transparent
      const rgb = ol.color.asArray(color);
      const fillColor = [rgb[0], rgb[1], rgb[2], 0.6];

      // Base radius depends on the current zoom level (bigger when zoomed out).
      const baseRadius = radiusForZoom(map.getView().getZoom());

      // A cell that just arrived as an update_point shows a brief cyan glow up.
      const now = Date.now();
      const highlightUntil = feature.get("highlightUntil") || 0;
      const isNew = highlightUntil > now;

      const point = new ol.style.Style({
        image: new ol.style.Circle({
          radius: isNew ? baseRadius + 2 : baseRadius,
          fill: new ol.style.Fill({
            color: isNew ? [rgb[0], rgb[1], rgb[2], 0.9] : fillColor,
          }),
          stroke: new ol.style.Stroke({
            color: isNew ? "#00e5ff" : color,
            width: isNew ? 3 : 1.5,
          }),
        }),
      });

      if (!isNew) return point;

      // A ring that expands and fades again for new points coming in:
      // that is a quick ping that stops on its own (remaining 1 -> 0).
      const remaining = (highlightUntil - now) / HIGHLIGHT_MS;
      const halo = new ol.style.Style({
        image: new ol.style.Circle({
          radius: baseRadius + 16 * (1 - remaining),
          fill: new ol.style.Fill({ color: [0, 229, 255, 0.4 * remaining] }),
        }),
      });
      return [halo, point];
    },
  });
  map.addLayer(vectorLayer);

  // Live-update glowup: when a cell arrives as an update_point it should do one short
  // "ping" (in the cyan color).
  const HIGHLIGHT_MS = 900; // how long a single ping is visible
  const REPULSE_MS = 4000; // min time difference before the same cell can ping again
  let hadActiveHighlights = false;

  function highlightFeature(feature) {
    const now = Date.now();
    if (now < (feature.get("highlightBlockedUntil") || 0)) return; // in cooldown, stay quiet
    feature.set("highlightUntil", now + HIGHLIGHT_MS);
    feature.set("highlightBlockedUntil", now + REPULSE_MS);
    feature.changed(); // render the ping immediately
  }

  setInterval(() => {
    const now = Date.now();
    let active = 0;
    vectorSource.forEachFeature((f) => {
      if ((f.get("highlightUntil") || 0) > now) active++;
    });
    // Re-render while a ping is animating, plus one final frame once it ends.
    if (active > 0 || hadActiveHighlights) vectorLayer.changed();
    hadActiveHighlights = active > 0;
  }, 60);

  const notifications = document.getElementById("notifications");
  const notificationsToggle = document.getElementById("notifications-toggle");
  const notificationsCount = document.getElementById("notifications-count");
  const notificationsList = document.getElementById("notifications-list");
  const notificationItems = [];
  const maxNotifications = 100;
  let totalNotificationCount = 0;

  if (notifications && notificationsToggle) {
    notificationsToggle.addEventListener("click", () => {
      const collapsed = notifications.classList.toggle("collapsed");
      notificationsToggle.setAttribute("aria-expanded", String(!collapsed));
    });
  }

  function shortNotificationTime(value) {
    const d = value ? new Date(String(value).replace(" ", "T")) : new Date();
    if (Number.isNaN(d.getTime())) return String(value ?? "");
    const now = new Date();
    const today =
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate();
    const time = d.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    });
    if (today) return time;
    return (
      d.toLocaleDateString([], { month: "numeric", day: "numeric" }) +
      " " +
      time
    );
  }

  function normalizeNotification(raw) {
    const payload = raw.payload || raw;
    const location = payload.location || {};
    const lat = location.lat ?? payload.lat;
    const lon = location.lon ?? payload.lon;
    const cpm = payload.cpm ?? payload.radiation ?? "unknown";
    const status =
      payload.status || (Number(cpm) > CRITICAL_CPM ? "critical" : "warning");
    const locationName =
      location.name || payload.location_name || "Unknown location";

    return {
      id: `${raw.topic || "notification"}-${raw.partition ?? 0}-${raw.offset ?? Date.now()}`,
      status,
      cpm,
      locationName,
      lat,
      lon,
      capturedAt: payload.captured_at || payload.capturedAt || null,
    };
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function renderNotifications() {
    if (!notificationsList || !notificationsCount) return;
    notificationsCount.textContent = String(totalNotificationCount);

    if (notificationItems.length === 0) {
      notificationsList.innerHTML =
        '<p class="notifications-empty">No hotspot notifications yet.</p>';
      return;
    }

    notificationsList.innerHTML = notificationItems
      .map((item) => {
        const hasCoords =
          item.lat !== null &&
          item.lat !== undefined &&
          item.lon !== null &&
          item.lon !== undefined;
        const dataAttrs = hasCoords
          ? `data-lat="${escapeHtml(item.lat)}" data-lon="${escapeHtml(item.lon)}"`
          : "";
        const cpmDisplay = Number.isFinite(Number(item.cpm))
          ? Number(item.cpm).toLocaleString(undefined, {
              maximumFractionDigits: 0,
            }) + " CPM"
          : "? CPM";
        const hasName =
          item.locationName && item.locationName !== "Unknown location";
        const location = hasName
          ? escapeHtml(item.locationName)
          : hasCoords
            ? `${Number(item.lat).toFixed(3)}, ${Number(item.lon).toFixed(3)}`
            : "Unknown location";
        return `
          <article class="notification-item" ${dataAttrs}>
            <div class="notification-row">
              <span class="notification-cpm">${escapeHtml(cpmDisplay)}</span>
              <time class="notification-time">${escapeHtml(shortNotificationTime(item.capturedAt))}</time>
            </div>
            <div class="notification-location">${location}</div>
          </article>
        `;
      })
      .join("");
  }

  if (notificationsList) {
    notificationsList.addEventListener("click", (e) => {
      const article = e.target.closest("article.notification-item[data-lat]");
      if (!article) return;
      const lat = parseFloat(article.dataset.lat);
      const lon = parseFloat(article.dataset.lon);
      if (Number.isNaN(lat) || Number.isNaN(lon)) return;
      map.getView().animate({
        center: ol.proj.fromLonLat([lon, lat]),
        zoom: 12,
        duration: 600,
      });
    });
  }

  function addNotification(raw) {
    const notification = normalizeNotification(raw);
    totalNotificationCount += 1;
    notificationItems.unshift(notification);
    if (notificationItems.length > maxNotifications) {
      notificationItems.length = maxNotifications;
    }
    renderNotifications();
  }

  renderNotifications();

  // Color legend on the top-right).
  // We want it to be in the header to open/close it.
  // Starts collapsed

  const legend = document.getElementById("legend");
  const legendToggle = document.getElementById("legend-toggle");
  const legendTabs = document.querySelectorAll(".legend-tab");
  const legendPanels = document.querySelectorAll(".legend-panel");

  function isProducerSettingsOpen() {
    const producerPanel = document.getElementById("legend-panel-producer");
    return Boolean(
      producerPanel &&
      producerPanel.classList.contains("active") &&
      !producerPanel.hidden,
    );
  }

  if (legend && legendToggle) {
    legendToggle.addEventListener("click", () => {
      const collapsed = legend.classList.toggle("collapsed");
      legendToggle.setAttribute("aria-expanded", String(!collapsed));

      if (!collapsed && isProducerSettingsOpen()) {
        fetchProducerSpeed();
      }
    });
  }

  legendTabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      legendTabs.forEach((item) => {
        item.classList.toggle("active", item === tab);
        item.setAttribute("aria-selected", String(item === tab));
      });

      legendPanels.forEach((panel) => {
        const isActive = panel.id === tab.getAttribute("aria-controls");
        panel.classList.toggle("active", isActive);
        panel.hidden = !isActive;
      });

      if (tab.getAttribute("aria-controls") === "legend-panel-producer") {
        fetchProducerSpeed();
      }
    });
  });

  const notificationThreshold = document.getElementById("notification-threshold");
  const notificationThresholdInput = document.getElementById(
    "notification-threshold-input",
  );
  const notificationThresholdStatus = document.getElementById(
    "notification-threshold-status",
  );
  const producerSpeed = document.getElementById("producer-speed");
  const producerSpeedInput = document.getElementById("producer-speed-input");
  const producerSentMb = document.getElementById("producer-sent-mb");
  const producerReset = document.getElementById("producer-reset");
  const producerStatus = document.getElementById("producer-status");
  let producerUpdateTimeout = null;
  let notificationUpdateTimeout = null;
  let selectedNotificationThreshold = 200;

  function setNotificationThresholdStatus(message, isError = false) {
    if (!notificationThresholdStatus) return;
    notificationThresholdStatus.textContent = message;
    notificationThresholdStatus.classList.toggle("error", isError);
  }

  function setNotificationThresholdValue(value) {
    const clamped = Math.max(
      200,
      Math.min(10000, Math.round(Number(value) || 200)),
    );
    selectedNotificationThreshold = clamped;
    if (notificationThreshold) notificationThreshold.value = String(clamped);
    if (notificationThresholdInput) {
      notificationThresholdInput.value = String(clamped);
    }
    return clamped;
  }

  function sendNotificationThreshold(value) {
    const threshold = setNotificationThresholdValue(value);
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      setNotificationThresholdStatus(
        `Notify at or above ${threshold.toLocaleString()} CPM when connected.`,
      );
      return;
    }

    socket.send(
      JSON.stringify({
        type: "notification_settings",
        critical_cpm: threshold,
      }),
    );
    setNotificationThresholdStatus("Updating notification threshold...");
  }

  if (notificationThreshold) {
    notificationThreshold.addEventListener("input", () => {
      const threshold = setNotificationThresholdValue(
        notificationThreshold.value,
      );
      window.clearTimeout(notificationUpdateTimeout);
      notificationUpdateTimeout = window.setTimeout(() => {
        sendNotificationThreshold(threshold);
      }, 250);
    });
  }

  if (notificationThresholdInput) {
    notificationThresholdInput.addEventListener("input", () => {
      const value = Math.max(
        200,
        Math.min(10000, Number(notificationThresholdInput.value) || 200),
      );
      selectedNotificationThreshold = value;
      if (notificationThreshold) notificationThreshold.value = String(value);
    });
    notificationThresholdInput.addEventListener("change", () => {
      window.clearTimeout(notificationUpdateTimeout);
      sendNotificationThreshold(notificationThresholdInput.value);
    });
  }

  function setProducerStatus(message, isError = false) {
    if (!producerStatus) return;
    producerStatus.textContent = message;
    producerStatus.classList.toggle("error", isError);
  }

  function setProducerSpeedValue(value) {
    const clamped = Math.max(
      0,
      Math.min(1000000, Math.round(Number(value) || 0)),
    );
    if (producerSpeed) producerSpeed.value = String(clamped);
    if (producerSpeedInput) producerSpeedInput.value = String(clamped);
  }

  function setProducerSentMb(value) {
    if (!producerSentMb) return;
    const sentMb = Number(value) || 0;
    producerSentMb.textContent = `${sentMb.toLocaleString(undefined, {
      maximumFractionDigits: 2,
    })} MB`;
  }

  async function parseProducerResponse(response) {
    const text = await response.text();
    try {
      return JSON.parse(text);
    } catch (_error) {
      return { submission_speed: Number(text), sent_mb: null };
    }
  }

  function updateProducerSettings(settings) {
    setProducerSpeedValue(settings.submission_speed ?? settings.speed ?? 0);
    if (settings.sent_mb !== null && settings.sent_mb !== undefined) {
      setProducerSentMb(settings.sent_mb);
    }
  }

  async function fetchProducerSpeed() {
    try {
      const response = await fetch(PRODUCER_API_URL);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const settings = await parseProducerResponse(response);
      updateProducerSettings(settings);
      setProducerStatus("Connected to producer.");
    } catch (error) {
      setProducerStatus(`Could not load producer settings.`, true);
      console.error("Producer settings error:", error);
    }
  }

  async function updateProducerSpeed(value) {
    try {
      const response = await fetch(PRODUCER_API_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ speed: Number(value) }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const settings = await parseProducerResponse(response);
      updateProducerSettings(settings);
      setProducerStatus(
        `Producer speed set to ${Number(settings.submission_speed ?? value).toLocaleString()}`,
      );
    } catch (error) {
      setProducerStatus("Could not update producer speed.", true);
      console.error("Producer speed update error:", error);
    }
  }

  async function resetProducerSpeed() {
    try {
      const response = await fetch(PRODUCER_API_URL, { method: "DELETE" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const settings = await parseProducerResponse(response);
      updateProducerSettings({
        submission_speed: settings.submission_speed ?? DEFAULT_PRODUCER_SPEED,
        sent_mb: settings.sent_mb,
      });
      setProducerStatus("Producer speed reset.");
    } catch (error) {
      setProducerStatus("Could not reset producer speed.", true);
      console.error("Producer speed reset error:", error);
    }
  }

  if (producerSpeed) {
    producerSpeed.addEventListener("input", () => {
      if (producerSpeedInput) producerSpeedInput.value = producerSpeed.value;
      window.clearTimeout(producerUpdateTimeout);
      producerUpdateTimeout = window.setTimeout(() => {
        updateProducerSpeed(producerSpeed.value);
      }, 250);
    });
  }

  if (producerSpeedInput) {
    producerSpeedInput.addEventListener("input", () => {
      const v = Math.max(
        0,
        Math.min(1000000, Number(producerSpeedInput.value) || 0),
      );
      if (producerSpeed) producerSpeed.value = String(v);
    });
    producerSpeedInput.addEventListener("change", () => {
      const v = Math.max(
        0,
        Math.min(1000000, Math.round(Number(producerSpeedInput.value) || 0)),
      );
      producerSpeedInput.value = String(v);
      if (producerSpeed) producerSpeed.value = String(v);
      window.clearTimeout(producerUpdateTimeout);
      updateProducerSpeed(v);
    });
  }

  if (producerReset) {
    producerReset.addEventListener("click", resetProducerSpeed);
  }

  fetchProducerSpeed();

  // popup overlay
  const popupElement = document.getElementById("popup");
  const popupContent = document.getElementById("popup-content");
  const popupOverlay = new ol.Overlay({
    element: popupElement,
    positioning: "bottom-center",
    offset: [0, -10],
  });
  map.addOverlay(popupOverlay);

  // Show popup on hover
  map.on("pointermove", function (evt) {
    const feature = map.forEachFeatureAtPixel(evt.pixel, function (feat) {
      return feat;
    });
    if (feature) {
      const coords = feature.getGeometry().getCoordinates();
      popupOverlay.setPosition(coords);
      let html = "";

      const isComposite = feature.get("isComposite"); // future flag
      if (isComposite) {
        // Composite point: list all sub-points
        const subPoints = feature.get("subPoints"); // array of {name, cpm}
        html = "<b>Cluster (" + subPoints.length + " sensors)</b><br>";
        subPoints.forEach((p) => {
          html += `${p.name}: ${p.cpm} CPM<br>`;
        });
      } else {
        // Single point
        const locationName = feature.get("locationName");
        let name =
          locationName && locationName !== "Unknown" ? locationName : null;
        if (!name) {
          const lonLat = ol.proj.toLonLat(
            feature.getGeometry().getCoordinates(),
          );
          name = lonLat[1].toFixed(4) + ", " + lonLat[0].toFixed(4);
        }

        const cpm = feature.get("value");
        html = `<b>${name}</b><br>CPM: ${cpm}`;
      }

      popupContent.innerHTML = html;
      popupElement.style.display = "block";
    } else {
      popupElement.style.display = "none";
    }
  });

  //Goal: make frontend independent of the raw format
  function normalizeMessage(raw) {
    // Aggregated H3/cell message from the middleware.
    // snapshot_point: already in the topic for this viewport
    // update_point: live pushing of a new/changed cell when the viewport is open
    if (
      (raw.type === "snapshot_point" || raw.type === "update_point") &&
      raw.payload?.center
    ) {
      const p = raw.payload;

      return {
        key: p.marker_id || p.h3,
        lat: p.center.lat,
        lon: p.center.lon,
        value: p.avg_cpm,
        capturedAt: p.captured_at || null,
        locationName: p.h3 || "H3 cell",
        serverColor: p.color || null,
        status: p.status || null,
        h3: p.h3 || null,
        action: p.action || "upsert",
      };
    }

    // Raw sensor message
    if (raw.location?.lat !== undefined && raw.location?.lon !== undefined) {
      const key = raw.sensor_id
        ? raw.sensor_id
        : `${raw.location.lat},${raw.location.lon}`;

      return {
        key,
        lat: raw.location.lat,
        lon: raw.location.lon,
        value: raw.radiation !== undefined ? raw.radiation : raw.cpm,
        capturedAt: raw.captured_at,
        locationName: raw.location.name || "Unknown",
        serverColor: raw.color || null,
        status: raw.status || null,
        h3: raw.h3 || null,
        action: raw.action || "upsert",
      };
    }

    return null;
  }

  // Viewport reporting + off-screen culling

  // Viewport for: Read the lon/lat bounds of what is currently on screen.

  function getViewportBounds() {
    const view = map.getView();
    const size = map.getSize();
    if (!size) return null;

    const [minX, minY, maxX, maxY] = view.calculateExtent(size);
    const [west, south] = ol.proj.toLonLat([minX, minY]);
    const [east, north] = ol.proj.toLonLat([maxX, maxY]);
    const bounds = {
      north,
      east,
      south,
      west,
      zoom: Math.round(view.getZoom()),
    };

    console.log("Viewport bounds:", bounds);
    return { north, east, south, west, zoom: Math.round(view.getZoom()) };
  }

  // Tell the middleware which region we are looking at at the moment
  function sendViewport() {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    const bounds = getViewportBounds();
    if (!bounds) return;

    socket.send(
      JSON.stringify({
        type: "viewport",
        zoom: bounds.zoom,
        bounds: {
          north: bounds.north,
          south: bounds.south,
          east: bounds.east,
          west: bounds.west,
        },
      }),
    );
  }

  // Here we want to drop markers that are outside of our screen, so we never
  // have too many points on the map for performance.
  function cullOutsideViewport() {
    // get the current size of the map
    const size = map.getSize();
    // if size is null, stop
    if (!size) return;

    // view.calculateExtent() returns the bounding box of the visible map
    // The result is an array [minX, minY, maxX, maxY]
    const extent = map.getView().calculateExtent(size);

    // iterate over all existing markers (stored by key).
    // Using Object.keys() gives us an array of the keys.
    for (const key of Object.keys(markers)) {
      const feature = markers[key];
      const coord = feature.getGeometry().getCoordinates();

      // ol.extent.containsCoordinate() checks whether a coordinate
      // lies inside the given extent (bounding box)
      if (!ol.extent.containsCoordinate(extent, coord)) {
        vectorSource.removeFeature(feature);
        delete markers[key];
      }
    }
  }

  // when the zoom level changes remove all blobs.
  // Reason: the server serves new H3 sells with different keys now. 
  function clearAllMarkers() {
    vectorSource.clear();
    for (const key of Object.keys(markers)) delete markers[key];
  }

  // Remember the H3 resolution (not the raw zoom) so small zoom nudges that stay
  // within the same resolution band do not wipe the map.
  let lastResolution = resolutionForZoom(Math.round(map.getView().getZoom()));

  // Is fired every time after the zoom once the movement ends, then asks the server for
  // the new region.
  map.on("moveend", () => {
    const resolution = resolutionForZoom(Math.round(map.getView().getZoom()));
    if (resolution !== lastResolution) {
      // Resolution changed: delete all blobs.
      clearAllMarkers();
      lastResolution = resolution;
    } else {
      // Same resolution: only delete
      // the points that are off the screen.
      cullOutsideViewport();
    }
    sendViewport();
  });

  let socket = null;

  const markers = {}; // e.g., key -> ol.Feature

  function connectWebSocket() {
    socket = new WebSocket(WS_URL);

    socket.onopen = () => {
      // Here we tell the server what we are looking at at the moment. From the start.
      sendViewport();
      sendNotificationThreshold(selectedNotificationThreshold);
    };

    socket.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);

        if (msg.type === "notification") {
          addNotification(msg);
          return;
        }

        if (msg.type === "notification_settings") {
          const threshold = setNotificationThresholdValue(msg.critical_cpm);
          setNotificationThresholdStatus(
            `Notify at or above ${threshold.toLocaleString()} CPM.`,
          );
          return;
        }

        // We highlight the new points that are update_point 
        // but we keep the snapshot_point to render normally.
        const isLive = msg.type === "update_point";

        const data = normalizeMessage(msg);

        if (!data) {
          console.warn("Ignoring non-sensor message:", msg);
          return;
        }

        const {
          key,
          lat,
          lon,
          value,
          capturedAt,
          serverColor,
          status,
          locationName,
          h3,
        } = data;
        const coords = ol.proj.fromLonLat([lon, lat]);

        const existing = markers[key];
        let feature;
        if (existing) {
          // update existing marker
          feature = existing;
          feature.setGeometry(new ol.geom.Point(coords));
          feature.set("value", value);
          feature.set("capturedAt", capturedAt);
          feature.set("serverColor", serverColor);
          feature.set("status", status);
          feature.set("locationName", locationName);
          feature.set("h3", h3);
        } else {
          // create new marker
          feature = new ol.Feature({
            geometry: new ol.geom.Point(coords),
            value,
            capturedAt,
            serverColor,
            status,
            locationName,
            h3,
          });
          feature.setId(key);
          vectorSource.addFeature(feature);
          markers[key] = feature;
        }

        // Highlight points that arrived live via update_point.
        if (isLive) highlightFeature(feature);
      } catch (err) {
        console.error("Error parsing message:", err, event.data);
      }
    };

    socket.onerror = (err) => {
      console.error("Websocket error: ", err);
    };

    socket.onclose = () => {
      console.warn("WebSocket closed, retrying in 3s...");
      setTimeout(connectWebSocket, 3000);
    };
  }

  connectWebSocket();
});

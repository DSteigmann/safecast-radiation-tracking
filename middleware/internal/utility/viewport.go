package utility

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/uber/h3-go/v4"
)

type ViewportBounds struct {
	North float64 `json:"north"`
	East  float64 `json:"east"`
	South float64 `json:"south"`
	West  float64 `json:"west"`
	Zoom  int     `json:"zoom"`
}

func CellsForViewport(north float64, east float64, south float64, west float64, zoom int) ([]string, error) {
	bounds, err := NormalizeViewportBounds(ViewportBounds{
		North: north,
		East:  east,
		South: south,
		West:  west,
		Zoom:  zoom,
	})
	if err != nil {
		return nil, err
	}

	resolution := ZoomToH3Resolution(zoom)
	cellSet := make(map[string]struct{})

	for _, piece := range h3PiecesForViewport(bounds) {
		cells, err := cellsForPiece(piece.North, piece.East, piece.South, piece.West, resolution)
		if err != nil {
			return nil, err
		}

		for _, cell := range cells {
			cellSet[h3.CellToString(cell)] = struct{}{}
		}
	}

	result := make([]string, 0, len(cellSet))
	for cell := range cellSet {
		result = append(result, cell)
	}

	return result, nil
}

// NormalizeViewportBounds converts frontend bounds into the backend's canonical
// representation. For non-world requests, the canonical longitude order is
// east < west. If the frontend sends east > west, west is shifted by +360 so the
// same wrapped viewport is preserved instead of swapped or resized.
func NormalizeViewportBounds(bounds ViewportBounds) (ViewportBounds, error) {
	if bounds.North < bounds.South {
		bounds.North, bounds.South = bounds.South, bounds.North
	}

	if bounds.North > 90 || bounds.North < -90 || bounds.South > 90 || bounds.South < -90 {
		return ViewportBounds{}, fmt.Errorf("invalid latitude bounds")
	}

	// At low zoom the viewport is intentionally treated as the whole map.
	if bounds.Zoom <= 3 {
		bounds.North = 90
		bounds.South = -90
		bounds.West = 180
		bounds.East = -180
		return bounds, nil
	}

	span := bounds.East - bounds.West
	if span < 0 {
		span = -span
	}

	if span >= 360 {
		bounds.West = 180
		bounds.East = -180
		return bounds, nil
	}

	if bounds.East > bounds.West {
		bounds.West += 360
	}

	return bounds, nil
}

// h3PiecesForViewport converts the canonical viewport into one or more H3-safe
// polygons. H3 polygon longitudes are kept inside [-180, 180], so wrapped or
// shifted viewport coordinates are split before calling PolygonToCells.
func h3PiecesForViewport(bounds ViewportBounds) []ViewportBounds {
	const maxLngSpan = 60.0

	// Canonical full-world longitude viewport.
	if bounds.East == -180 && bounds.West == 180 {
		return splitLngRange(bounds.South, bounds.North, -180, 180, bounds.Zoom, maxLngSpan)
	}

	// Canonical wrapped viewport: west is greater than east. Treat it as a
	// continuous interval from west to east+360, then normalize each H3 piece.
	start := bounds.West
	end := bounds.East
	if end < start {
		end += 360
	}

	var pieces []ViewportBounds
	for lng := start; lng < end; lng += maxLngSpan {
		pieceEnd := lng + maxLngSpan
		if pieceEnd > end {
			pieceEnd = end
		}
		pieces = appendNormalizedH3Piece(pieces, bounds.South, bounds.North, lng, pieceEnd, bounds.Zoom)
	}

	return pieces
}

func splitLngRange(south float64, north float64, west float64, east float64, zoom int, maxLngSpan float64) []ViewportBounds {
	var pieces []ViewportBounds
	for lng := west; lng < east; lng += maxLngSpan {
		pieceEnd := lng + maxLngSpan
		if pieceEnd > east {
			pieceEnd = east
		}
		pieces = append(pieces, ViewportBounds{
			North: north,
			East:  clampLng(pieceEnd),
			South: south,
			West:  clampLng(lng),
			Zoom:  zoom,
		})
	}
	return pieces
}

func appendNormalizedH3Piece(pieces []ViewportBounds, south float64, north float64, west float64, east float64, zoom int) []ViewportBounds {
	west = normalizeLng(west)
	east = normalizeLng(east)

	if east < west {
		pieces = append(pieces, ViewportBounds{
			North: north,
			East:  180 - lngEpsilon,
			South: south,
			West:  west,
			Zoom:  zoom,
		})
		pieces = append(pieces, ViewportBounds{
			North: north,
			East:  east,
			South: south,
			West:  -180 + lngEpsilon,
			Zoom:  zoom,
		})
		return pieces
	}

	pieces = append(pieces, ViewportBounds{
		North: north,
		East:  east,
		South: south,
		West:  west,
		Zoom:  zoom,
	})
	return pieces
}

func cellsForPiece(north float64, east float64, south float64, west float64, resolution int) ([]h3.Cell, error) {
	polygon := h3.GeoPolygon{
		GeoLoop: h3.GeoLoop{
			{Lat: south, Lng: west},
			{Lat: south, Lng: east},
			{Lat: north, Lng: east},
			{Lat: north, Lng: west},
			{Lat: south, Lng: west},
		},
	}

	return h3.PolygonToCells(polygon, resolution)
}

const lngEpsilon = 0.000001

func clampLng(lng float64) float64 {
	if lng >= 180 {
		return 180 - lngEpsilon
	}

	if lng <= -180 {
		return -180 + lngEpsilon
	}

	return lng
}

func normalizeLng(lng float64) float64 {
	for lng < -180 {
		lng += 360
	}
	for lng > 180 {
		lng -= 360
	}
	return clampLng(lng)
}

func CellsForViewportRequest(bounds ViewportBounds) ([]string, error) {
	return CellsForViewport(
		bounds.North,
		bounds.East,
		bounds.South,
		bounds.West,
		bounds.Zoom,
	)
}

// This reports whether a (lat, lon) coord. falls inside the
// given viewport bounds. This will be the server-side decision for "is this point on
// the user's screen" so the frontend never has to compute it.
func PointInViewport(lat float64, lon float64, bounds ViewportBounds) bool {
	bounds, err := NormalizeViewportBounds(bounds)
	if err != nil {
		return false
	}

	if bounds.Zoom <= 3 {
		return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
	}

	if lat < bounds.South || lat > bounds.North {
		return false
	}

	start := bounds.West
	end := bounds.East
	if end < start {
		end += 360
	}

	lon = normalizeLng(lon)
	for candidate := lon - 720; candidate <= lon+720; candidate += 360 {
		if candidate >= start && candidate <= end {
			return true
		}
	}

	return false
}

// This will return the H3 cell (as a string) that a coord. belongs to
// at the resolution matching the given map zoom level. This is the spatial key
// for H3-based aggregation across zoom levels.
func CellForPoint(lat float64, lon float64, zoom int) (string, error) {
	resolution := ZoomToH3Resolution(zoom)
	cell, err := h3.LatLngToCell(h3.LatLng{Lat: lat, Lng: lon}, resolution)
	if err != nil {
		return "", err
	}
	return h3.CellToString(cell), nil
}

func ZoomToH3Resolution(zoom int) int {
	switch { // example zoom aggregations, finetuning probably needed
	case zoom < 6:
		return 2
	case zoom < 10:
		return 4
	case zoom < 14:
		return 6
	case zoom < 17:
		return 8
	default:
		return 12
	}
}

func HandleViewportRequest(w http.ResponseWriter, r *http.Request) {
	north, err := strconv.ParseFloat(r.URL.Query().Get("north"), 64)
	if err != nil {
		http.Error(w, "invalid north value", http.StatusBadRequest)
		return
	}

	east, err := strconv.ParseFloat(r.URL.Query().Get("east"), 64)
	if err != nil {
		http.Error(w, "invalid east value", http.StatusBadRequest)
		return
	}

	south, err := strconv.ParseFloat(r.URL.Query().Get("south"), 64)
	if err != nil {
		http.Error(w, "invalid south value", http.StatusBadRequest)
		return
	}

	west, err := strconv.ParseFloat(r.URL.Query().Get("west"), 64)
	if err != nil {
		http.Error(w, "invalid west value", http.StatusBadRequest)
		return
	}

	zoom, err := strconv.Atoi(r.URL.Query().Get("zoom"))
	if err != nil {
		http.Error(w, "invalid zoom value", http.StatusBadRequest)
		return
	}

	cells, err := CellsForViewport(north, east, south, west, zoom)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cells)
}

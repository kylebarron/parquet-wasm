import * as test from "tape";
import * as parquet from "../../pkg/node/arrow2";
import { readFileSync } from "fs";
import { tableFromIPC } from "apache-arrow";

// Path from repo root
const dataDir = "tests/data";
const NATURALEARTH_CITIES_WKB = "naturalearth_cities_wkb.parquet";
const NATURALEARTH_CITIES_GEOARROW = "naturalearth_cities_geoarrow.parquet";

const EXPECTED_META_WKB = `\
{"primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "crs": {"$schema": "https://proj.org/schemas/v0.4/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84", "datum_ensemble": {"name": "World Geodetic System 1984 ensemble", "members": [{"name": "World Geodetic System 1984 (Transit)"}, {"name": "World Geodetic System 1984 (G730)"}, {"name": "World Geodetic System 1984 (G873)"}, {"name": "World Geodetic System 1984 (G1150)"}, {"name": "World Geodetic System 1984 (G1674)"}, {"name": "World Geodetic System 1984 (G1762)"}, {"name": "World Geodetic System 1984 (G2139)"}], "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563}, "accuracy": "2.0", "id": {"authority": "EPSG", "code": 6326}}, "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}, {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]}, "scope": "Horizontal component of 3D system.", "area": "World.", "bbox": {"south_latitude": -90, "west_longitude": -180, "north_latitude": 90, "east_longitude": 180}, "id": {"authority": "EPSG", "code": 4326}}, "geometry_type": "Point", "bbox": [-175.22056447761656, -41.29997393927641, 179.21664709402887, 64.15002361973922]}}, "version": "0.4.0", "creator": {"library": "geopandas", "version": "0.11.1"}}`;

const EXPECTED_META_GEOARROW = `\
{"primary_column": "geometry", "columns": {"geometry": {"encoding": "geoarrow", "crs": {"$schema": "https://proj.org/schemas/v0.4/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84", "datum_ensemble": {"name": "World Geodetic System 1984 ensemble", "members": [{"name": "World Geodetic System 1984 (Transit)"}, {"name": "World Geodetic System 1984 (G730)"}, {"name": "World Geodetic System 1984 (G873)"}, {"name": "World Geodetic System 1984 (G1150)"}, {"name": "World Geodetic System 1984 (G1674)"}, {"name": "World Geodetic System 1984 (G1762)"}, {"name": "World Geodetic System 1984 (G2139)"}], "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563}, "accuracy": "2.0", "id": {"authority": "EPSG", "code": 6326}}, "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}, {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]}, "scope": "Horizontal component of 3D system.", "area": "World.", "bbox": {"south_latitude": -90, "west_longitude": -180, "north_latitude": 90, "east_longitude": 180}, "id": {"authority": "EPSG", "code": 4326}}, "geometry_type": "Point", "bbox": [-175.22056447761656, -41.29997393927641, 179.21664709402887, 64.15002361973922]}}, "version": "0.4.0", "creator": {"library": "geopandas", "version": "0.11.1"}}`;

test("test geo-arrow-spec (wkb) metadata passed through", async (t) => {
  const dataPath = `${dataDir}/${NATURALEARTH_CITIES_WKB}`;
  const arr = new Uint8Array(readFileSync(dataPath));
  const table = tableFromIPC(parquet.readParquet(arr));
  t.equals(
    table.schema.metadata.get("geo"),
    EXPECTED_META_WKB,
    "arrow table metadata should match expected"
  );

  t.end();
});

test("test geo-arrow-spec (geoarrow encoding) metadata passed through", async (t) => {
  const dataPath = `${dataDir}/${NATURALEARTH_CITIES_GEOARROW}`;
  const arr = new Uint8Array(readFileSync(dataPath));
  const table = tableFromIPC(parquet.readParquet(arr));

  t.equals(
    table.schema.metadata.get("geo"),
    EXPECTED_META_GEOARROW,
    "arrow table metadata should match expected"
  );

  const firstCoord = table.getChild("geometry").get(0).toArray();
  t.ok(
    isCloseEqual(firstCoord[0], 12.453386544971766),
    "Nested list should be read correctly"
  );
  t.ok(
    isCloseEqual(firstCoord[1], 41.903282179960115),
    "Nested list should be read correctly"
  );

  t.end();
});

function isCloseEqual(a: number, b: number, eps: number = 0.0001): boolean {
  return Math.abs(a - b) < eps;
}

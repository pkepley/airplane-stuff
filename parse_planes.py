from pathlib import Path
import sqlite3
import json
import numpy as np
import pandas as pd


class airplane:
    def __init__(self, hex_code):
        self.hex_code = hex_code
        self.flight = []
        self.times = []
        self.lat = []
        self.lon = []
        self.alt_baro = []
        self.alt_geom = []


def update_airplane(airplane, t, d):
    keys = d.keys()

    if 'lat' in keys and 'lon' in keys:
        airplane.times.append(t)
        airplane.lat.append(d['lat'])
        airplane.lon.append(d['lon'])
    else:
        # do nothing!
        return

    if 'flight' in keys:
        airplane.flight.append(d['flight'].strip())
    else:
        airplane.flight.append('')

    if 'alt_baro' in keys:
        airplane.alt_baro.append(d['alt_baro'])
    else:
        airplane.alt_baro.append(None)

    if 'alt_geom' in keys:
        airplane.alt_geom.append(d['alt_geom'])
    else:
        airplane.alt_geom.append(None)


def sort_airplane(airplane):
    order = np.array(airplane.times).argsort()
    airplane.flight = np.array(airplane.flight)[order].tolist()
    airplane.lon = np.array(airplane.lon)[order].tolist()
    airplane.lat = np.array(airplane.lat)[order].tolist()
    airplane.alt_baro = np.array(airplane.alt_baro)[order].tolist()
    airplane.alt_geom = np.array(airplane.alt_geom)[order].tolist()
    airplane.times = np.array(airplane.times)[order].tolist()


def airplane_to_dataframe(airplane):
    df = pd.DataFrame({
        'time': airplane.times,
        'flight': airplane.flight,
        'lat': airplane.lat,
        'lon': airplane.lon,
        'alt_baro': [np.nan if x is None else x for x in airplane.alt_baro],
        'alt_geom': [np.nan if x is None else x for x in airplane.alt_geom]
    })
    df['hex_code'] = airplane.hex_code
    df = df[['hex_code', 'flight', 'time', 'lat', 'lon', 'alt_baro', 'alt_geom']]

    return df


def make_db(db_path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS plane_observations (
             id INTEGER PRIMARY KEY AUTOINCREMENT
           , hex_code  TEXT
           , flight    TEXT
           , time      INTEGER
           , lat       REAL
           , lon       REAL
           , alt_baro  REAL
           , alt_geom  REAL
           , added     INTEGER DEFAULT (strftime('%s', 'now'))
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS time_idx ON plane_observations (time);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS hex_code_idx ON plane_observations (hex_code);
    """)
    con.commit()


def update_db_from_df(db_path, df_plane):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("select max(time) from plane_observations;")
    last_obs_time = cur.fetchone()[0]

    if last_obs_time is None:
        last_obs_time = 0

    df_plane_in = df_plane[df_plane.time > last_obs_time]
    n_rows = df_plane_in.shape[0]
    n_times = df_plane_in.time.unique().shape[0]
    print(f"Inserting {n_rows} rows for {n_times} time observations.")

    df_plane_in.to_sql(
        'plane_observations', con, if_exists='append', index=False
    )

    con.commit()


def main(dump_1090_path = "/var/run/dump1090-fa/",
         db_path = "./data/plane_observations.db",
         update_db = True):

    # input path
    dump_1090_dir = Path(dump_1090_path)
    ffs = list(dump_1090_dir.glob("history_*.json"))
    ffs = sorted(ffs, key = lambda fs: fs.stat().st_mtime)

    # load the json files, index by their filename
    data = dict()

    for fs in ffs:
        with open(fs, "r") as f:
            data[fs.stem] = json.load(f)

    # read the planes
    airplanes = dict()

    for fs in list(data.keys()):
        datum = data[fs]
        t = datum['now']

        for aa in datum['aircraft']:
            if 'hex' in aa.keys():
                if aa['hex'] not in airplanes.keys():
                    airplanes[aa['hex']] = airplane(aa['hex'])
                if aa['hex'] in airplanes.keys():
                    update_airplane(airplanes[aa['hex']], t, aa)

    # sort plane records by observation time
    for hh in airplanes.keys():
        aa = airplanes[hh]
        sort_airplane(aa)

        if aa.times != sorted(aa.times):
            break

    # create a dataframe of observations
    df_plane = pd.concat([airplane_to_dataframe(airplane) for airplane in airplanes.values()])
    df_plane.sort_values(['time', 'hex_code'], inplace=True)
    df_plane.reset_index(inplace=True, drop=True)
    df_plane['time'] = np.floor(df_plane.time).astype('int')

    # output database path
    if update_db:
        db_path = Path(db_path)
        db_path.parents[0].mkdir(parents = True, exist_ok=True)
        make_db(db_path)
        update_db_from_df(db_path, df_plane)

    return df_plane


if __name__ == '__main__':
    df_plane = main()

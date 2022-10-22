from pathlib import Path
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.animation as animation


# data lives here
plane_data_dir = Path("./data")
map_data_dir   = Path("./map-data")

# get start time stamp
start_time_stamp = str(pd.Timestamp.today()).split(" ")[0]

# start and end times
start_time = (pd.to_datetime(start_time_stamp))\
  .tz_localize("US/Eastern")\
  .tz_convert("UTC")\
  .timestamp()
start_time = int(start_time)

end_time = (pd.to_datetime(start_time, unit='s') + timedelta(days=1))\
  .tz_localize("US/Eastern")\
  .tz_convert("UTC")\
  .timestamp()
end_time = int(end_time)

# set up plot
fig, ax = plt.subplots(facecolor='k')
ax.set_facecolor('k')
ax.set_xlim([-73.25, -76.25])
ax.set_ylim([38.75, 41.75])
ax.invert_xaxis()
ax.set_xticks([],[])
ax.set_yticks([],[])

# read plane data
plane_data_path = plane_data_dir/"plane_observations.db"
if not plane_data_path.exists():
    print("Plane db does not exist. Nothing to Plot.")
    exit()

con = sqlite3.connect(plane_data_path)
df_plane = pd.read_sql(
    """
    select * from plane_observations
    where time >= :start_time
    and time <= :end_time;
    """,
    con,
    params={'start_time':start_time, 'end_time':end_time}
)
times = np.array(df_plane.time.unique())
times.sort()
planes = np.array(df_plane.hex_code.unique())
planes.sort()

# create colors and line-holder for all observed planes
plane_colors = {
    p: plt.cm.tab20(i % 20) for (i, p) in enumerate(planes)
}
lines = dict()

# plot state boundaries
states_path = map_data_dir/"states"/"cb_2018_us_state_500k.shp"
if states_path.exists():
    states = gpd.read_file(states_path)
    states.boundary.plot(ax = ax, edgecolor='w', facecolor='k')
else:
    print("States shapefile (states_path) not found.")

# plot the receiver location
home_path = map_data_dir/"home.csv"
if home_path.exists():
    home = pd.read_csv(home_path)
    ax.scatter(home.lon, home.lat, s=49,
               c='lightpink',edgecolor='firebrick',zorder=100)
else:
    print("Receiver file (home_path) not found.")

# plot map stuff
airports_path = map_data_dir/"airports.csv"
if airports_path.exists():
    airports = pd.read_csv(airports_path)
    ax.scatter(airports.lon, airports.lat, s=49,
               c='lightskyblue', edgecolor='dodgerblue', zorder=100)
else:
    print("Airports files (airports_path) not found.")

def update_lines(ti, n_trail=10):
    # plot n_trail time steps into past
    tlo, thi = times[max(0, ti-n_trail)], times[max(1, ti)]

    # subset data and get current plane set
    df_curr = df_plane[
        (df_plane.time >= tlo) &
        (df_plane.time <= thi)
    ]
    curr_planes = set(df_curr.hex_code.unique())

    # clean up old lines
    old_planes = [p for p in lines.keys() if p not in curr_planes]

    for p in old_planes:
        lines[p].set_data([],[])
        del lines[p]

    # update all lines
    for p in curr_planes:
        if p not in lines.keys():
            lines[p] = ax.plot([], [], c=plane_colors[p])[0]

        line = lines[p]
        dfp = df_curr[df_curr.hex_code == p]
        line.set_xdata(dfp.lon)
        line.set_ydata(dfp.lat)

    # set the recorded time as the plot title
    t_record = (
        pd.to_datetime(times[ti], unit='s')\
            .tz_localize("UTC")\
            .tz_convert("US/Eastern")\
            .strftime("%Y-%m-%d %I:%M %p")
    )
    ax.set_title(t_record, color='w', size=12, family='mono')
    # ax.set_title(t_record, color='w', size=12, family='mono')#family='Fira Code')

    return lines

# make animation
ani = animation.FuncAnimation(
   fig, update_lines, len(times), interval=50
)
plt.show()


#ani = animation.FuncAnimation(
#   fig, update_lines, len(times), interval=25
#)
#ani.save("my-output.mp4", dpi=600, bitrate=-1)

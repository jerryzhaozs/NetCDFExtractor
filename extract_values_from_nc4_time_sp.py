import netCDF4 as nc
import numpy as np
import pandas as pd
import csv
from datetime import datetime
import math
def depose1(x):
    nearest_number = math.floor(x) + 0.25 if (x % 1) <= 0.5 else math.floor(x) + 0.75
    return nearest_number
# 读取.nc4文件
nc_file = nc.Dataset(r"all_file2.nc4")
csv_file=r'C:\Users\Administrator\Desktop\gdhy-test\Argentinal_Point_Output.txt'
df = pd.read_csv(csv_file)
# 读取coord字段中的纬度和经度信息
nc_lat = nc_file.variables["lat"][:]
nc_lon = nc_file.variables["lon"][:]
nc_time=nc_file.variables["time"][:]
value_values = nc_file.variables['var'][:]
with open('output.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    header = ['lat', 'lon']
    for time_idx, time in enumerate(nc_time):
        header.append(str(time_idx+1))
    writer.writerow(header)
    for index, row in df.iterrows():
        df_lat=row['lat']
        df_lon=row['lon']
        df_lat_use=depose1(df_lat)
        df_lon_use=depose1(df_lon)
        input_data=[df_lat,df_lon]
        latitude_index = np.abs(nc_lat - df_lat_use).argmin()
        longitude_index = np.abs(nc_lon - df_lon_use).argmin()
        for time_idx, time in enumerate(nc_time):
            input_data.append(value_values[time_idx,latitude_index,longitude_index])
        writer.writerow(input_data)
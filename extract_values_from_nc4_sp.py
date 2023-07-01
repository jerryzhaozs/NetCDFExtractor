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
nc_file = nc.Dataset(r"C:\Users\Administrator\Desktop\gdhy-test\maize\yield_1984.nc4")
csv_file=r'C:\Users\Administrator\Desktop\gdhy-test\ba.txt'
df = pd.read_csv(csv_file)
# 读取coord字段中的纬度和经度信息
nc_lat = nc_file.variables["lat"][:]
nc_lon = nc_file.variables["lon"][:]
value_values = nc_file.variables['var'][:]
with open('output.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    header = ['lat', 'lon']
    # for time_idx, time in enumerate(nc_time):
    header.append(str(1))
    writer.writerow(header)
    for index, row in df.iterrows():
        df_lat=row['lat']
        df_lon=row['lon']
        if df_lon<0:
            df_lon+=360.0
        df_lat_use=depose1(df_lat)
        df_lon_use=depose1(df_lon)
        input_data=[]
        latitude_index = np.abs(nc_lat - df_lat_use).argmin()
        longitude_index = np.abs(nc_lon - df_lon_use).argmin()
        if type(value_values[latitude_index,longitude_index])==np.ma.core.MaskedConstant:
            print('??')
        input_data.append(value_values[latitude_index,longitude_index])
        writer.writerow(input_data)
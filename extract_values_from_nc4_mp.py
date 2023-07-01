import netCDF4 as nc
import numpy as np
import pandas as pd
import csv
from datetime import datetime
import math
import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import freeze_support

def depose1(x):
    nearest_number = math.floor(x) + 0.25 if (x % 1) <= 0.5 else math.floor(x) + 0.75
    return nearest_number

def process_file(file, df, idx):
    with nc.Dataset(file) as nc_file:
        nc_lat = nc_file.variables["lat"][:]
        nc_lon = nc_file.variables["lon"][:]
        value_values = nc_file.variables['var'][:]

        output_data = []
        for index, row in df.iterrows():
            if index%1000==0:
                print(index)
            # print(row)
            df_lat = row['lat']
            df_lon = row['lon']
            if df_lon<0:
                df_lon+=360.0
            df_lat_use = depose1(df_lat)
            df_lon_use = depose1(df_lon)
            # input_data = [df_lat, df_lon]
            input_data=[]
            # latitude_index=df_lat_use
            # longitude_index=df_lon_use
            latitude_index = np.abs(nc_lat - df_lat_use).argmin()
            longitude_index = np.abs(nc_lon - df_lon_use).argmin()
            out=value_values[latitude_index,longitude_index]
            if type(out)==np.ma.core.MaskedConstant:
                out=-1.0
            input_data.append(out)
            output_data.append(input_data)
        
        with open(r'C:\Users\Administrator\Desktop\gdhy-test\maize_output2\\'+f'{idx}_output.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            header=[]
            header.append(str(idx))
            writer.writerow(header)
            writer.writerows(output_data)

if __name__ == '__main__':
    # freeze_support()

    # 读取.nc4文件
    path = r'C:\Users\Administrator\Desktop\gdhy-test\maize'
    nc_file_set = [path + "\\" + d for d in os.listdir(path) if d.endswith(".nc4")]
    csv_file = r'C:\Users\Administrator\Desktop\gdhy-test\ba.txt'
    df = pd.read_csv(csv_file)
    output_files = []

    with ProcessPoolExecutor() as executor:
        for idx, nc_file in enumerate(nc_file_set):
            output_files.append(f'{idx} output.csv')
            executor.submit(process_file, nc_file, df, idx)

    # 打印输出文件列表
    print(output_files)

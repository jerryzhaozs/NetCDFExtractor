import netCDF4 as nc
import numpy as np
import pandas as pd
import csv
from datetime import datetime
import math
import os
import re
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import freeze_support

def depose1(x):
    nearest_number = math.floor(x) + 0.25 if (x % 1) <= 0.5 else math.floor(x) + 0.75
    return nearest_number

def process_file(file,out_file,out_dic, df, idx, lat_lon_data):
    with nc.Dataset(file) as nc_file:
        list_name=re.search(r"\d{8}", out_file)[0]
        # print(out_file,type(out_file))
        nc_lat = nc_file.variables["lat"][:]
        nc_lon = nc_file.variables["lon"][:]
        value_values = nc_file.variables['precipitationCal'][:]
        # print(value_values)
        output_data = []
        for index, row in df.iterrows():
            if index%10000==0:
                print(index)
            # print(row)
            df_lat = row['lat']
            df_lon = row['lon']
            # if df_lon<0:
            #     df_lon+=360.0
            # df_lat_use = depose1(df_lat)
            # df_lon_use = depose1(df_lon)
            df_lat_use=df_lat
            df_lon_use=df_lon
            # input_data = [df_lat, df_lon]
            input_data=[]
            # latitude_index=df_lat_use
            # longitude_index=df_lon_use
            latitude_index = np.abs(nc_lat - df_lat_use).argmin()
            longitude_index = np.abs(nc_lon - df_lon_use).argmin()
            out=value_values[0,longitude_index,latitude_index]
            if type(out)==np.ma.core.MaskedConstant:
                out=-1.0
            # input_data.append(round(out,2))
            input_data.append(format(out, '.2f'))
            output_data.append(input_data)

        lesss=str(idx)+'_output.csv'
        out_path=os.path.join(out_dic,lesss)
        # print(out_path)
        if not os.path.exists(out_dic):
            os.makedirs(out_dic)
        if not os.path.exists(os.path.join(out_dic,'latlon.csv')):
            lat_lon_data.to_csv(os.path.join(out_dic,'latlon.csv'), index=False)
        with open(out_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            header=[]
            
            header.append(list_name)
            # print(list_name,type(list_name))
            writer.writerow(header)
            writer.writerows(output_data)

if __name__ == '__main__':
    # freeze_support()
    # 读取.nc4文件
    path = r'C:\Users\Administrator\Desktop\debug0713\gpm_1degree' # nc文件夹
    csv_file = r'C:\Users\Administrator\Desktop\debug0713\zhengzhou_coord.txt' # 坐标文件
    out_dic=r'C:\Users\Administrator\Desktop\debug0713\output' # 输出位置

    name_set=os.listdir(path)
    nc_file_set = [path + "\\" + d for d in name_set if d.endswith(".nc4")]
    # print(nc_file_set)
    name_list=[re.search(r"\d{4}", d)[0] for d in name_set if d.endswith(".nc4")]
    name_list=[os.path.join(out_dic,d) for d in name_list]
    # print(name_list)
    df = pd.read_csv(csv_file)
    output_files = []

    lat_lon_data = df[['lat', 'lon']]
    # print("!",out_dic)
    # lat_lon_data.to_csv(os.path.join(out_dic,'latlon.csv'), index=False)
    with ProcessPoolExecutor() as executor:
        for idx, nc_file in enumerate(nc_file_set):
            output_files.append(f'{idx}_output.csv')
            executor.submit(process_file, nc_file, name_set[idx],name_list[idx],df, idx, lat_lon_data)

    # 打印输出文件列表
    print(output_files)

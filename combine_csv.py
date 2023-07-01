import os

folder_path = r'C:\Users\Administrator\Desktop\gdhy-test\maize_output2'  # 替换为你的文件夹路径

file_paths = []

for root, dirs, files in os.walk(folder_path):
    for file in files:
        file_path = os.path.join(root, file)
        file_paths.append(file_path)

print(file_paths)
csv_files=file_paths


import pandas as pd
merged_data = pd.DataFrame()
# csv_files = [r'C:\Users\Administrator\Desktop\gdhy-test\maize_output2\0_output.csv', r'C:\Users\Administrator\Desktop\gdhy-test\maize_output2\\1_output.csv', r'C:\Users\Administrator\Desktop\gdhy-test\maize_output2\\2_output.csv']  # 替换为你的CSV文件列表

for file in csv_files:
    df = pd.read_csv(file, header=None)  # 假设每个CSV文件只有一列且没有列头
    merged_data = pd.concat([merged_data, df], axis=1)
merged_data.to_csv('merged_file.csv', index=False)  # 替换为你想要保存的文件名

# import pandas as pd

# class DataReader:
#     def __init__(self, file_path=None) -> None:
#         self.filepath = file_path

#     def get_uid(self, filename, row_num):

#         return f"{filename}_{row_num}"

#     def read_file(self, path: str) -> list:
        
#         with open(path, 'r') as f:
#             lines = f.readlines()[1:]
#             lines = list(map(lambda l: l.strip('\n'), lines))
#             return lines

#     def parse(self, lines: list, filename: str) -> tuple:
#         vehicle_info = {
#             "unique_id": [],
#             "track_id": [],
#             "veh_type": [],
#             "traveled_distance": [],
#             "avg_speed": [],
#         }
#         trajectory_info = {
#             "unique_id": [],
#             "lat": [],
#             "lon": [],
#             "speed": [],
#             "lon_acc": [],
#             "lat_acc": [],
#             "time": [],
#         }
#         for row_num, line in enumerate(lines):
#             uid = self.get_uid(filename, row_num)
#             line = line.split("; ")[:-1]
#             assert len(line[4:]) % 6 == 0, f"{line}"
#             vehicle_info["unique_id"].append(uid)
#             vehicle_info["track_id"].append(int(line[0]))
#             vehicle_info["veh_type"].append(line[1])
#             vehicle_info["traveled_distance"].append(float(line[2]))
#             vehicle_info["avg_speed"].append(float(line[3]))
#             for i in range(0, len(line[4:]), 6):
#                 trajectory_info["unique_id"].append(uid)
#                 trajectory_info["lat"].append(float(line[4+i+0]))
#                 trajectory_info["lon"].append(float(line[4+i+1]))
#                 trajectory_info["speed"].append(float(line[4+i+2]))
#                 trajectory_info["lon_acc"].append(float(line[4+i+3]))
#                 trajectory_info["lat_acc"].append(float(line[4+i+4]))
#                 trajectory_info["time"].append(float(line[4+i+5]))

#         vehicle_df = pd.DataFrame(vehicle_info).reset_index(drop=True)
#         trajectories_df = pd.DataFrame(trajectory_info).reset_index(drop=True)
#         return vehicle_df, trajectories_df

#     def get_dfs(self, file_path: str = None, v=0) -> tuple:
      
#         if not file_path and self.filepath:
#             file_path = self.filepath

#         lines = self.read_file(file_path)
#         filename = file_path.split("/")[-1].strip(".csv")
#         vehicle_df, trajectories_df = self.parse(lines, filename)
#         if v > 0:
#             print("vehicle dataframe")
#             print(vehicle_df.head())
#             print(vehicle_df.info())
#             print("trajectories dataframe")
#             print(trajectories_df.head())
#             print(trajectories_df.info())
#         return vehicle_df, trajectories_df

# if __name__ == "__main__":
#     DataReader(file_path="./data/raw_traffic_data.csv").get_dfs(v=1)


# import pandas as pd
# # 1. lets blindly try to read the data into a dataframe

# df = pd.read_csv("/home/addisu/Desktop/10 academy/data/raw_data.csv", sep=";")
# with open("/home/addisu/Desktop/10 academy/data/raw_data.csv", 'r') as file:
#     lines = file.readlines()
# print(f"The number of rows/lines is {len(lines)}")
# print(lines[0]) # column names
# print(lines[0].strip('\n').strip().strip(';').split(';')) # columns names as a list
# lines_as_lists = [line.strip('\n').strip().strip(';').split(';') for line in lines]
# len(lines_as_lists)
# print(f"the number of fields in row 1 is {len(lines_as_lists[1])}, row 2 is {len(lines_as_lists[2])}")
# no_field_max = 0

# for row in lines_as_lists:
#     if len(row) > no_field_max:
#         no_field_max = len(row)

# print(f"the maximum number of fields is {no_field_max}")
# largest_n = int((no_field_max-4)/6)
# print(f"the largest n = {largest_n}")
# cols = lines_as_lists.pop(0)
# track_cols = cols[:4]
# trajectory_cols = ['track_id'] + cols[4:]

# print(track_cols)
# print(trajectory_cols)

# track_info = []
# trajectory_info = []

# for row in lines_as_lists:
#     track_id = row[0]

#     # add the first 4 values to track_info
#     track_info.append(row[:4]) 

#     remaining_values = row[4:]
#     # reshape the list into a matrix and add track_id
#     trajectory_matrix = [ [track_id] + remaining_values[i:i+6] for i in range(0,len(remaining_values),6)]
#     # add the matrix rows to trajectory_info
#     trajectory_info = trajectory_info + trajectory_matrix

# df_track = pd.DataFrame(data= track_info,columns=track_cols)

# df_track.head()
# df_trajectory = pd.DataFrame(data= trajectory_info,columns=trajectory_cols)

# df_trajectory.head()

import pandas as pd

# Read the CSV file, skipping any rows with inconsistent number of fields
df = pd.read_csv("/home/addisu/Desktop/10 academy/data/raw_data.csv", sep=";", on_bad_lines='skip')

with open("/home/addisu/Desktop/10 academy/data/raw_data.csv", 'r') as file:
    lines = file.readlines()
print(f"The number of rows/lines is {len(lines)}")
print(lines[0])  # column names
print(lines[0].strip('\n').strip().strip(';').split(';'))  # columns names as a list
lines_as_lists = [line.strip('\n').strip().strip(';').split(';') for line in lines]
len(lines_as_lists)
print(f"the number of fields in row 1 is {len(lines_as_lists[1])}, row 2 is {len(lines_as_lists[2])}")
no_field_max = 0

for row in lines_as_lists:
    if len(row) > no_field_max:
        no_field_max = len(row)

print(f"the maximum number of fields is {no_field_max}")
largest_n = int((no_field_max-4)/6)
print(f"the largest n = {largest_n}")
cols = lines_as_lists.pop(0)
track_cols = cols[:4]
trajectory_cols = ['track_id'] + cols[4:]

print(track_cols)
print(trajectory_cols)

track_info = []
trajectory_info = []

for row in lines_as_lists:
    track_id = row[0]

    # add the first 4 values to track_info
    track_info.append(row[:4])

    remaining_values = row[4:]
    # reshape the list into a matrix and add track_id
    trajectory_matrix = [[track_id] + remaining_values[i:i+6] for i in range(0,len(remaining_values),6)]
    # add the matrix rows to trajectory_info
    trajectory_info = trajectory_info + trajectory_matrix

df_track = pd.DataFrame(data=track_info, columns=track_cols)
df_track.head()

df_trajectory = pd.DataFrame(data=trajectory_info, columns=trajectory_cols)
df_trajectory.head()

# Save DataFrames to CSV files
df_track.to_csv('/home/addisu/Desktop/10 academy/data/track_data.csv', index=False)
df_trajectory.to_csv('/home/addisu/Desktop/10 academy/data/trajectory_data.csv', index=False)

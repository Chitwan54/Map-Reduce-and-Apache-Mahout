# Import Libraries

import csv
import os
from collections import defaultdict

import numpy as np

'''
Problem Statement:
Find the difference between the maximum and the minimum, “Wind Speed,” from all 
of the weather stations for each day in the month.
'''
# Solution starts
'''
Mapper Function:
-> Takes a line from the data file.
-> store the daily wind speed.
-> return the date and the list of wind speed.
'''


def mapper(record):
    """
    :param record: A line from the data file
    :return: date and wind speed
    """
    curr_date = int(record[1])
    try:
        curr_wind_speed = float(record[11])
    except ValueError:
        curr_wind_speed = np.nan
    yield curr_date, curr_wind_speed


'''
Reducer Function:
-> Takes in date and wind speed from the mapper.
-> Compute the max wind speed.
-> Compute the min wind speed.
-> Compute the difference of max and min wind speed.
-> Yield the date and the difference.
'''


def reducer(curr_date, curr_wind_speed):
    """
    :param curr_date: current date
    :param curr_wind_speed: wind speed on the corresponding date
    :return: date and difference of maximum and minimum wind speed
    """
    max_val = max(curr_wind_speed)
    min_val = min(curr_wind_speed)
    diff = max_val - min_val

    yield curr_date, diff


# main function
if __name__ == "__main__":

    txt_file = r"C:\Users\Chitwan Manchanda\Desktop\UpworksTask1\DataSets-20221101"
    weather_files = sorted(os.listdir(txt_file))
    for file_name in weather_files:
        weather_file_path = os.path.join(txt_file, file_name)
        with open(weather_file_path, "r") as f:
            weather_data = csv.reader(f)

            # skip header
            next(weather_data)

            # Execute the mapper
            intermediate_results = defaultdict(list)
            for line in weather_data:
                # Skipping the empty lines
                if len(line) <= 1:
                    continue
                for date, wind_speed in mapper(line):
                    intermediate_results[date].append(wind_speed)

            # Execute into reducer
            results = []
            for date1, wind_speed in intermediate_results.items():
                for date2, max_diff in reducer(date1, wind_speed):
                    results.append((date2, max_diff))

            # Sort the output
            results.sort()

            # Print the final result
            for i in range(len(results)):
                date = str(results[i][0])
                max_diff = results[i][1]

                print(f"Date: {date[: 4]}/{date[4:6]}/{date[6:]}")
                print(f"Maximum and Minimum Wind Speed Difference: {max_diff}")
                print()

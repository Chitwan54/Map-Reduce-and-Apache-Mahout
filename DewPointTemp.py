# Import Libraries

import csv
import os
from collections import defaultdict

import numpy as np

'''
Problem Statement:
Find the daily mean and	variance of, “Dew Point Temp,” from all of the weather stations.
'''
# Solution
'''
Mapper Function:
-> Takes in the line (curr_record) from the data fields.
-> Extract the current date and the dew point temperature.
-> Yield current date and dew point temperature.
'''


def mapper(curr_record):
    """
    :param curr_record: A line from the data file
    :return: date and dew point temperature
    """
    curr_date = int(curr_record[1])
    try:
        dew_point_temp = float(curr_record[9])
    except ValueError:
        dew_point_temp = np.nan

    yield curr_date, dew_point_temp


'''
Reducer Function:
-> Takes in the current date and a list of dew point temperatures.
-> Computes the mean and variance of dew point temperatures for the corresponding date.
-> Yields the date and the corresponding mean and variance of dew point temperatures.
'''


def reducer(curr_date, dew_point_temps):
    """
    :param curr_date: date
    :param dew_point_temps: a list of dew point temperatures (dictionary values)
    :return: date and the corresponding mean and variance of dew point temperatures
    """
    dew_point_temps = np.asarray(dew_point_temps)
    dew_point_temps = dew_point_temps[~np.isnan(dew_point_temps)]
    mean_temp = sum(dew_point_temps) / len(dew_point_temps)
    variance = sum((temp - mean_temp) ** 2 for temp in dew_point_temps) / len(dew_point_temps)

    yield curr_date, (mean_temp, variance)


if __name__ == "__main__":
    txt_file = r"C:\Users\Chitwan Manchanda\Desktop\UpworksTask1\DataSets-20221101"
    weather_files = sorted(os.listdir(txt_file))
    for file_name in weather_files:
        # Read the weather data from the file
        weather_file_path = os.path.join(txt_file, file_name)

        with open(weather_file_path, "r") as f:
            reader = csv.reader(f)

            # Skip the header row
            next(reader)

            # Create a dictionary to store the intermediate values from the mapper
            intermediate = defaultdict(list)

            # Call the mapper function for each record
            for record in reader:
                # Skipping the empty lines
                if len(record) <= 1:
                    continue
                for key, value in mapper(record):
                    intermediate[key].append(value)

            # Create a list to store the final results
            result = []

            # Call the reducer function for each key-value pair in the intermediate dictionary
            for key, value in intermediate.items():
                for k, v in reducer(key, value):
                    result.append((k, v))

            # Sort the result list by the date
            result.sort()

            # Print the final result
            for i in range(len(result)):
                date = str(result[i][0])
                mean = result[i][1][0]
                var = result[i][1][1]

                print(f"Date: {date[: 4]}/{date[4:6]}/{date[6:]}")
                print(f"Mean: {mean} ; Variance: {var}")
                print()

# Import Libraries

import csv
import os
from collections import defaultdict

import numpy as np

'''
Problem Statement:
Find the daily minimum, “Relative Humidity” from all of the	weather	stations.
'''


def mapper(record):
    """
    :param record: a line from the data files
    :return: date and relative humidity
    """
    curr_date = int(record[1].strip())
    try:
        relative_humidity_val = float(record[11].strip())
    except ValueError:
        relative_humidity_val = np.nan

    yield curr_date, relative_humidity_val


def reducer(curr_date, relative_humidity_val):
    """
    :param curr_date: current date
    :param relative_humidity_val: relative humidity value corresponding current date
    :return: current date and minimum relative humidity value
    """
    # Compute the minimum relative humidity
    yield curr_date, min(relative_humidity_val)


if __name__ == "__main__":
    txt_file = r"C:\Users\Chitwan Manchanda\Desktop\UpworksTask1\DataSets-20221101"
    weather_files = sorted(os.listdir(txt_file))

    for file_name in weather_files:
        weather_file_path = os.path.join(txt_file, file_name)

        with open(weather_file_path, "r") as f:
            weather_data = csv.reader(f)

            # Skip the header
            next(weather_data)

            # Create a dictionary to store the intermediate values from the mapper
            intermediate_steps = defaultdict(list)

            # Call the mapper function for each record
            for line in weather_data:
                # Skip the missing lines
                if len(line) <= 1:
                    continue
                for date, relative_humidity in mapper(line):
                    intermediate_steps[date].append(relative_humidity)

            # Create a list to store the final results
            results = []

            # Call the reducer function for each key-value pair in the intermediate dictionary
            for date, relative_humidity in intermediate_steps.items():
                for key, value in reducer(date, relative_humidity):
                    results.append((key, value))

            # Sort the result list by the date
            results.sort()

            # Print the final result
            for i in range(len(results)):
                date = str(results[i][0])
                min_relative_humidity = results[i][1]

                print(f"Date: {date[: 4]}/{date[4:6]}/{date[6:]}")
                print(f"Minimum Relative Humidity: {min_relative_humidity}")
                print()

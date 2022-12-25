import csv
import os
from collections import defaultdict

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer

'''
Problem Statement:
Find the correlation matrix	that describes the monthly correlation among, “Relative
Humidity”, “Wind Speed” and	“Dry Bulb Temp," from all of the weather stations.
'''


def pearson_correlation(x1, x2):
    """
    :param x1: variable 1
    :param x2: variable 2
    :return: Pearson Correlation Coefficient b/w x1 and x2
    """
    # Convert into numpy arrays
    x1 = np.asarray(x1)
    x2 = np.asarray(x2)

    # Remove nan values
    x1 = x1[~(np.isnan(x1))]
    x2 = x2[~(np.isnan(x2))]

    # Compute the mean of both variables
    mu1 = np.sum(x1) / len(x1)
    mu2 = np.sum(x2) / len(x2)

    # Compute the Pearson Correlation Coefficient
    pearson_coef = np.sum((x1 - mu1) * (x2 - mu2)) / np.sqrt(np.sum((x1 - mu1) ** 2) * np.sum((x2 - mu2) ** 2))

    return pearson_coef


'''
Mapper Function:
-> Takes in the line from the data file.
-> Extracts the current month.
-> Extracts the relative humidity, wind speed and dry bulb temperature.
-> Yields the current month, relative humidity, wind speed and dry bulb temperature.
'''


def mapper(curr_record):
    """
    :param curr_record: a line from current data
    :return: month, relative humidity, wind speed and dry bulb temperature
    """
    curr_month = int(curr_record[1][4:6])
    # Extract relative humidity
    try:
        relative_humidity = float(curr_record[11].strip())
    except ValueError:
        relative_humidity = np.nan
    # Extract wind speed
    try:
        wind_speed = float(curr_record[12].strip())
    except ValueError:
        wind_speed = np.nan
    # Extract dry bulb temperature
    try:
        dry_bulb_temp = float(curr_record[8])
    except ValueError:
        dry_bulb_temp = np.nan

    yield curr_month, (relative_humidity, wind_speed, dry_bulb_temp)


'''
Reducer Function:
-> Takes in the month and weather data (relative humidity, wind speed, dry bulb temperature.
-> Computes the Pearson Correlation Matrix.
-> Yields the month and the Pearson Correlation Matrix for variables for the given month.
'''


def reducer(curr_month, weather_data_arr):
    """
    :param curr_month: current month
    :param weather_data_arr: weather data comprizing of relative humidity, wind speed and dry bulb temperature
    :return: current month and pearson correlation dataframe
    """
    correlation_matrix = [[0 for _ in range(weather_data_arr.shape[0])] for _ in range(weather_data_arr.shape[0])]

    for i in range(weather_data_arr.shape[0]):
        for j in range(weather_data_arr.shape[0]):
            x1 = weather_data_arr[i]
            x2 = weather_data_arr[j]

            corr = pearson_correlation(x1, x2)
            correlation_matrix[i][j] = corr

    correlation_df = pd.DataFrame(correlation_matrix,
                                  index=["relative humidity",
                                         "Wind Speed",
                                         "Dry Bulb Temp"],
                                  columns=["relative humidity",
                                           "Wind Speed",
                                           "Dry Bulb Temp"]
                                  )
    yield curr_month, correlation_df


if __name__ == "__main__":
    txt_file = r"C:\Users\Chitwan Manchanda\Desktop\UpworksTask1\DataSets-20221101"
    weather_files = sorted(os.listdir(txt_file))
    for file_name in weather_files:
        weather_file_path = os.path.join(txt_file, file_name)
        with open(weather_file_path, "r") as f:
            reader = csv.reader(f)

            # Skip header
            next(reader)

            # Create a dictionary to store the intermediate values from the mapper
            intermediate_results = defaultdict(list)

            # Call the mapper function for each record
            for record in reader:
                # Skip the missing lines
                if len(record) <= 1:
                    continue
                for month, weather_data in mapper(record):
                    intermediate_results[month].append(weather_data)

            # Create a list to store the final results
            results = []

            # Call the reducer function for each key-value pair in the intermediate dictionary
            for month1, weather_data in intermediate_results.items():

                # Convert weather data into ndarray
                weather_data = np.array(weather_data).reshape(3, -1)

                # Impute Null Values with mean
                imputer_obj = SimpleImputer(missing_values=np.nan, strategy='mean')
                imputer_obj = imputer_obj.fit(weather_data)
                weather_data = imputer_obj.transform(weather_data)

                # Pass the preprocessed values into the reducer
                for month2, corr_matrix in reducer(month1, weather_data):
                    results.append((month2, corr_matrix))

            # Sort the result list by the date
            results.sort()

            # Print the final result
            for i in range(len(results)):
                month = str(results[i][0])
                corr_matrix = results[i][1]

                print(f"Month: {month}")
                print("Correlation Matrix:")
                print(corr_matrix)
                print()

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


class MockFTPData:
    def generate_mock_data(
        self, file_name="mock_electricity_data.csv", num_entries=100
    ):
        """
        Generate mock electricity consumption data and save it as a CSV file.

        This function creates random watt-minute data with corresponding watt-hour values,
        generates timestamps 15 minutes apart, and saves the data to a CSV file.

        Parameters
        ----------
        file_name : str, optional
            The name of the CSV file to save the data (default is 'mock_electricity_data.csv').

        num_entries : int, optional
            The number of data entries to generate (default is 100).

        Returns
        -------
        None
            The method retun the dataframe of the mocked Energy Consumption Data.
        """
        # Generate timestamps (every 15 minutes)
        start_time = datetime.now()
        timestamps = [
            start_time - timedelta(minutes=15 * i) for i in range(num_entries)
        ]

        # Generate random watt-minute consumption values (between 10 and 500 watt-minutes)
        watt_minutes = np.random.uniform(10, 500, size=num_entries)

        # Convert watt-minutes to watt-hours (watt-minutes / 60)
        watt_hours = watt_minutes / 60

        # Create a DataFrame with the mock data
        data = {
            "timestamp": timestamps,
            "watt_minutes": watt_minutes,
            "watt_hours": watt_hours,
        }

        df = pd.DataFrame(data)

        return df

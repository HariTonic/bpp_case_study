from datetime import datetime, timedelta

import numpy as np
import pandas as pd


class MockFTPData:
    """
    A class to generate mock electricity consumption data for testing or simulation purposes.
    """

    def generate_mock_data(
        self, file_name="mock_electricity_data.csv", num_entries=100
    ):
        """
        Generate mock electricity consumption data and return it as a Pandas DataFrame.

        The function generates timestamps at 15-minute intervals, random watt-minute consumption values,
        and computes the corresponding watt-hour values. The generated data is returned as a Pandas DataFrame.

        Parameters
        ----------
        file_name : str, optional
            The name of the CSV file to save the data (default is 'mock_electricity_data.csv').

        num_entries : int, optional
            The number of data entries to generate (default is 100).

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the generated mock electricity consumption data.
        """
        # Get the current timestamp
        start_time = datetime.now()

        # Generate timestamps at 15-minute intervals
        timestamps = [
            start_time - timedelta(minutes=15 * i) for i in range(num_entries)
        ]

        # Generate random watt-minute consumption values between 10 and 500
        watt_minutes = np.random.uniform(10, 500, size=num_entries)

        # Convert watt-minutes to watt-hours (1 watt-hour = 60 watt-minutes)
        watt_hours = watt_minutes / 60

        # Create a dictionary with the generated data
        data = {
            "timestamp": timestamps,
            "watt_minutes": watt_minutes,
            "watt_hours": watt_hours,
        }

        # Convert the dictionary to a Pandas DataFrame
        df = pd.DataFrame(data)

        return df

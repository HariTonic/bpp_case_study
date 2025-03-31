import os

import pandas as pd


class Golden:
    """
    The Golden class processes and refines data for electricity, IEX market, and PVWatts.
    It removes duplicates, computes additional metrics, and stores cleaned datasets.
    """

    def read_csv(self, file_name):
        """
        Reads a CSV file into a pandas DataFrame.

        Parameters:
        file_name (str): Path to the CSV file.

        Returns:
        pd.DataFrame: Loaded data.
        """
        return pd.read_csv(file_name)

    def save_df(self, file_path, df):
        """
        Saves a DataFrame to the 'golden' directory.

        Parameters:
        file_path (str): Original file path (used to extract filename).
        df (pd.DataFrame): Processed DataFrame to save.
        """
        file_name = os.path.basename(file_path)  # Extract filename
        new_file_path = os.path.join("golden", file_name)  # Construct new path
        os.makedirs("golden", exist_ok=True)  # Ensure directory exists
        df.to_csv(new_file_path, index=False)  # Save DataFrame

    def process_iex_data(self):
        """
        Processes IEX market data by removing duplicate entries.
        """
        file_path = os.path.join("processed", "iex_data.csv")
        df = self.read_csv(file_name=file_path)
        # Extract END time from "Time Block"
        df["End Time"] = df["Time Block"].str.split("-").str[1]
        # Combine Date and End Time
        df["Timestamp"] = pd.to_datetime(df["Date"] + " " + df["End Time"], format="%d-%m-%Y %H:%M")
        df = df[["Timestamp", "MCP (Rs/MWh) *"]]
        df.drop_duplicates(inplace=True)
        self.save_df(file_path=file_path, df=df)

    def process_electricity_data(self):
        """
        Processes electricity consumption data by removing duplicates.
        """
        file_path = os.path.join("processed", "mock_electricity_data.csv")
        df = self.read_csv(file_name=file_path)
        df.drop_duplicates(inplace=True)
        self.save_df(file_path=file_path, df=df)

    def process_pvwatt_data(self):
        """
        Processes PVWatts solar energy data by computing Energy Yield (kWh) and formatting timestamps.
        """
        file_path = os.path.join("processed", "pvwatts_hourly.csv")
        df = self.read_csv(file_name=file_path)

        # Create a timestamp column using Month, Day, and Hour
        df["Timestamp"] = df["Month"].astype(str) + "-" + df["Day"].astype(str) + " " + df["Hour"].astype(str) + ":00"

        # Compute Energy Yield (AC System Output in kWh)
        df["Energy_Yield_kWh"] = df["AC System Output (W)"] / 1000

        # Select only relevant columns
        df = df[["Timestamp", "Energy_Yield_kWh"]]

        print(df)

        df.drop_duplicates(inplace=True)
        self.save_df(file_path=file_path, df=df)

    def runner(self):
        """
        Executes the full data processing pipeline.
        """
        self.process_iex_data()
        self.process_electricity_data()
        self.process_pvwatt_data()


if __name__ == "__main__":
    obj = Golden()
    obj.runner()

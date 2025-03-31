import os

import pandas as pd


class Silver:
    """
    Silver class for validating and processing energy consumption, IEX market,
    and PVWatts data. This class reads data from CSV files, validates required columns,
    and saves processed or rejected data accordingly.
    """

    def read_data(self, file_path):
        """
        Reads a CSV file into a Pandas DataFrame.

        Parameters
        ----------
        file_path : str
            Path to the CSV file.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the CSV data.
        """
        return pd.read_csv(file_path)

    def validate_df(self, columns, df):
        """
        Validates if the DataFrame contains the required columns.

        Parameters
        ----------
        columns : list of str
            List of required column names.
        df : pd.DataFrame
            DataFrame to be validated.

        Returns
        -------
        bool
            True if the DataFrame contains all required columns, False otherwise.
        """
        return set(df.columns).issuperset(columns)

    def save_data(self, status, file_name, df):
        """
        Saves the DataFrame to a CSV file under a specified directory.

        Parameters
        ----------
        status : str
            Subdirectory name ('processed' or 'rejected').
        file_name : str
            Name of the file to save.
        df : pd.DataFrame
            DataFrame to be saved.
        """
        file_path = os.path.join(status, file_name)
        print(f"Saving file to: {file_path}")
        os.makedirs(status, exist_ok=True)
        df.to_csv(file_path, index=False)

    def validate_energy_consume_data(self):
        """
        Validates energy consumption data by checking for required columns.
        Moves valid data to 'processed' and invalid data to 'rejected'.
        """
        file_name = "mock_electricity_data.csv"
        df = self.read_data(file_name)
        columns = ["timestamp", "watt_minutes", "watt_hours"]
        cond = self.validate_df(columns, df)
        self.save_data(
            status="processed" if cond else "rejected", file_name=file_name, df=df
        )

    def validate_iex_data(self):
        """
        Validates IEX market data by checking for required columns.
        Moves valid data to 'processed' and invalid data to 'rejected'.
        """
        file_name = "iex_data.csv"
        df = self.read_data(file_name)
        columns = [
            "Date",
            "Hour",
            "Session ID",
            "Time Block",
            "Purchase Bid (MW)",
            "Sell Bid (MW)",
            "MCV (MW)",
            "Final Scheduled Volume (MW)",
            "MCP (Rs/MWh) *",
        ]
        cond = self.validate_df(columns, df)
        print(f"IEX data validation: {cond}")
        self.save_data(
            status="processed" if cond else "rejected", file_name=file_name, df=df
        )

    def validate_pv_watt_data(self):
        """
        Validates PVWatts energy data by checking for required columns.
        Moves valid data to 'processed' and invalid data to 'rejected'.
        """
        file_name = "pvwatts_hourly.csv"
        df = pd.read_csv(file_name, skiprows=31, header=0)
        columns = [
            "Month",
            "Day",
            "Hour",
            "Plane of Array Irradiance (W/m2)",
            "DC Array Output (W)",
            "AC System Output (W)",
        ]
        print("Validating PVWatts data:")
        print(df[columns])
        cond = self.validate_df(columns, df)
        print(f"PVWatts data validation: {cond}")
        self.save_data(
            status="processed" if cond else "rejected", file_name=file_name, df=df
        )

    def runner(self):
        """
        Executes the validation processes for all datasets.
        """
        self.validate_energy_consume_data()
        self.validate_iex_data()
        self.validate_pv_watt_data()


if __name__ == "__main__":
    obj = Silver()
    obj.runner()

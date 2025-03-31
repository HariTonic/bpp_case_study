import os

import pandas as pd


class Silver:

    def read_data(self, file_path):
        df = pd.read_csv(file_path)
        return df

    def validate_df(self, columns, df):
        cond = set(df.columns).issuperset(columns)
        return cond

    def save_data(self, status, file_name, df):
        file_path = rf"{status}\{file_name}"
        print(f"file_path: {file_path}")
        os.makedirs(status, exist_ok=True)
        df.to_csv(file_path, index=False)

    def validate_energy_consume_data(self):
        file_name = "mock_electricity_data.csv"
        df = self.read_data(file_path=file_name)
        columns = ["timestamp", "watt_minutes", "watt_hours"]
        cond = self.validate_df(columns=columns, df=df)

        if cond:
            self.save_data(status="processed", file_name=file_name, df=df)
        else:
            self.save_data(status="rejected", file_name=file_name, df=df)

    def vlaidate_iex_data(self):
        file_name = "iex_data.csv"
        df = self.read_data(file_path=file_name)
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
        cond = self.validate_df(columns=columns, df=df)
        print(f"cond: {cond}")
        if cond:
            self.save_data(status="processed", file_name=file_name, df=df)
        else:
            self.save_data(status="rejected", file_name=file_name, df=df)

    def validate_pv_watt_data(self):
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
        print("df")
        print(df[columns])
        cond = self.validate_df(columns=columns, df=df)
        print(f"cond: {cond}")
        if cond:
            self.save_data(status="processed", file_name=file_name, df=df)
        else:
            self.save_data(status="rejected", file_name=file_name, df=df)

    def runner(self):
        self.validate_energy_consume_data()
        self.vlaidate_iex_data()
        self.validate_pv_watt_data()


if __name__ == "__main__":
    obj = Silver()
    obj.runner()

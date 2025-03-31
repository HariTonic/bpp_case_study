import pandas as pd
import os

class Golden:

    def read_csv(self, file_name):
        df = pd.read_csv(file_name)
        return df

    def save_df(self, file_path, df):
        file_name = file_path.split("\\")[1]
        new_file_path = rf"golden\{file_name}"
        os.makedirs("golden", exist_ok=True)
        df.to_csv(new_file_path, index = False)

    def process_iex_data(self):
        file_path = r"processed\iex_data.csv"
        df = self.read_csv(file_name=file_path)
        df.drop_duplicates(inplace=True)
        self.save_df(file_path=file_path, df = df)

    def process_electricity_data(self):
        file_path = r"processed\mock_electricity_data.csv"
        df = self.read_csv(file_name=file_path)
        df.drop_duplicates(inplace=True)
        self.save_df(file_path=file_path, df = df)

    def process_pvwatt_data(self):
        # Load the dataset (Replace 'solar_data.csv' with actual file path)
        file_path = r"processed\pvwatts_hourly.csv"
        df = self.read_csv(file_name=file_path)

        # Convert 'Month', 'Day', 'Hour' to a datetime format
        df['Timestamp'] = pd.to_datetime(df[['Month', 'Day', 'Hour']].astype(str).agg('-'.join, axis=1), format='%m-%d-%H')

        # Compute Energy Yield (AC System Output in kWh)
        df['Energy_Yield_kWh'] = df['AC System Output (W)'] / 1000


        df = df[['Timestamp', 'Energy_Yield_kWh']]

        df.drop_duplicates(inplace=True)
        self.save_df(file_path=file_path, df = df)


    def runner(self):
        self.process_iex_data()
        self.process_electricity_data()
        self.process_pvwatt_data()

if __name__ == "__main__":
    obj = Golden()
    obj.runner()
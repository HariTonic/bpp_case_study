from enrgy_consptn_data_mock import MockFTPData
from ftp_mock import MockFTP
import threading
# from utils.s3 import S3
import pandas as pd
import time
import socket
from iex_webscrapping import IEXWebScrap

class BronzeZone:
    def __init__(self):
        self.file_name = 'mock_electricity_data.csv'
        self.file_relative_path = r"C:\Users\h.magendhiran\Documents\BPP\mock_electricity_data.csv"

    def wait_for_ftp(self, host="127.0.0.1", port=2121, timeout=5):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                with socket.create_connection((host, port), timeout=2):
                    print("FTP server is ready!")
                    return True
            except (socket.timeout, ConnectionRefusedError):
                time.sleep(0.5)  # Wait and retry
        raise ConnectionError("FTP server did not start in time!")

    def prepare_mock_data(self):
        mock_ftp_data_obj = MockFTPData()
        df = mock_ftp_data_obj.generate_mock_data()

        # df.to_csv(self.file_name, index=False)
        self.save_df(file_name=self.file_name, df = df)

        print(df)

    def read_iex_data(self):
        # Read the IEX Market Prices
        iex_webscrap = IEXWebScrap()
        market_price_df = iex_webscrap.api_invoke()
        self.save_df(file_name=market_price_df, df = market_price_df)

    def save_df(self, file_name, df):
        df.to_csv(file_name, index = False)

    def ftp_process(self):
        mock_ftp_obj = MockFTP()

        ftp_server_thread = threading.Thread(target=mock_ftp_obj.start_ftp_server)
        ftp_server_thread.daemon = True  # Ensure it stops when the main program stops
        ftp_server_thread.start()
        time.sleep(10)
        self.wait_for_ftp()
        mock_ftp_obj.ftp_upload(file_name=self.file_relative_path)
        elect_data_df = mock_ftp_obj.ftp_download(file_name=self.file_name)

    def runner(self):
        self.prepare_mock_data()
        self.ftp_process()
        self.read_iex_data()


if __name__ == "__main__":
    obj = BronzeZone()
    obj.runner()
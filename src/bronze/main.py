from mock_ftp_data import MockFTPData
from ftp_mock import MockFTP
import threading
from utils.s3 import S3
import pandas as pd

class BronzeZone:
    def __init__(self):
        self.file_name = 'mock_electricity_data.csv'

    def runner(self):
        mock_ftp_data_obj = MockFTPData()
        df = mock_ftp_data_obj.generate_mock_data()

        df.to_csv(self.file_name, index=False)

        print(df)

        mock_ftp_obj = MockFTP()

        ftp_server_thread = threading.Thread(target=mock_ftp_obj.start_ftp_server)
        ftp_server_thread.daemon = True  # Ensure it stops when the main program stops
        ftp_server_thread.start()

        mock_ftp_obj.ftp_upload(file_name=self.file_name)
        elect_data_df = mock_ftp_obj.ftp_download(file_name=self.file_name)

        print(elect_data_df)

if __name__ == "__main__":
    obj = BronzeZone()
    obj.runner()
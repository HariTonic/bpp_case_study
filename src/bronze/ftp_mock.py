import ftplib
import os
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import pandas as pd

class MockFTP:
    # Function to upload a file to an FTP server
    def ftp_upload(self, file_name='mock_electricity_data.csv', ftp_host='127.0.0.1', ftp_port=21):
        """
        Uploads the file to the FTP server.
        """
        # Connect to FTP server
        with ftplib.FTP() as ftp:
            ftp.connect(ftp_host, ftp_port)
            ftp.login('user', 'password')  # Login using credentials
            with open(file_name, 'rb') as f:
                ftp.storbinary(f'STOR {file_name}', f)
            print(f"File uploaded to FTP server: {file_name}")

    # Function to download a file from the FTP server
    def ftp_download(self, file_name='mock_electricity_data.csv', ftp_host='127.0.0.1', ftp_port=21):
        """
        Downloads the file from the FTP server.
        """
        # Connect to FTP server
        with ftplib.FTP() as ftp:
            ftp.connect(ftp_host, ftp_port)
            ftp.login('user', 'password')  # Login using credentials
            with open(f'downloaded_{file_name}', 'wb') as f:
                ftp.retrbinary(f'RETR {file_name}', f.write)
            print(f"File downloaded from FTP server: {file_name}")

        df = pd.read_csv(f'downloaded_{file_name}')
        print("DataFrame created from downloaded file:")
        return df

    # Function to start a local FTP server
    def start_ftp_server(self):
        """
        Starts a simple FTP server locally using pyftpdlib.
        """
        authorizer = DummyAuthorizer()
        # Allow anonymous access with read/write permissions to the directory containing files
        authorizer.add_user('user', 'password', os.getcwd(), perm='elradfmw')  # 'elradfmw' means read/write permissions

        handler = FTPHandler
        handler.authorizer = authorizer

        server = FTPServer(('127.0.0.1', 21), handler)
        print("Starting FTP server on 127.0.0.1:21...")
        server.serve_forever()

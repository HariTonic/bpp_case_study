import ftplib
import os

import pandas as pd
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer


class MockFTP:
    """
    A class to simulate FTP server interactions, including uploading, downloading,
    and running a local FTP server for testing purposes.
    """

    def ftp_upload(
        self, file_name="mock_electricity_data.csv", ftp_host="127.0.0.1", ftp_port=2121
    ):
        """
        Upload a file to an FTP server.

        Parameters
        ----------
        file_name : str, optional
            The name of the file to be uploaded (default is 'mock_electricity_data.csv').
        ftp_host : str, optional
            The hostname or IP address of the FTP server (default is '127.0.0.1').
        ftp_port : int, optional
            The port number of the FTP server (default is 2121).

        Returns
        -------
        None
        """
        # Connect to the FTP server
        with ftplib.FTP() as ftp:
            ftp.connect(ftp_host, ftp_port)
            ftp.login("user", "password")  # Login using credentials
            ftp.set_pasv(True)  # Enable passive mode

            # Open the file in binary mode and upload it
            with open(file_name, "rb") as f:
                print(f"Uploading file: {file_name}")
                ftp.storbinary(f"STOR {file_name}", f)
            print(f"File uploaded successfully: {file_name}")

    def ftp_download(
        self, file_name="mock_electricity_data.csv", ftp_host="127.0.0.1", ftp_port=2121
    ):
        """
        Download a file from an FTP server.

        Parameters
        ----------
        file_name : str, optional
            The name of the file to be downloaded (default is 'mock_electricity_data.csv').
        ftp_host : str, optional
            The hostname or IP address of the FTP server (default is '127.0.0.1').
        ftp_port : int, optional
            The port number of the FTP server (default is 2121).

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the data from the downloaded file.
        """
        # Connect to the FTP server
        with ftplib.FTP() as ftp:
            ftp.connect(ftp_host, ftp_port)
            ftp.login("user", "password")  # Login using credentials

            # Download the file in binary mode
            with open(f"downloaded_{file_name}", "wb") as f:
                ftp.retrbinary(f"RETR {file_name}", f.write)
            print(f"File downloaded successfully: {file_name}")

        # Read the downloaded CSV file into a DataFrame
        df = pd.read_csv(f"downloaded_{file_name}")
        print("DataFrame created from downloaded file.")
        return df

    def start_ftp_server(self):
        """
        Start a local FTP server using pyftpdlib.

        The server allows a single user with read/write permissions to interact
        with the current working directory.

        Returns
        -------
        None
        """
        # Set up user authentication for the FTP server
        authorizer = DummyAuthorizer()
        authorizer.add_user(
            "user", "password", os.getcwd(), perm="elradfmw"
        )  # Read/write permissions

        # Create FTP handler and assign authorizer
        handler = FTPHandler
        handler.authorizer = authorizer

        # Start the FTP server
        server = FTPServer(("127.0.0.1", 21), handler)
        print("Starting FTP server on 127.0.0.1:21...")
        server.serve_forever()

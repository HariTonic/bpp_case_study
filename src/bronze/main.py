import socket
import threading
import time

# Importing necessary modules
from enrgy_consptn_data_mock import MockFTPData
from ftp_mock import MockFTP
from iex_webscrapping import IEXWebScrap
from pvwatts import PVWattDataRead


class BronzeZone:
    """
    A class to handle data collection, processing, and FTP operations for electricity consumption and market data.

    This class generates mock electricity data, retrieves market data from IEX, downloads PV watt data,
    and manages FTP operations for uploading and downloading data files.
    """

    def __init__(self):
        """
        Initializes the BronzeZone class with default file names and paths.
        """
        self.file_name = "mock_electricity_data.csv"
        self.file_relative_path = (
            r"C:\Users\h.magendhiran\Documents\BPP\mock_electricity_data.csv"
        )

    def wait_for_ftp(self, host="127.0.0.1", port=2121, timeout=5):
        """
        Waits for the FTP server to be available within a given timeout period.

        Parameters
        ----------
        host : str, optional
            FTP server hostname (default is "127.0.0.1").
        port : int, optional
            FTP server port (default is 2121).
        timeout : int, optional
            Maximum time to wait for the FTP server to be ready (default is 5 seconds).

        Returns
        -------
        bool
            Returns True if the FTP server is ready, otherwise raises a ConnectionError.
        """
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
        """
        Generates mock electricity consumption data and saves it as a CSV file.
        """
        mock_ftp_data_obj = MockFTPData()
        df = mock_ftp_data_obj.generate_mock_data()
        self.save_df(file_name=self.file_name, df=df)
        print(df)

    def read_iex_data(self):
        """
        Retrieves market price data from the IEX website and saves it as a CSV file.
        """
        iex_webscrap = IEXWebScrap()
        market_price_df = iex_webscrap.api_invoke()
        self.save_df(file_name="iex_data.csv", df=market_price_df, index=True)

    def save_df(self, file_name, df, index=False):
        """
        Saves a pandas DataFrame to a CSV file.

        Parameters
        ----------
        file_name : str
            The name of the file where data will be saved.
        df : pandas.DataFrame
            The DataFrame containing data to be saved.
        index : bool, optional
            Whether to include the index in the saved CSV file (default is False).
        """
        print(f"file name : {file_name}")
        print(f"df : {df}")
        df.to_csv(file_name, index=index)

    def read_pv_watt(self):
        """
        Retrieves and downloads PV watt data from the PVWatts website.
        """
        pv_watt_obj = PVWattDataRead()
        pv_watt_obj.pv_watt_read_data()

    def ftp_process(self):
        """
        Manages the FTP process by starting a server, uploading, and downloading a data file.
        """
        mock_ftp_obj = MockFTP()

        # Start FTP server in a separate thread
        ftp_server_thread = threading.Thread(target=mock_ftp_obj.start_ftp_server)
        ftp_server_thread.daemon = (
            True  # Ensures the server stops when the main program stops
        )
        ftp_server_thread.start()
        time.sleep(10)  # Allow time for the server to start
        self.wait_for_ftp()

        # Upload and download data via FTP
        mock_ftp_obj.ftp_upload(file_name=self.file_relative_path)
        elect_data_df = mock_ftp_obj.ftp_download(file_name=self.file_name)
        self.save_df(file_name=self.file_name, df=elect_data_df)

    def runner(self):
        """
        Executes the main workflow including mock data preparation, FTP operations, and web scraping.
        """
        self.prepare_mock_data()
        # self.ftp_process()  # Uncomment if FTP operations are needed
        self.read_iex_data()
        self.read_pv_watt()


if __name__ == "__main__":
    obj = BronzeZone()
    obj.runner()

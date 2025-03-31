import pandas as pd
import requests
from bs4 import BeautifulSoup


class IEXWebScrap:
    """
    A class to scrape real-time market data from the Indian Energy Exchange (IEX) website.
    """

    def __init__(self):
        """
        Initialize the web scraper with target URL and headers.
        """
        # Target URL for market snapshot data
        self.url = (
            "https://www.iexindia.com/market-data/real-time-market/market-snapshot"
        )

        # Headers to mimic a real browser request
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

    def api_invoke(self):
        """
        Fetch market snapshot data from the IEX website and parse it into a Pandas DataFrame.

        Returns
        -------
        pd.DataFrame or None
            A DataFrame containing the extracted table data if successful, otherwise None.
        """
        # Send HTTP request to fetch the webpage
        response = requests.get(url=self.url, headers=self.headers)

        # Verify successful response
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # Locate the table container
            table_container = soup.find(
                "div", class_="MuiTableContainer-root mui-1x1it2d"
            )

            if table_container:
                # Find the table inside the container
                table = table_container.find("table")

                if table:
                    # Extract table headers
                    headers = [th.text.strip() for th in table.find_all("th")]

                    # Extract table rows
                    data = []
                    for row in table.find_all("tr")[1:]:  # Skip header row
                        cols = [td.text.strip() for td in row.find_all("td")]
                        if cols:
                            data.append(cols)

                    # Convert extracted data to Pandas DataFrame
                    df = pd.DataFrame(data, columns=headers)
                    return df
                else:
                    print("Table not found inside the container.")
            else:
                print("Table container not found.")
        else:
            print(f"Failed to fetch the page. Status Code: {response.status_code}")

        return None

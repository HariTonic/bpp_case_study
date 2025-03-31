import requests
from bs4 import BeautifulSoup
import pandas as pd


class IEXWebScrap:

    def __init__(self):
        # Target URL
        self.url = "https://www.iexindia.com/market-data/real-time-market/market-snapshot"

        # Set headers to mimic a real browser
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

    def api_invoke(self):
        # Fetch the webpage
        response = requests.get(url = self.url, headers=self.headers)\


        # Check if the request was successful
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # Find the div container
            table_container = soup.find("div", class_="MuiTableContainer-root mui-1x1it2d")

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

                    # Convert to Pandas DataFrame
                    df = pd.DataFrame(data, columns=headers)
                    return df
                else:
                    print("Table not found inside the container.")
            else:
                print("Table container not found.")
        else:
            print(f"Failed to fetch the page. Status Code: {response.status_code}")

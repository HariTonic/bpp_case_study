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
        self.url = "https://www.iexindia.com/market-data/real-time-market/market-snapshot"

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
        response = requests.get(url=self.url, headers=self.headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            table_container = soup.find("div", class_="MuiTableContainer-root mui-1x1it2d")

            if table_container:
                table = table_container.find("table")

                if table:
                    headers = [th.text.strip() for th in table.find_all("th")]

                    data = []
                    rowspan_dict = {}  # To store rowspans

                    for row in table.find_all("tr"):
                        row_data = []
                        cols = row.find_all(["td", "th"])

                        col_index = 0
                        for col in cols:
                            # Handle previous rowspans
                            while col_index in rowspan_dict and rowspan_dict[col_index]["count"] > 0:
                                row_data.append(rowspan_dict[col_index]["value"])
                                rowspan_dict[col_index]["count"] -= 1
                                col_index += 1

                            cell_text = col.get_text(strip=True)
                            row_data.append(cell_text)

                            # Handle rowspan
                            if col.has_attr("rowspan"):
                                rowspan_dict[col_index] = {
                                    "count": int(col["rowspan"]) - 1,  # Convert to integer
                                    "value": cell_text,
                                }

                            col_index += 1

                        # Fill remaining columns from rowspan_dict
                        while col_index in rowspan_dict and rowspan_dict[col_index]["count"] > 0:
                            row_data.append(rowspan_dict[col_index]["value"])
                            rowspan_dict[col_index]["count"] -= 1
                            col_index += 1

                        data.append(row_data)

                    df = pd.DataFrame(data[1:], columns=headers)
                    return df
                else:
                    print("Table not found inside the container.")
            else:
                print("Table container not found.")
        else:
            print(f"Failed to fetch the page. Status Code: {response.status_code}")

        return None


# # Instantiate and run
# scraper = IEXWebScrap()
# df = scraper.api_invoke()

# if df is not None:
#     print(df.head())  # Print first few rows
# else:
#     print("No data extracted.")

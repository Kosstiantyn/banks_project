import requests
from bs4 import BeautifulSoup

# Fetch the webpage
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
page = requests.get(url).text

# Parse the HTML
soup = BeautifulSoup(page, "html.parser")

# Find all tables on the page
tables = soup.find_all('table')

# Print the number of tables found
print(f"Number of tables found: {len(tables)}")

# Print the content of each table for inspection
for i, table in enumerate(tables):
   print(f"Index tables: {i}:")
   print(table.prettify()[:500])# Print the first 500 characters for a preview

# Optionally, you can print the entire content of a specific table to inspect it
if len(tables) > 2:
    print("\nContent of the third table (index 2):")
    print(tables[0].prettify())
else:
    print("The expected table index 2 does not exist on the webpage.")

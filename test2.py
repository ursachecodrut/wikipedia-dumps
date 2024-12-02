import bz2
import xml.etree.ElementTree as ET
import requests
from datetime import datetime, timedelta

WIKIPEDIA_API_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"

def extract_titles_from_dump(file_path, limit=10):
    """Extract a limited number of titles from Wikipedia dump."""
    titles = []
    with bz2.open(file_path, 'rt', encoding='utf-8') as file:
        context = ET.iterparse(file, events=("start", "end"))
        context = iter(context)
        
        event, root = next(context)  # Get the root element

        for event, elem in context:
            if event == "end" and elem.tag.endswith("page"):
                title = elem.find("./{*}title").text
                titles.append(title)
                root.clear()
                if len(titles) >= limit:
                    break
                
    return titles

    
def fetch_pageviews(title, start_date, end_date, project="simple.wikipedia"):
    """Fetch pageview data for a given page title using Wikimedia Pageviews API."""
    headers = {
        "User-Agent": "TestScript/1.0 aicitrebuiemailultau@gmail.com"
    }
    response = requests.get(
        f"{WIKIPEDIA_API_URL}/{project}/all-access/all-agents/{title}/monthly/{start_date}/{end_date}",
        headers=headers
    )
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {title}: {response.status_code}")
        return None


def process_wikipedia_dump_and_pageviews(dump_file):
    """Main function to process dump and get pageviews."""
    # Extract a limited number of titles
    print("Extracting titles from dump...")
    titles = extract_titles_from_dump(dump_file, limit=10)
    
    # Define date range for the last 6 months
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=30*6)).strftime("%Y%m%d")
    
    print("Fetching pageviews for 10 pages...")
    for title in titles:
        print(f"Processing title: {title}")
        data = fetch_pageviews(title, start_date, end_date)
        if data:
            total_views = sum(item["views"] for item in data["items"])
            print(f"Page: {title} - Total Views (Last 6 Months): {total_views}")
            print("-" * 40)

if __name__ == "__main__":
    DUMP_FILE = "simplewiki-20240901-pages-articles-multistream.xml.bz2"
    process_wikipedia_dump_and_pageviews(DUMP_FILE)

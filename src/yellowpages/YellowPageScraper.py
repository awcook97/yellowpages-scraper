import csv
import math
import argparse
from typing import Any
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote_plus
from playwright.sync_api import sync_playwright, Browser
from pathvalidate import sanitize_filepath
from .contact_info_finder import get_sites_from_csv



class YellowPageScraper:
    """A class to scrape business listings from YellowPages.com based on search terms and geographical location.
    Args:
            search_terms (str): the keyword you want to use on YellowPages.com
            geo_location_terms (str): The geographical location terms for the search.
            start_page (int, optional): The page to start on. Defaults to 1.
    Attributes:
        search_terms (str): The search terms for the businesses.
        geo_location_terms (str): The geographical location terms for the search.
        start_page (int): The starting page number for scraping. Defaults to 1.
        file_path (str): The file path where the scraped data will be saved.
        base_url (str): The base URL of YellowPages.com.
        current_page (int): The current page number being scraped.
        max_page (int): The maximum page number to scrape, updated based on the result count.
        fieldnames (list): The list of field names for the CSV file.
    Methods:
        scrape_all_pages() -> None:
            Scrapes all pages of business listings starting from the specified start page."""
    def __init__(self, search_terms: str, geo_location_terms: str, start_page: int = 1) -> None:
        """Initialize the YellowPageScraper class with the search terms and geo location terms.
        Use the scrape_all_pages method to start scraping the data.
        
        After initializing the class, you can call the scrape_all_pages method to start scraping the data.

        Args:
            search_terms (str): the keyword you want to use on YellowPages.com
            geo_location_terms (str): The geographical location terms for the search.
            start_page (int, optional): The page to start on. Defaults to 1.
        """
        self.search_terms: str = search_terms
        self.geo_location_terms: str = geo_location_terms
        self.start_page: int = start_page
        self.file_path = sanitize_filepath(
            f"output/{search_terms}{geo_location_terms}.csv"
        )
        self.base_url: str = "https://www.yellowpages.com"
        self.current_page: int = start_page
        self.max_page: int = 1  # Will be updated based on the result count
        self.fieldnames = [
            "Rank",
            "Business Name",
            "Phone Number",
            "Business Page",
            "Website",
            "Category",
            "Rating",
            "Street Name",
            "Locality",
            "Region",
            "Zipcode",
        ]

    def extract_business_listing(self, card) -> dict[str, Any]:
        # Extract business details using BeautifulSoup selectors
        rank_elem = card.select_one(".info-primary h2")
        rank = rank_elem.text.strip().split(". ")[0] if rank_elem else ""

        business_name_elem = card.select_one(".business-name span")
        business_name = business_name_elem.text.strip() if business_name_elem else ""

        phone_elem = card.select_one(".phones")
        phone_number = phone_elem.text.strip() if phone_elem else ""

        business_page_elem = card.select_one(".business-name")
        business_page = (
            self.base_url + business_page_elem["href"] if business_page_elem else ""
        )

        website_elem = card.select_one(".track-visit-website")
        website = website_elem["href"] if website_elem else ""

        category_elems = card.select(".categories a")
        category = (
            ", ".join(a.text.strip() for a in category_elems) if category_elems else ""
        )

        rating_elem = card.select_one(".ratings .count")
        rating = rating_elem.text.strip("()") if rating_elem else ""

        street_elem = card.select_one(".street-address")
        street_name = street_elem.text.strip() if street_elem else ""

        locality_elem = card.select_one(".locality")
        locality_text = locality_elem.text.strip() if locality_elem else ""

        # Attempt to split locality into locality, region, and zipcode
        locality, region, zipcode = "", "", ""
        if locality_text:
            parts = locality_text.split(",")
            if len(parts) >= 2:
                locality = parts[0].strip()
                region_zip = parts[1].strip().split()
                if len(region_zip) >= 2:
                    region = region_zip[0]
                    zipcode = region_zip[1]
                elif len(region_zip) == 1:
                    region = region_zip[0]
            else:
                locality = locality_text

        business_info = {
            "Rank": rank,
            "Business Name": business_name,
            "Phone Number": phone_number,
            "Business Page": business_page,
            "Website": website,
            "Category": category,
            "Rating": rating,
            "Street Name": street_name,
            "Locality": locality,
            "Region": region,
            "Zipcode": zipcode,
        }
        return business_info

    def save_to_csv(self, data_list: list) -> None:
        # Check if the file exists to decide if header is needed
        file_exists = False
        try:
            with open(self.file_path, "r", encoding="utf-8"):
                file_exists = True
        except FileNotFoundError:
            pass

        with open(self.file_path, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(data_list)

    def parse_page(self, content) -> None:
        soup = BeautifulSoup(content, "html.parser")
        all_cards = soup.select(".organic .srp-listing")
        result_count_elem = soup.select_one(".showing-count")
        result_count = 0
        if result_count_elem:
            try:
                # The text might look like "Showing 1-30 of 120" – we assume the last number is the total
                parts = result_count_elem.text.strip().replace("More info", "").split()
                result_count = int(parts[-1])
            except Exception as e:
                print("Error parsing result count:", e)

        # Update max_page based on total results (assuming 30 results per page)
        if result_count:
            self.max_page = math.ceil(result_count / 30)
        else:
            self.max_page = self.current_page

        data_list = []
        if all_cards:
            for card in all_cards:
                info = self.extract_business_listing(card)
                data_list.append(info)
            self.save_to_csv(data_list)
        else:
            print("No listings found on the page.")

    def scrape_all_pages(self) -> str:
        with sync_playwright() as p:
            # Launch a Firefox browser instance (set headless=True for headless mode)
            browser: Browser = p.firefox.launch(
                headless=True,
                args=["--start-maximized"],
                downloads_path="output",
                ignore_default_args=[
                    "--mute-audio",
                    "--hide-scrollbars",
                    "--disable-infobars",
                    "--disable-notifications",
                    "--disable-dev-shm-usage",
                    "--disable-webgl",
                    "--disable-xss-auditor",
                    "--disable-accelerated-2d-canvas",
                ],
            )
            context = browser.new_context()
            page = context.new_page()

            self.current_page = self.start_page
            while self.current_page <= self.max_page:
                print(f"Scraping data for page {self.current_page}...")

                # Build the URL with query parameters
                params = {
                    "search_terms": self.search_terms,
                    "geo_location_terms": self.geo_location_terms,
                    "page": self.current_page,
                }
                url = (
                    self.base_url + "/search?" + urlencode(params, quote_via=quote_plus)
                )
                page.goto(url, wait_until="domcontentloaded")

                # Optionally, you can wait for specific elements to appear:
                page.wait_for_selector(".organic .srp-listing", timeout=30000)

                content = page.content()
                self.parse_page(content)
                print(f"Page {self.current_page} scraped successfully.")
                self.current_page += 1

            browser.close()
        return self.file_path


def main():
    parser = argparse.ArgumentParser(
        description="Yellow Pages Scraper using Playwright"
    )
    parser.add_argument("search_terms", type=str, help="Search terms for scraping")
    parser.add_argument(
        "geo_location_terms", type=str, help="Geographical location terms for scraping"
    )
    parser.add_argument(
        "--start_page", type=int, default=1, help="Start page number (default: 1)"
    )
    # parser.add_argument(
    #     "--filename",
    #     type=str,
    #     default="business_data321.csv",
    #     help="CSV filename (default: business_data321.csv)",
    # )
    parser.add_argument(
        "--emails",
        action="store_true",
        default=False,
        required=False,
        help="Find emails for the websites scraped",
    )
    
    args = parser.parse_args()

    scraper = YellowPageScraper(
        args.search_terms, args.geo_location_terms, args.start_page
    )
    scraper.scrape_all_pages()
    if args.emails:
        find_emails(scraper.file_path)




def json_to_csv(data, output_file) -> None:
    # data = json.loads(json_data)  # Parse JSON string if needed

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["website", "emails"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for entry in data["results"]:
            writer.writerow(
                {
                    "website": entry["website"],
                    "emails": ", ".join(entry["emails"]),  # Convert list to string
                    # "social_links": ", ".join(entry["social_links"])
                }
            )


def find_emails(file_to_read: str, file_to_write: str | None = None) -> None:
    websites = get_sites_from_csv(csv_input_file=file_to_read, csv_output_file=file_to_write)
    if not websites:
        print("No websites found in the CSV file.")
        return

if __name__ == "__main__":
    main()

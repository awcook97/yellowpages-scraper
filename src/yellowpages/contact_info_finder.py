import asyncio
from typing import Any, Hashable
import aiohttp
import re
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import dns.resolver

# Regular expression for matching emails.
EMAIL_REGEX = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")

# Social media domains to look for.
SOCIAL_DOMAINS = [
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
]


def is_clean_email(email: str) -> bool:
    """
    Check if an email address looks like a valid contact email.
    For example, filter out emails whose domain part contains no alphabetic
    characters (which often indicates a package/version string).
    """
    try:
        local, domain = email.rsplit("@", 1)
        if "gmail" in domain:
            return True
        # Require that the domain contains at least one alphabetic character.
        if not any(c.isalpha() for c in domain):
            return False
        return True
    except Exception as e:
        print(f"Error cleaning email '{email}': {e}")
        return False


def extract_contact_info(html: str, base_url: str) -> dict:
    """
    Given HTML content and its base URL, extract emails and social media links.
    Returns a dictionary with lists for each contact type.
    """
    emails = set(EMAIL_REGEX.findall(html))
    social_links = set()

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]  # type: ignore
        # Convert relative URLs to absolute.
        absolute_href = urljoin(base_url, href)  # type: ignore
        # Check if this link is a social media link.
        for domain in SOCIAL_DOMAINS:
            if domain in absolute_href:
                social_links.add(absolute_href)
                break

    return {"emails": list(emails), "social_links": list(social_links)}


def find_contact_page_links(html: str, base_url: str) -> list:
    """
    Search for links in the HTML that might lead to a contact page.
    This function looks for anchor tags that contain the word "contact"
    in the text or href.
    """
    soup = BeautifulSoup(html, "html.parser")
    contact_urls = set()

    for a in soup.find_all("a", href=True):
        text = a.get_text().strip().lower()
        href = a["href"].lower()  # type: ignore
        if "contact" in text or "contact" in href:
            full_url = urljoin(base_url, a["href"])  # type: ignore
            contact_urls.add(full_url)

    return list(contact_urls)


async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    """Fetch page content for a given URL asynchronously."""
    try:
        async with session.get(url, timeout=15) as response:  # type: ignore
            if response.status == 200:
                return await response.text()
            else:
                print(f"Non-200 status code {response.status} for URL: {url}")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return ""


async def process_website(session: aiohttp.ClientSession, website: str) -> dict | None:
    """
    Process a single website: fetch its homepage, extract contact info,
    then look for a contact page, fetch it, and merge any contact info found.
    If no contact info (emails or social media links) is found or the site is unreachable,
    returns None.
    """
    print(f"Processing website: {website}")
    contact_emails = set()
    contact_social_links = set()

    # Ensure website URL starts with http/https.
    if not website.startswith(("http://", "https://")):
        website = "http://" + website

    homepage_content = await fetch(session, website)
    if not homepage_content:
        print(f"Failed to fetch homepage for: {website}")
        return None

    # Extract info from homepage.
    info = extract_contact_info(homepage_content, website)
    contact_emails.update(info["emails"])
    contact_social_links.update(info["social_links"])

    # Look for potential contact page links.
    contact_page_urls = find_contact_page_links(homepage_content, website)
    if not contact_page_urls:
        # Try a common pattern '/contact' if none found.
        contact_page_urls.append(urljoin(website, "contact"))

    # Process each found contact page.
    for contact_url in contact_page_urls:
        print(f"  Fetching contact page: {contact_url}")
        contact_content = await fetch(session, contact_url)
        if contact_content:
            contact_info = extract_contact_info(contact_content, contact_url)
            contact_emails.update(contact_info["emails"])
            contact_social_links.update(contact_info["social_links"])

    # Discard this website if no contact info was found.
    if not contact_emails and not contact_social_links:
        print(f"No contact info found for: {website}")
        return None

    return {
        "website": website,
        "emails": list(contact_emails),
        "social_links": list(contact_social_links),
    }


def has_mx_record(domain: str) -> bool:
    """
    Check if the given domain has MX records.
    Returns True if one or more MX records are found, otherwise False.
    """
    try:
        answers = dns.resolver.resolve(domain, "MX")
        return bool(answers)
    except Exception:
        # Uncomment the following line to see detailed DNS errors:
        # print(f"DNS lookup failed for domain {domain}: {e}")
        return False


async def verify_email(email: str) -> bool:
    """
    Verify an email address by checking that its domain has MX records.
    This function runs the blocking DNS lookup in a separate thread.
    """
    try:
        domain = email.split("@")[-1]
        return await asyncio.to_thread(has_mx_record, domain)
    except Exception as e:
        print(f"Error verifying email {email}: {e}")
        return False


async def verify_emails_in_results(results: list) -> list:
    """
    For each website result, clean and verify all emails asynchronously.
    Only keep emails that pass both the cleaning check and verification.
    If a website ends up with no verified emails, it is discarded.
    """
    verified_results = []
    for result in results:
        emails = result.get("emails", [])
        if not emails:
            continue

        # First, filter emails based on our cleaning criteria.
        cleaned_emails = [email for email in emails if is_clean_email(email)]
        if not cleaned_emails:
            print(f"No clean emails found for website: {result['website']}")
            continue

        # Create verification tasks for all cleaned emails.
        tasks = [verify_email(email) for email in cleaned_emails]
        verification_outcomes = await asyncio.gather(*tasks)

        # Keep only emails that passed verification.
        verified_emails = [
            email
            for email, valid in zip(cleaned_emails, verification_outcomes)
            if valid
        ]
        if verified_emails:
            result["emails"] = verified_emails
            verified_results.append(result)
        else:
            print(f"All emails failed verification for website: {result['website']}")

    return verified_results


async def _get_sites_from_csv(
    csv_input_file: str, csv_output_file: str | None
) -> dict[Hashable, Any] | None:
    # Read the spreadsheet containing websites.
    # Adjust the filename and column name as needed.
    if not csv_output_file:
        csv_output_file = f"{csv_input_file.split('.')[0]}_emails.csv"
    df = pd.read_csv(csv_input_file)
    websites = df["Website"].dropna().tolist()

    scrape_results = []
    connector = aiohttp.TCPConnector(limit_per_host=10)

    async with aiohttp.ClientSession(connector=connector) as session:
        # Create scraping tasks.
        scrape_tasks = [process_website(session, website) for website in websites]
        for future in asyncio.as_completed(scrape_tasks):
            try:
                result = await future
                if result is not None:
                    scrape_results.append(result)
                    print(f"Finished processing: {result['website']}")
            except Exception as e:
                print(f"Error processing a website: {e}")

        # Now verify (and clean) the emails in the scraped results.
        final_results = await verify_emails_in_results(scrape_results)

    # Save final results (only websites with at least one verified email) to a CSV file.
    if final_results:
        results_df = pd.DataFrame(final_results)
        results_df.to_csv(csv_output_file, index=False)
        return results_df.to_dict("dict")
    else:
        print("No websites with verified email contact info were found.")


def get_sites_from_csv(
    csv_input_file: str, csv_output_file: str | None = None
) -> dict[Hashable, Any] | None:
    if not csv_output_file:
        csv_output_file = f"{csv_input_file.split('.')[0]}_emails.csv"
    return asyncio.run(_get_sites_from_csv(csv_input_file, csv_output_file))


import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from tld import get_fld
import validators


class EmailScraper:
    def __init__(self):
        self.email_sources = {}  # Dict to store email -> source page mapping
        self.visited_urls = set()

    def is_valid_url(self, url):
        try:
            return validators.url(url)
        except:
            return False

    def extract_emails(self, text, source_url):
        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        emails = re.findall(email_pattern, text)
        for email in emails:
            if email not in self.email_sources:
                self.email_sources[email] = source_url
        return emails

    def find_contact_links(self, soup, base_url):
        contact_keywords = [
            "contact",
            "about",
            "about-us",
            "contact-us",
            "reach",
            "connect",
        ]
        links = soup.find_all("a", href=True)
        contact_links = []

        for link in links:
            href = link.get("href").lower()
            text = link.text.lower()
            if any(keyword in href or keyword in text for keyword in contact_keywords):
                full_url = urljoin(base_url, href)
                if self.is_valid_url(full_url):
                    contact_links.append(full_url)
        return contact_links

    def scrape_page(self, url):
        if not self.is_valid_url(url):
            return []

        if url in self.visited_urls:
            return []

        self.visited_urls.add(url)
        page_emails = []

        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract emails from current page
            page_emails.extend(self.extract_emails(response.text, url))

            # Find and scrape contact pages
            contact_links = self.find_contact_links(soup, url)
            for contact_url in contact_links:
                if contact_url not in self.visited_urls:
                    page_emails.extend(self.scrape_page(contact_url))

        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")

        return page_emails

    def scrape_multiple_sites(self, urls):
        all_results = []
        for url in urls:
            try:
                self.email_sources = {}  # Reset for each main website
                base_url = url if url.startswith("http") else f"https://{url}"
                domain = (
                    get_fld(base_url, fail_silently=True) or urlparse(base_url).netloc
                )
                emails = self.scrape_page(base_url)
                if emails:
                    email_data = []
                    for email in set(emails):
                        email_data.append(
                            {
                                "email": email,
                                "source_page": self.email_sources.get(email, base_url),
                            }
                        )
                    all_results.append(
                        {"domain": domain, "url": base_url, "email_data": email_data}
                    )
            except Exception as e:
                print(f"Error processing {url}: {str(e)}")

        return all_results

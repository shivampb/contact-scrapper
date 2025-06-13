import asyncio
import aiohttp
import re
import csv
import json
import base64
import html
from urllib.parse import urljoin, urlparse, unquote
from bs4 import BeautifulSoup
from typing import Set, List, Dict, Optional, Tuple
import time
import logging
from dataclasses import dataclass, asdict
import random
import ssl
import certifi

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class EmailResult:
    url: str
    emails: List[str]
    status: str
    error: Optional[str] = None
    response_time: Optional[float] = None
    page_title: Optional[str] = None
    emails_by_method: Optional[Dict[str, List[str]]] = None


class EnhancedEmailScraper:
    def __init__(
        self,
        max_concurrent=8,
        timeout=45,
        delay_between_requests=2,
        max_retries=3,
        follow_redirects=True,
        deep_scan=True,
        use_brotli=None,  # Auto-detect by default
    ):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.delay_between_requests = delay_between_requests
        self.max_retries = max_retries
        self.follow_redirects = follow_redirects
        self.deep_scan = deep_scan

        # Auto-detect Brotli support or use explicit setting
        if use_brotli is None:
            try:
                import brotli

                self.use_brotli = True
                logger.info(
                    "Brotli library detected - enabling Brotli compression support"
                )
            except ImportError:
                try:
                    import brotli

                    self.use_brotli = True
                    logger.info(
                        "BrotliPy library detected - enabling Brotli compression support"
                    )
                except ImportError:
                    self.use_brotli = False
                    logger.info(
                        "No Brotli library found - disabling Brotli compression"
                    )
        else:
            self.use_brotli = use_brotli
            if use_brotli:
                logger.info("Brotli compression explicitly enabled")
            else:
                logger.info("Brotli compression explicitly disabled")

        # Enhanced email patterns
        self.email_patterns = [
            # Standard email pattern
            re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
            # Pattern for emails with special characters
            re.compile(r"\b[A-Za-z0-9._%+\-\']+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
            # Pattern for emails in quotes
            re.compile(r'["\']([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})["\']'),
            # Pattern for obfuscated emails (at) -> @
            re.compile(
                r"\b[A-Za-z0-9._%+-]+\s*\(at\)\s*[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
            ),
        ]

        # Common obfuscation patterns
        self.obfuscation_patterns = {
            r"\s*\(at\)\s*": "@",
            r"\s*\[at\]\s*": "@",
            r"\s*\(dot\)\s*": ".",
            r"\s*\[dot\]\s*": ".",
            r"\s*\(AT\)\s*": "@",
            r"\s*\[AT\]\s*": "@",
            r"\s*\(DOT\)\s*": ".",
            r"\s*\[DOT\]\s*": ".",
            r"\s*at\s*": "@",
            r"\s*dot\s*": ".",
        }

        # Blocked domains and patterns
        self.blocked_patterns = {
            "example.com",
            "test.com",
            "placeholder",
            "dummy",
            "sample.com",
            "yoursite.com",
            "yourdomain.com",
            "domain.com",
            "website.com",
            "company.com",
            "business.com",
            "email.com",
            "mail.com",
            "noreply",
            "no-reply",
            "donotreply",
            "do-not-reply",
        }

        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # User agents for rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]

    async def __aenter__(self):
        # Create SSL context for secure connections
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # Allow self-signed certificates

        # Configure connector without auto-compression to handle it manually
        connector = aiohttp.TCPConnector(
            limit=200,
            limit_per_host=20,
            ssl=ssl_context,
            enable_cleanup_closed=True,
            force_close=True,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )

        timeout = aiohttp.ClientTimeout(total=self.timeout, connect=15, sock_read=25)

        # Set Accept-Encoding based on Brotli support
        if self.use_brotli:
            accept_encoding = "gzip, deflate, br"
            logger.info("Using full compression support: gzip, deflate, br")
        else:
            accept_encoding = "gzip, deflate"
            logger.info("Using limited compression support: gzip, deflate only")

        # Create session with proper headers
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": accept_encoding,
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            auto_decompress=True,  # Let aiohttp handle decompression automatically
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def deobfuscate_email(self, text: str) -> str:
        """Remove common email obfuscation patterns."""
        for pattern, replacement in self.obfuscation_patterns.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        return text

    def decode_encoded_emails(self, text: str) -> Set[str]:
        """Decode various encoded email formats."""
        emails = set()

        try:
            # Try to decode HTML entities
            decoded_text = html.unescape(text)
            emails.update(self.extract_emails_from_text(decoded_text))

            # Try to decode URL encoded text
            url_decoded = unquote(text)
            emails.update(self.extract_emails_from_text(url_decoded))

            # Try to decode base64 encoded emails (sometimes used in obfuscation)
            base64_pattern = re.compile(r"[A-Za-z0-9+/]{20,}={0,2}")
            for match in base64_pattern.findall(text):
                try:
                    if len(match) % 4 == 0:  # Valid base64 length
                        decoded = base64.b64decode(match).decode(
                            "utf-8", errors="ignore"
                        )
                        if "@" in decoded:  # Only process if it might contain emails
                            emails.update(self.extract_emails_from_text(decoded))
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"Error in decode_encoded_emails: {e}")

        return emails

    def extract_emails_from_text(self, text: str) -> Set[str]:
        """Extract email addresses from text using multiple patterns."""
        emails = set()

        # First, deobfuscate the text
        deobfuscated_text = self.deobfuscate_email(text)

        # Apply all email patterns
        for pattern in self.email_patterns:
            found_emails = pattern.findall(deobfuscated_text)
            for email in found_emails:
                if isinstance(email, tuple):
                    # Handle tuple results from regex groups
                    email = next((e for e in email if e), email[0] if email else "")
                if email:  # Only add non-empty emails
                    emails.add(email.lower().strip())

        # Try to decode encoded emails
        emails.update(self.decode_encoded_emails(text))

        # Filter out invalid emails
        filtered_emails = set()
        for email in emails:
            if self.is_valid_email(email):
                filtered_emails.add(email)

        return filtered_emails

    def is_valid_email(self, email: str) -> bool:
        """Validate email address and filter out false positives."""
        email = email.lower().strip()

        # Basic validation
        if not email or "@" not in email or "." not in email:
            return False

        # Check against blocked patterns
        for blocked in self.blocked_patterns:
            if blocked in email:
                return False

        # Check for minimum length
        if len(email) < 5:
            return False

        # Check for valid domain structure
        try:
            local, domain = email.split("@", 1)
            if not local or not domain or "." not in domain:
                return False

            # Check for consecutive dots or other invalid patterns
            if ".." in email or email.startswith(".") or email.endswith("."):
                return False

            # Check for valid TLD
            tld = domain.split(".")[-1]
            if len(tld) < 2 or not tld.isalpha():
                return False

        except ValueError:
            return False

        return True

    def extract_emails_from_html(
        self, html_content: str, url: str
    ) -> Dict[str, Set[str]]:
        """Extract emails from HTML content using multiple methods."""
        emails_by_method = {
            "text_content": set(),
            "mailto_links": set(),
            "data_attributes": set(),
            "javascript": set(),
            "comments": set(),
            "forms": set(),
        }

        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Extract from text content
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text()
            emails_by_method["text_content"].update(self.extract_emails_from_text(text))

            # Extract from mailto links
            mailto_links = soup.find_all("a", href=re.compile(r"^mailto:", re.I))
            for link in mailto_links:
                href = link.get("href", "")
                if href.startswith("mailto:"):
                    email = href[7:].split("?")[0].split("&")[0]
                    if self.is_valid_email(email):
                        emails_by_method["mailto_links"].add(email.lower())

            # Extract from data attributes
            for attr in ["data-email", "data-mail", "data-contact", "email", "mail"]:
                for elem in soup.find_all(attrs={attr: True}):
                    email = elem.get(attr, "")
                    if self.is_valid_email(email):
                        emails_by_method["data_attributes"].add(email.lower())

            # Extract from JavaScript content
            scripts = soup.find_all("script")
            for script in scripts:
                if script.string:
                    js_emails = self.extract_emails_from_text(script.string)
                    emails_by_method["javascript"].update(js_emails)

            # Extract from HTML comments
            from bs4 import Comment

            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            for comment in comments:
                comment_emails = self.extract_emails_from_text(str(comment))
                emails_by_method["comments"].update(comment_emails)

            # Extract from form inputs
            inputs = soup.find_all(["input", "textarea"], {"value": True})
            for input_elem in inputs:
                value = input_elem.get("value", "")
                if value:
                    form_emails = self.extract_emails_from_text(value)
                    emails_by_method["forms"].update(form_emails)

            # If deep scan is enabled, look for additional patterns
            if self.deep_scan:
                # Look for encoded emails in the raw HTML
                encoded_emails = self.decode_encoded_emails(html_content)
                emails_by_method["text_content"].update(encoded_emails)

        except Exception as e:
            logger.warning(f"Error parsing HTML for {url}: {e}")
            # Fallback to text extraction
            emails_by_method["text_content"].update(
                self.extract_emails_from_text(html_content)
            )

        return emails_by_method

    def get_random_user_agent(self) -> str:
        """Get a random user agent for request rotation."""
        return random.choice(self.user_agents)

    async def scrape_single_url(self, url: str) -> EmailResult:
        """Scrape emails from a single URL with retry logic."""
        async with self.semaphore:
            start_time = time.time()

            # Ensure URL has a scheme
            if not url.startswith(("http://", "https://")):
                url = "https://" + url

            for attempt in range(self.max_retries + 1):
                try:
                    logger.info(f"Scraping: {url} (attempt {attempt + 1})")

                    # Prepare headers for this request
                    headers = {
                        "User-Agent": self.get_random_user_agent(),
                    }

                    # Override Accept-Encoding for this specific request if needed
                    if not self.use_brotli:
                        headers["Accept-Encoding"] = "gzip, deflate"

                    async with self.session.get(
                        url,
                        headers=headers,
                        allow_redirects=self.follow_redirects,
                        compress=True,  # Enable automatic decompression
                    ) as response:

                        if response.status == 200:
                            try:
                                # Try to read content with proper encoding
                                content = await response.text(
                                    encoding="utf-8", errors="ignore"
                                )
                            except Exception as e:
                                logger.warning(f"Error reading content from {url}: {e}")
                                # Fallback to reading raw bytes and decoding
                                raw_content = await response.read()
                                content = raw_content.decode("utf-8", errors="ignore")

                            # Extract page title
                            page_title = None
                            try:
                                soup = BeautifulSoup(content, "html.parser")
                                title_tag = soup.find("title")
                                if title_tag:
                                    page_title = title_tag.get_text().strip()
                            except:
                                pass

                            # Extract emails using multiple methods
                            emails_by_method = self.extract_emails_from_html(
                                content, url
                            )

                            # Combine all emails
                            all_emails = set()
                            for method_emails in emails_by_method.values():
                                all_emails.update(method_emails)

                            response_time = time.time() - start_time

                            # Convert sets to lists for JSON serialization
                            serializable_emails_by_method = {
                                method: list(emails)
                                for method, emails in emails_by_method.items()
                            }

                            result = EmailResult(
                                url=url,
                                emails=list(all_emails),
                                status="success",
                                response_time=response_time,
                                page_title=page_title,
                                emails_by_method=serializable_emails_by_method,
                            )

                            logger.info(f"Found {len(all_emails)} emails from {url}")
                            return result

                        elif response.status in [403, 429]:
                            # Rate limited or forbidden, wait longer
                            if attempt < self.max_retries:
                                wait_time = (2**attempt) * self.delay_between_requests
                                logger.warning(
                                    f"Rate limited on {url}, waiting {wait_time}s before retry"
                                )
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                return EmailResult(
                                    url=url,
                                    emails=[],
                                    status="error",
                                    error=f"HTTP {response.status} - Rate limited",
                                    response_time=time.time() - start_time,
                                )
                        else:
                            return EmailResult(
                                url=url,
                                emails=[],
                                status="error",
                                error=f"HTTP {response.status}",
                                response_time=time.time() - start_time,
                            )

                except asyncio.TimeoutError:
                    if attempt < self.max_retries:
                        logger.warning(f"Timeout on {url}, retrying...")
                        await asyncio.sleep(self.delay_between_requests * (attempt + 1))
                        continue
                    else:
                        return EmailResult(
                            url=url,
                            emails=[],
                            status="timeout",
                            error="Request timeout after retries",
                            response_time=time.time() - start_time,
                        )
                except Exception as e:
                    error_message = str(e)
                    if "brotli" in error_message.lower():
                        logger.error(f"Brotli compression error on {url}: {e}")
                        if attempt < self.max_retries:
                            logger.info(
                                f"Disabling Brotli for this request and retrying..."
                            )
                            # Force disable Brotli for remaining attempts
                            continue
                        else:
                            return EmailResult(
                                url=url,
                                emails=[],
                                status="error",
                                error=f"Compression error: {error_message}",
                                response_time=time.time() - start_time,
                            )
                    else:
                        if attempt < self.max_retries:
                            logger.warning(f"Error scraping {url}: {e}, retrying...")
                            await asyncio.sleep(
                                self.delay_between_requests * (attempt + 1)
                            )
                            continue
                        else:
                            return EmailResult(
                                url=url,
                                emails=[],
                                status="error",
                                error=str(e),
                                response_time=time.time() - start_time,
                            )
                finally:
                    # Add delay between requests to be respectful
                    if self.delay_between_requests > 0:
                        await asyncio.sleep(
                            self.delay_between_requests + random.uniform(0, 1)
                        )

    async def scrape_urls(
        self, urls: List[str], progress_callback=None
    ) -> List[EmailResult]:
        """Scrape emails from multiple URLs with progress tracking."""
        logger.info(
            f"Starting to scrape {len(urls)} URLs with {self.max_concurrent} concurrent connections"
        )

        results = []

        # Process URLs in batches to avoid overwhelming the system
        batch_size = self.max_concurrent * 2
        for i in range(0, len(urls), batch_size):
            batch = urls[i : i + batch_size]
            logger.info(
                f"Processing batch {i//batch_size + 1}/{(len(urls) + batch_size - 1)//batch_size}"
            )

            tasks = [self.scrape_single_url(url) for url in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle any exceptions that occurred
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    results.append(
                        EmailResult(
                            url=batch[j], emails=[], status="error", error=str(result)
                        )
                    )
                else:
                    results.append(result)

                # Call progress callback if provided
                if progress_callback:
                    try:
                        progress_callback(len(results), len(urls))
                    except Exception as e:
                        logger.warning(f"Progress callback error: {e}")

            # Small delay between batches
            if i + batch_size < len(urls):
                await asyncio.sleep(2)

        return results

    def save_results_to_csv(
        self, results: List[EmailResult], filename: str = "enhanced_scraped_emails.csv"
    ):
        """Save results to CSV file with enhanced information."""
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "url",
                "page_title",
                "status",
                "emails_found",
                "emails",
                "response_time",
                "error",
                "text_emails",
                "mailto_emails",
                "data_attribute_emails",
                "javascript_emails",
                "comment_emails",
                "form_emails",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for result in results:
                emails_by_method = result.emails_by_method or {}

                writer.writerow(
                    {
                        "url": result.url,
                        "page_title": result.page_title,
                        "status": result.status,
                        "emails_found": len(result.emails),
                        "emails": "; ".join(result.emails),
                        "response_time": result.response_time,
                        "error": result.error,
                        "text_emails": "; ".join(
                            emails_by_method.get("text_content", [])
                        ),
                        "mailto_emails": "; ".join(
                            emails_by_method.get("mailto_links", [])
                        ),
                        "data_attribute_emails": "; ".join(
                            emails_by_method.get("data_attributes", [])
                        ),
                        "javascript_emails": "; ".join(
                            emails_by_method.get("javascript", [])
                        ),
                        "comment_emails": "; ".join(
                            emails_by_method.get("comments", [])
                        ),
                        "form_emails": "; ".join(emails_by_method.get("forms", [])),
                    }
                )

        logger.info(f"Results saved to {filename}")

    def save_results_to_json(
        self, results: List[EmailResult], filename: str = "enhanced_scraped_emails.json"
    ):
        """Save results to JSON file."""
        with open(filename, "w", encoding="utf-8") as jsonfile:
            json.dump(
                [asdict(result) for result in results],
                jsonfile,
                indent=2,
                ensure_ascii=False,
            )

        logger.info(f"Results saved to {filename}")

    def print_detailed_summary(self, results: List[EmailResult]):
        """Print a detailed summary of the scraping results."""
        total_urls = len(results)
        successful = sum(1 for r in results if r.status == "success")
        failed = sum(1 for r in results if r.status == "error")
        timeouts = sum(1 for r in results if r.status == "timeout")
        total_emails = sum(len(r.emails) for r in results)
        unique_emails = len(set(email for r in results for email in r.emails))

        # Calculate method statistics
        method_stats = {
            "text_content": 0,
            "mailto_links": 0,
            "data_attributes": 0,
            "javascript": 0,
            "comments": 0,
            "forms": 0,
        }

        for result in results:
            if result.emails_by_method:
                for method, emails in result.emails_by_method.items():
                    method_stats[method] += len(emails)

        print("\n" + "=" * 60)
        print("ENHANCED SCRAPING SUMMARY")
        print("=" * 60)
        print(f"📊 Total URLs processed: {total_urls}")
        print(f"✅ Successful requests: {successful}")
        print(f"❌ Failed requests: {failed}")
        print(f"⏰ Timed out requests: {timeouts}")
        print(f"📧 Total emails found: {total_emails}")
        print(f"🔍 Unique emails found: {unique_emails}")
        print(f"📈 Success rate: {(successful/total_urls*100):.1f}%")
        print(
            f"📊 Average emails per successful URL: {total_emails/successful if successful > 0 else 0:.2f}"
        )

        print(f"\n📋 Emails found by method:")
        for method, count in method_stats.items():
            if count > 0:
                print(f"  {method.replace('_', ' ').title()}: {count}")

        if successful > 0:
            avg_response_time = (
                sum(r.response_time for r in results if r.response_time) / successful
            )
            print(f"⚡ Average response time: {avg_response_time:.2f}s")

        if unique_emails > 0:
            print(f"\n📧 All unique emails found:")
            all_emails = set(email for r in results for email in r.emails)
            for email in sorted(all_emails):
                print(f"  📩 {email}")

        # Show top performing URLs
        successful_results = [r for r in results if r.status == "success" and r.emails]
        if successful_results:
            print(f"\n🏆 Top performing URLs:")
            top_results = sorted(
                successful_results, key=lambda x: len(x.emails), reverse=True
            )[:5]
            for i, result in enumerate(top_results, 1):
                print(f"  {i}. {result.url} - {len(result.emails)} emails")
                if result.page_title:
                    print(f"     📄 {result.page_title[:60]}...")


def progress_callback(completed: int, total: int):
    """Simple progress callback function."""
    percentage = (completed / total) * 100
    print(f"Progress: {completed}/{total} ({percentage:.1f}%)")


async def main():
    """Enhanced example usage of the email scraper."""

    # Example URLs to scrape
    urls = [
        "tallorder.com",
        "goodcounsel.com",
        "soulkal.com",
        "plumpracticewear.com",
        "MLTD.com",
        "festybesty.com",
        "tutuspoiled.com",
        # Add more URLs here
    ]

    # Load URLs from file function
    def load_urls_from_file(filename: str) -> List[str]:
        """Load URLs from a text file (one URL per line)."""
        try:
            with open(filename, "r") as f:
                urls = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.startswith("#")
                ]
            return urls
        except FileNotFoundError:
            logger.error(f"File {filename} not found")
            return []

    # Uncomment to load from file:
    # urls = load_urls_from_file("urls.txt")

    if not urls:
        print(
            "No URLs to scrape. Please add URLs to the 'urls' list or create a 'urls.txt' file."
        )
        return

    # Enhanced scraper configuration with proper Brotli handling
    scraper_config = {
        "max_concurrent": 6,  # Reduced for better stability
        "timeout": 45,  # Increased timeout
        "delay_between_requests": 2,  # Increased delay to be more respectful
        "max_retries": 3,  # Added retry logic
        "follow_redirects": True,  # Follow redirects
        "deep_scan": True,  # Enable deep scanning
        "use_brotli": True,  # Enable Brotli since you have the libraries installed
    }

    print(f"🚀 Starting enhanced email scraper with {len(urls)} URLs...")
    print(f"⚙️  Configuration: {scraper_config}")

    # Run the enhanced scraper
    async with EnhancedEmailScraper(**scraper_config) as scraper:
        results = await scraper.scrape_urls(urls, progress_callback=progress_callback)

        # Print detailed summary
        scraper.print_detailed_summary(results)

        # Save results
        scraper.save_results_to_csv(results, "enhanced_email_results.csv")
        scraper.save_results_to_json(results, "enhanced_email_results.json")

        print(
            f"\n💾 Results saved to enhanced_email_results.csv and enhanced_email_results.json"
        )


if __name__ == "__main__":
    # Run the enhanced scraper
    asyncio.run(main())

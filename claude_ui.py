import streamlit as st
import asyncio
import aiohttp
import re
import pandas as pd
import csv
import json
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import Set, List, Dict, Optional
import time
import logging
from dataclasses import dataclass, asdict
from io import StringIO
import plotly.express as px
import plotly.graph_objects as go

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


class SmartEmailScraper:
    def __init__(self, max_concurrent=10, timeout=30, delay_between_requests=1):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.delay_between_requests = delay_between_requests
        self.email_pattern = re.compile(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        )
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def extract_emails_from_text(self, text: str) -> Set[str]:
        """Extract email addresses from text using regex."""
        emails = set(self.email_pattern.findall(text))
        # Filter out common false positives
        filtered_emails = set()
        for email in emails:
            if not any(
                skip in email.lower()
                for skip in ["example.com", "test.com", "placeholder", "dummy"]
            ):
                filtered_emails.add(email.lower())
        return filtered_emails

    def extract_emails_from_html(self, html: str) -> Set[str]:
        """Extract emails from HTML content including mailto links."""
        emails = set()

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()

            # Get text content
            text = soup.get_text()
            emails.update(self.extract_emails_from_text(text))

            # Extract from mailto links
            mailto_links = soup.find_all("a", href=re.compile(r"^mailto:", re.I))
            for link in mailto_links:
                href = link.get("href", "")
                if href.startswith("mailto:"):
                    email = href[7:].split("?")[0]  # Remove mailto: and query params
                    if self.email_pattern.match(email):
                        emails.add(email.lower())

            # Extract from data attributes and hidden inputs
            for elem in soup.find_all(attrs={"data-email": True}):
                email = elem.get("data-email", "")
                if self.email_pattern.match(email):
                    emails.add(email.lower())

        except Exception as e:
            logger.warning(f"Error parsing HTML: {e}")
            # Fallback to text extraction
            emails.update(self.extract_emails_from_text(html))

        return emails

    def normalize_url(self, url: str) -> str:
        """Normalize and enhance URL to make it more likely to find emails."""
        # Remove whitespace
        url = url.strip()

        # Add https:// if no protocol
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        # Remove trailing slash
        url = url.rstrip("/")

        return url

    def generate_url_variants(self, base_url: str) -> List[str]:
        """Generate common URL variants that might contain emails."""
        variants = [base_url]

        # Parse the base URL
        parsed = urlparse(base_url)
        base_domain = f"{parsed.scheme}://{parsed.netloc}"

        # Common pages that often contain emails
        common_paths = [
            "/contact",
            "/contact-us",
            "/about",
            "/about-us",
            "/team",
            "/staff",
            "/people",
            "/leadership",
            "/management",
            "/directors",
            "/support",
            "/help",
            "/info",
            "/reach-us",
            "/get-in-touch",
        ]

        # Only add variants if the original URL is just a domain
        if parsed.path in ["", "/"]:
            for path in common_paths:
                variants.append(base_domain + path)

        return variants

    async def scrape_single_url(self, url: str) -> EmailResult:
        """Scrape emails from a single URL."""
        async with self.semaphore:
            start_time = time.time()

            try:
                # Normalize the URL
                original_url = url
                url = self.normalize_url(url)

                async with self.session.get(url, allow_redirects=True) as response:
                    if response.status == 200:
                        content = await response.text()
                        emails = list(self.extract_emails_from_html(content))
                        response_time = time.time() - start_time

                        result = EmailResult(
                            url=original_url,  # Keep original URL for display
                            emails=emails,
                            status="success",
                            response_time=response_time,
                        )

                        return result
                    else:
                        return EmailResult(
                            url=original_url,
                            emails=[],
                            status="error",
                            error=f"HTTP {response.status}",
                            response_time=time.time() - start_time,
                        )

            except asyncio.TimeoutError:
                return EmailResult(
                    url=original_url,
                    emails=[],
                    status="timeout",
                    error="Request timeout",
                    response_time=time.time() - start_time,
                )
            except Exception as e:
                return EmailResult(
                    url=original_url,
                    emails=[],
                    status="error",
                    error=str(e),
                    response_time=time.time() - start_time,
                )
            finally:
                # Add delay between requests to be respectful
                if self.delay_between_requests > 0:
                    await asyncio.sleep(self.delay_between_requests)

    async def scrape_urls(
        self, urls: List[str], progress_callback=None
    ) -> List[EmailResult]:
        """Scrape emails from multiple URLs in parallel."""
        tasks = [self.scrape_single_url(url) for url in urls]
        results = []

        for i, task in enumerate(asyncio.as_completed(tasks)):
            result = await task
            results.append(result)
            if progress_callback:
                progress_callback(i + 1, len(urls), result)

        return results


# Streamlit App
def main():
    st.set_page_config(
        page_title="Smart Email Scraper",
        page_icon="📧",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("🔍 Smart Parallel Email Scraper")
    st.markdown("---")

    # Initialize session state
    if "results" not in st.session_state:
        st.session_state.results = []
    if "scraping_complete" not in st.session_state:
        st.session_state.scraping_complete = False

    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")

        max_concurrent = st.slider("Max Concurrent Requests", 1, 20, 5)
        timeout = st.slider("Request Timeout (seconds)", 10, 120, 30)
        delay = st.slider("Delay Between Requests (seconds)", 0.1, 5.0, 1.0)

        st.markdown("---")
        st.header("📊 Quick Stats")

        if st.session_state.results:
            total_urls = len(st.session_state.results)
            successful = sum(
                1 for r in st.session_state.results if r.status == "success"
            )
            total_emails = sum(len(r.emails) for r in st.session_state.results)
            unique_emails = len(
                set(email for r in st.session_state.results for email in r.emails)
            )

            st.metric("Total URLs", total_urls)
            st.metric("Successful", successful)
            st.metric("Total Emails", total_emails)
            st.metric("Unique Emails", unique_emails)

    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(
        ["🚀 Scraper", "📋 Results", "❌ Failed URLs", "📊 Analytics"]
    )

    with tab1:
        st.header("URL Input")

        # URL enhancement options
        st.subheader("🎯 Smart URL Enhancement")
        col1, col2 = st.columns(2)
        with col1:
            auto_enhance = st.checkbox(
                "Auto-enhance URLs",
                value=True,
                help="Automatically try common pages like /contact, /about when only domain is provided",
            )
        with col2:
            try_both_protocols = st.checkbox(
                "Try both HTTP/HTTPS",
                value=False,
                help="Try both HTTP and HTTPS for each URL",
            )

        # URL input methods
        input_method = st.radio("Choose input method:", ["Paste URLs", "Upload File"])

        urls = []
        if input_method == "Paste URLs":
            st.info(
                "💡 **Supported URL formats:**\n"
                "• Full URLs: `https://example.com/contact`\n"
                "• Domain only: `example.com` (will auto-add https://)\n"
                "• Subdomains: `blog.example.com`\n"
                "• With paths: `example.com/about-us`"
            )

            url_text = st.text_area(
                "Enter URLs (one per line):",
                height=200,
                placeholder="example.com\nblog.example.com\nhttps://company.com/contact\nwww.business.org/team",
            )
            if url_text:
                urls = [url.strip() for url in url_text.split("\n") if url.strip()]

        else:
            uploaded_file = st.file_uploader(
                "Upload a text file with URLs", type=["txt"]
            )
            if uploaded_file is not None:
                content = StringIO(uploaded_file.getvalue().decode("utf-8"))
                urls = [line.strip() for line in content if line.strip()]

        if urls:
            # Process URLs based on enhancement options
            processed_urls = []
            scraper_temp = SmartEmailScraper()

            for url in urls:
                if auto_enhance:
                    # Generate URL variants for domain-only URLs
                    normalized = scraper_temp.normalize_url(url)
                    variants = scraper_temp.generate_url_variants(normalized)
                    processed_urls.extend(variants)
                else:
                    processed_urls.append(scraper_temp.normalize_url(url))

            # Add HTTP variants if requested
            if try_both_protocols:
                additional_urls = []
                for url in processed_urls.copy():
                    if url.startswith("https://"):
                        http_version = url.replace("https://", "http://", 1)
                        additional_urls.append(http_version)
                processed_urls.extend(additional_urls)

            # Remove duplicates while preserving order
            final_urls = []
            seen = set()
            for url in processed_urls:
                if url not in seen:
                    seen.add(url)
                    final_urls.append(url)

            st.success(
                f"Found {len(urls)} original URLs → Enhanced to {len(final_urls)} URLs to scrape"
            )

            # Show enhancement details
            if auto_enhance or try_both_protocols:
                with st.expander("🔍 URL Enhancement Details"):
                    st.write("**Original URLs:**")
                    for url in urls[:5]:
                        st.write(f"• {url}")
                    if len(urls) > 5:
                        st.write(f"... and {len(urls) - 5} more")

                    st.write("**Enhanced URLs (sample):**")
                    for url in final_urls[:10]:
                        st.write(f"• {url}")
                    if len(final_urls) > 10:
                        st.write(f"... and {len(final_urls) - 10} more")

            # Preview URLs
            with st.expander("Preview All URLs to be Scraped"):
                for i, url in enumerate(final_urls, 1):
                    st.write(f"{i}. {url}")

            # Update urls for scraping
            urls = final_urls

        # Scraping button
        if st.button("🚀 Start Scraping", disabled=len(urls) == 0):
            st.session_state.results = []
            st.session_state.scraping_complete = False

            # Progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()
            results_container = st.empty()

            async def run_scraping():
                scraper_config = {
                    "max_concurrent": max_concurrent,
                    "timeout": timeout,
                    "delay_between_requests": delay,
                }

                def progress_callback(completed, total, result):
                    progress = completed / total
                    progress_bar.progress(progress)
                    status_text.text(f"Progress: {completed}/{total} URLs processed")

                    # Update results in real-time
                    st.session_state.results.append(result)

                    # Show real-time results
                    if st.session_state.results:
                        df = create_results_dataframe(st.session_state.results)
                        results_container.dataframe(df, use_container_width=True)

                async with SmartEmailScraper(**scraper_config) as scraper:
                    results = await scraper.scrape_urls(urls, progress_callback)
                    st.session_state.results = results
                    st.session_state.scraping_complete = True

            # Run the scraping
            asyncio.run(run_scraping())

            if st.session_state.scraping_complete:
                st.success("Scraping completed!")
                st.balloons()

    with tab2:
        st.header("📋 Scraping Results")

        if st.session_state.results:
            # Create and display results dataframe
            df = create_results_dataframe(st.session_state.results)

            # Filter options
            col1, col2, col3 = st.columns(3)
            with col1:
                status_filter = st.selectbox(
                    "Filter by Status", ["All", "Success", "Error", "Timeout"]
                )
            with col2:
                min_emails = st.number_input("Min Emails Found", min_value=0, value=0)
            with col3:
                search_term = st.text_input("Search in URLs")

            # Apply filters
            filtered_df = df.copy()
            if status_filter != "All":
                filtered_df = filtered_df[
                    filtered_df["Status"] == status_filter.lower()
                ]
            if min_emails > 0:
                filtered_df = filtered_df[filtered_df["Emails Found"] >= min_emails]
            if search_term:
                filtered_df = filtered_df[
                    filtered_df["URL"].str.contains(search_term, case=False, na=False)
                ]

            st.dataframe(filtered_df, use_container_width=True)

            # Download options
            col1, col2 = st.columns(2)
            with col1:
                csv_data = convert_df_to_csv(filtered_df)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name="email_scraping_results.csv",
                    mime="text/csv",
                )

            with col2:
                json_data = convert_results_to_json(st.session_state.results)
                st.download_button(
                    label="📥 Download JSON",
                    data=json_data,
                    file_name="email_scraping_results.json",
                    mime="application/json",
                )

            # Show all unique emails
            if st.expander("📧 All Unique Emails Found"):
                all_emails = set()
                for result in st.session_state.results:
                    all_emails.update(result.emails)

                if all_emails:
                    emails_df = pd.DataFrame(sorted(all_emails), columns=["Email"])
                    st.dataframe(emails_df, use_container_width=True)

                    # Copy all emails button
                    emails_text = "\n".join(sorted(all_emails))
                    st.text_area("Copy all emails:", emails_text, height=200)
                else:
                    st.info("No emails found in any of the scraped URLs.")
        else:
            st.info("No results yet. Please run the scraper first.")

    with tab3:
        st.header("❌ Failed URLs")

        if st.session_state.results:
            failed_results = [
                r for r in st.session_state.results if r.status != "success"
            ]
            no_email_results = [
                r
                for r in st.session_state.results
                if r.status == "success" and len(r.emails) == 0
            ]

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("🚫 Failed to Scrape")
                if failed_results:
                    failed_df = pd.DataFrame(
                        [
                            {
                                "URL": r.url,
                                "Status": r.status,
                                "Error": r.error,
                                "Response Time": (
                                    f"{r.response_time:.2f}s"
                                    if r.response_time
                                    else "N/A"
                                ),
                            }
                            for r in failed_results
                        ]
                    )
                    st.dataframe(failed_df, use_container_width=True)

                    # Export failed URLs
                    failed_urls = "\n".join([r.url for r in failed_results])
                    st.text_area(
                        "Failed URLs (copy to retry):", failed_urls, height=150
                    )
                else:
                    st.success("No failed URLs! 🎉")

            with col2:
                st.subheader("📭 No Emails Found")
                if no_email_results:
                    no_email_df = pd.DataFrame(
                        [
                            {
                                "URL": r.url,
                                "Status": r.status,
                                "Response Time": (
                                    f"{r.response_time:.2f}s"
                                    if r.response_time
                                    else "N/A"
                                ),
                            }
                            for r in no_email_results
                        ]
                    )
                    st.dataframe(no_email_df, use_container_width=True)

                    # Export no-email URLs
                    no_email_urls = "\n".join([r.url for r in no_email_results])
                    st.text_area(
                        "URLs with no emails (copy to review):",
                        no_email_urls,
                        height=150,
                    )
                else:
                    st.success("All successful URLs had emails! 🎉")
        else:
            st.info("No results yet. Please run the scraper first.")

    with tab4:
        st.header("📊 Analytics Dashboard")

        if st.session_state.results:
            create_analytics_dashboard(st.session_state.results)
        else:
            st.info("No data available for analytics. Please run the scraper first.")


def create_results_dataframe(results: List[EmailResult]) -> pd.DataFrame:
    """Create a pandas DataFrame from results."""
    data = []
    for result in results:
        data.append(
            {
                "URL": result.url,
                "Status": result.status,
                "Emails Found": len(result.emails),
                "Emails": "; ".join(result.emails) if result.emails else "",
                "Response Time (s)": (
                    f"{result.response_time:.2f}" if result.response_time else "N/A"
                ),
                "Error": result.error or "",
            }
        )
    return pd.DataFrame(data)


def convert_df_to_csv(df: pd.DataFrame) -> str:
    """Convert DataFrame to CSV string."""
    return df.to_csv(index=False)


def convert_results_to_json(results: List[EmailResult]) -> str:
    """Convert results to JSON string."""
    return json.dumps(
        [asdict(result) for result in results], indent=2, ensure_ascii=False
    )


def create_analytics_dashboard(results: List[EmailResult]):
    """Create analytics dashboard with charts."""

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    total_urls = len(results)
    successful = sum(1 for r in results if r.status == "success")
    failed = total_urls - successful
    total_emails = sum(len(r.emails) for r in results)

    with col1:
        st.metric("Total URLs", total_urls)
    with col2:
        st.metric("Success Rate", f"{(successful/total_urls)*100:.1f}%")
    with col3:
        st.metric("Total Emails", total_emails)
    with col4:
        avg_response_time = sum(
            r.response_time for r in results if r.response_time
        ) / len([r for r in results if r.response_time])
        st.metric("Avg Response Time", f"{avg_response_time:.2f}s")

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        # Status distribution pie chart
        status_counts = {}
        for result in results:
            status_counts[result.status] = status_counts.get(result.status, 0) + 1

        fig_pie = px.pie(
            values=list(status_counts.values()),
            names=list(status_counts.keys()),
            title="Request Status Distribution",
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # Emails found distribution
        email_counts = [len(r.emails) for r in results if r.status == "success"]
        if email_counts:
            fig_hist = px.histogram(
                x=email_counts,
                title="Distribution of Emails Found",
                labels={"x": "Number of Emails", "y": "Frequency"},
            )
            st.plotly_chart(fig_hist, use_container_width=True)

    # Response time analysis
    response_times = [(r.url, r.response_time) for r in results if r.response_time]
    if response_times:
        urls, times = zip(*response_times)
        fig_response = px.bar(
            x=list(range(len(urls))),
            y=times,
            title="Response Times by URL",
            labels={"x": "URL Index", "y": "Response Time (seconds)"},
        )
        fig_response.update_layout(showlegend=False)
        st.plotly_chart(fig_response, use_container_width=True)

    # Top domains with most emails
    domain_emails = {}
    for result in results:
        if result.status == "success" and result.emails:
            domain = urlparse(result.url).netloc
            domain_emails[domain] = domain_emails.get(domain, 0) + len(result.emails)

    if domain_emails:
        top_domains = sorted(domain_emails.items(), key=lambda x: x[1], reverse=True)[
            :10
        ]
        domains, email_counts = zip(*top_domains)

        fig_domains = px.bar(
            x=email_counts,
            y=domains,
            orientation="h",
            title="Top 10 Domains by Email Count",
            labels={"x": "Number of Emails", "y": "Domain"},
        )
        st.plotly_chart(fig_domains, use_container_width=True)


if __name__ == "__main__":
    main()

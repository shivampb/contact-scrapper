import streamlit as st
import pandas as pd
from backend import EmailScraper

st.set_page_config(page_title="Email Scraper", page_icon="📧", layout="wide")

st.title("📧 Website Email Scraper")
st.markdown(
    """
This tool helps you scrape emails from websites and their contact pages.
Enter one website URL per line.
"""
)

# Input area for URLs
urls_input = st.text_area(
    "Enter website URLs (one per line)",
    height=200,
    placeholder="example.com\nanother-example.com",
)

if st.button("Start Scraping", type="primary"):
    if urls_input:
        urls = [url.strip() for url in urls_input.split("\n") if url.strip()]

        with st.spinner("Scraping emails... This might take a few minutes."):
            scraper = EmailScraper()
            results = scraper.scrape_multiple_sites(urls)

            if results:
                # Convert results to a more suitable format for DataFrame
                df_data = []
                for result in results:
                    for email_info in result["email_data"]:
                        df_data.append(
                            {
                                "Main Website": result["url"],
                                "Email": email_info["email"],
                            }
                        )

                df = pd.DataFrame(df_data)

                # Display results
                total_websites = len(results)
                total_emails = len(df)
                st.success(
                    f"Found {total_emails} unique email(s) from {total_websites} website(s)"
                )

                # Add download button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download results as CSV",
                    data=csv,
                    file_name="scraped_emails.csv",
                    mime="text/csv",
                )

                # Display the table
                st.dataframe(
                    df,
                    use_container_width=True,
                    column_config={
                        "Main Website": st.column_config.TextColumn("Main Website"),
                        "Email": st.column_config.TextColumn("Email"),
                    },
                )
            else:
                st.warning("No emails found from the provided websites.")
    else:
        st.error("Please enter at least one URL to scrape.")

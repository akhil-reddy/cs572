import re

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import time
import random
import ssl


class Crawler:
    def __init__(self, seed_url, max_pages=20000, max_depth=16):
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.visited_urls = set()
        self.urls_to_visit = [(seed_url, 0)]
        self.domain = urlparse(seed_url).netloc

        # CSV file handlers
        self.fetch_csv = csv.writer(open('fetch_latimes.csv', 'w', newline=''))
        self.visit_csv = csv.writer(open('visit_latimes.csv', 'w', newline=''))
        self.urls_csv = csv.writer(open('urls_latimes.csv', 'w', newline=''))

        # Write headers
        self.fetch_csv.writerow(['URL', 'Status'])
        self.visit_csv.writerow(['URL', 'Size', 'OutLinks', 'ContentType'])
        self.urls_csv.writerow(['URL', 'Valid'])

        # User agent
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; USCCrawler/1.0; +http://www.usc.edu/)'
        }

        # Statistics
        self.pages_crawled = 0
        self.successful_crawls = 0
        self.failed_crawls = 0

    def is_valid(self, url):
        parsed = urlparse(url)

        # Check if the URL is within the domain
        if not (bool(parsed.netloc) and parsed.netloc.endswith(self.domain)):
            return False

        # Filter out telephone number URLs
        if re.search(r'/tel:', parsed.path):
            return False

        # Filter out specified file types
        excluded_extensions = (
            'css', 'js', 'mid', 'mp2', 'mp3', 'mp4', 'wav', 'avi', 'mov', 'mpeg', 'ram',
            'm4v', 'rm', 'smil', 'wmv', 'swf', 'wma', 'zip', 'rar', 'gz', 'json', 'ttf',
            'svg', 'ico', 'jpg', 'jpeg', 'png', 'gif', 'pdf', 'xml'
        )
        if parsed.path.lower().split('.')[-1] in excluded_extensions:
            return False

        return True

        return True

    def crawl(self):
        print(f"Starting crawl from {self.seed_url}")
        print(f"Max pages to crawl: {self.max_pages}")
        print(f"Max depth: {self.max_depth}")
        print("Crawling in progress...")

        while self.urls_to_visit and self.pages_crawled < self.max_pages:
            url, depth = self.urls_to_visit.pop(0)

            if depth > self.max_depth:
                continue

            if url not in self.visited_urls:
                self.visited_urls.add(url)
                self.fetch_url(url, depth)

            # Print progress every 10 pages
            if self.pages_crawled % 10 == 0:
                print(f"Pages crawled: {self.pages_crawled}")
                print(f"Successful crawls: {self.successful_crawls}")
                print(f"Failed crawls: {self.failed_crawls}")
                print(f"URLs left to visit: {len(self.urls_to_visit)}")
                print("---")

            # Politeness delay
            #time.sleep(random.uniform(1, 3))

    def fetch_url(self, url, depth):
        self.pages_crawled += 1
        print(f"Crawling: {url} (Depth: {depth})")

        try:
            response = requests.get(url, headers=self.headers, timeout=10, verify=False)
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

            self.fetch_csv.writerow([url, response.status_code])

            if response.status_code == 200:
                self.successful_crawls += 1
                content_type = response.headers.get('Content-Type', '').split(';')[0]

                if 'text/html' in content_type:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    links = soup.find_all('a')
                    out_links = set()

                    for link in links:
                        href = link.get('href')
                        if href:
                            full_url = urljoin(url, href)
                            out_links.add(full_url)
                            if self.is_valid(full_url):
                                self.urls_to_visit.append((full_url, depth + 1))
                                self.urls_csv.writerow([full_url, 'OK'])
                            else:
                                self.urls_csv.writerow([full_url, 'N_OK'])

                    self.visit_csv.writerow([url, len(response.content), len(out_links), content_type])
                    print(f"Found {len(out_links)} outgoing links")
                elif any(t in content_type for t in ['application/pdf', 'image/jpeg', 'image/png', 'image/gif']):
                    self.visit_csv.writerow([url, len(response.content), 0, content_type])
                    print(f"Downloaded {content_type} file")

            # Handle other status codes
            elif response.status_code in [301, 302]:
                new_url = response.headers.get('Location')
                if new_url and self.is_valid(new_url):
                    self.urls_to_visit.append((new_url, depth))
                print(f"Redirect to: {new_url}")

            # Even for 403, 404, etc., we record the attempt
            else:
                self.failed_crawls += 1
                print(f"Failed to crawl: Status code {response.status_code}")

        except Exception as e:
            self.failed_crawls += 1
            print(f"Error crawling {url}: {str(e)}")
            self.fetch_csv.writerow([url, 'FAILED'])


if __name__ == "__main__":
    # Disable SSL verification warnings
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    # For macOS: This is a workaround for the SSL certificate issue
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context

    crawler = Crawler("https://www.latimes.com/", max_pages=20000, max_depth=16)
    crawler.crawl()

    print("\nCrawling completed.")
    print(f"Total pages crawled: {crawler.pages_crawled}")
    print(f"Successful crawls: {crawler.successful_crawls}")
    print(f"Failed crawls: {crawler.failed_crawls}")
    print("Check the CSV files for detailed results.")
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import time
import random
import ssl
import re
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor


class Crawler:
    def __init__(self, seed_url, max_pages=20000, max_depth=16, num_threads=4):
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.num_threads = num_threads
        self.visited_urls = set()
        self.url_queue = Queue()
        self.url_queue.put((seed_url, 0))
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

        # Locks for thread-safe operations
        self.csv_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self.print_lock = threading.Lock()

    def is_valid(self, url):
        parsed = urlparse(url)

        if not (bool(parsed.netloc) and parsed.netloc.endswith(self.domain)):
            return False

        if re.search(r'/tel:', parsed.path):
            return False

        excluded_extensions = (
            'css', 'js', 'mid', 'mp2', 'mp3', 'mp4', 'wav', 'avi', 'mov', 'mpeg', 'ram',
            'm4v', 'rm', 'smil', 'wmv', 'swf', 'wma', 'zip', 'rar', 'gz', 'json', 'ttf',
            'svg', 'ico', 'xml'
        )
        if parsed.path.lower().split('.')[-1] in excluded_extensions:
            return False

        return True

    def crawl(self):
        print(f"Starting crawl from {self.seed_url}")
        print(f"Max pages to crawl: {self.max_pages}")
        print(f"Max depth: {self.max_depth}")
        print(f"Number of threads: {self.num_threads}")
        print("Crawling in progress...")

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            while self.pages_crawled < self.max_pages:
                futures = []
                for _ in range(self.num_threads):
                    if not self.url_queue.empty():
                        futures.append(executor.submit(self.fetch_url))
                    else:
                        break
                for future in futures:
                    future.result()

                if self.url_queue.empty() and len(futures) == 0:
                    break

                # Print progress every 10 pages
                if self.pages_crawled % 10 == 0:
                    with self.print_lock:
                        print(f"Pages crawled: {self.pages_crawled}")
                        print(f"Successful crawls: {self.successful_crawls}")
                        print(f"Failed crawls: {self.failed_crawls}")
                        print(f"URLs left to visit: {self.url_queue.qsize()}")
                        print("---")

    def fetch_url(self):
        url, depth = self.url_queue.get()

        if depth > self.max_depth or url in self.visited_urls:
            return

        self.visited_urls.add(url)

        with self.stats_lock:
            self.pages_crawled += 1
        with self.print_lock:
            print(f"Crawling: {url} (Depth: {depth})")

        try:
            response = requests.get(url, headers=self.headers, timeout=10, verify=False)
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

            with self.csv_lock:
                self.fetch_csv.writerow([url, response.status_code])

            if response.status_code == 200:
                with self.stats_lock:
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
                                self.url_queue.put((full_url, depth + 1))
                                with self.csv_lock:
                                    self.urls_csv.writerow([full_url, 'OK'])
                            else:
                                with self.csv_lock:
                                    self.urls_csv.writerow([full_url, 'N_OK'])

                    with self.csv_lock:
                        self.visit_csv.writerow([url, len(response.content), len(out_links), content_type])
                    with self.print_lock:
                        print(f"Found {len(out_links)} outgoing links")
                elif any(t in content_type for t in ['application/pdf', 'image/jpeg', 'image/png', 'image/gif']):
                    with self.csv_lock:
                        self.visit_csv.writerow([url, len(response.content), 0, content_type])
                    with self.print_lock:
                        print(f"Downloaded {content_type} file")

            elif response.status_code in [301, 302]:
                new_url = response.headers.get('Location')
                if new_url and self.is_valid(new_url):
                    self.url_queue.put((new_url, depth))
                with self.print_lock:
                    print(f"Redirect to: {new_url}")

            else:
                with self.stats_lock:
                    self.failed_crawls += 1
                with self.print_lock:
                    print(f"Failed to crawl: Status code {response.status_code}")

        except Exception as e:
            with self.stats_lock:
                self.failed_crawls += 1
            with self.print_lock:
                print(f"Error crawling {url}: {str(e)}")
            with self.csv_lock:
                self.fetch_csv.writerow([url, 'FAILED'])

        # Politeness delay
        #time.sleep(random.uniform(1, 3))


if __name__ == "__main__":
    # Disable SSL verification warnings
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    # For macOS: This is a workaround for the SSL certificate issue
    import ssl

    ssl._create_default_https_context = ssl._create_unverified_context

    crawler = Crawler("https://www.latimes.com/", max_pages=20000, max_depth=16, num_threads=4)
    crawler.crawl()

    print("\nCrawling completed.")
    print(f"Total pages crawled: {crawler.pages_crawled}")
    print(f"Successful crawls: {crawler.successful_crawls}")
    print(f"Failed crawls: {crawler.failed_crawls}")
    print(f"Number of threads used: {crawler.num_threads}")
    print("Check the CSV files for detailed results.")
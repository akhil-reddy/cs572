import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv


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

    def is_valid(self, url):
        parsed = urlparse(url)
        return bool(parsed.netloc) and parsed.netloc.endswith(self.domain)

    def crawl(self):
        while self.urls_to_visit and len(self.visited_urls) < self.max_pages:
            url, depth = self.urls_to_visit.pop(0)

            if depth > self.max_depth:
                continue

            if url not in self.visited_urls:
                self.visited_urls.add(url)
                self.fetch_url(url, depth)

    def fetch_url(self, url, depth):
        try:
            response = requests.get(url, timeout=10)
            self.fetch_csv.writerow([url, response.status_code])

            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '').split(';')[0]

                if content_type == 'text/html':
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
                elif content_type in ['application/pdf', 'image/jpeg', 'image/png', 'image/gif']:
                    self.visit_csv.writerow([url, len(response.content), 0, content_type])

        except Exception as e:
            print(f"Error crawling {url}: {str(e)}")
            self.fetch_csv.writerow([url, 'FAILED'])


if __name__ == "__main__":
    crawler = Crawler("https://www.latimes.com/", max_pages=20000, max_depth=16)
    crawler.crawl()
    print("Crawling completed. Check the CSV files for results.")
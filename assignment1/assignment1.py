import time
import random
import logging
from urllib.parse import urlparse, unquote
import requests
from bs4 import BeautifulSoup
import json
import csv

logging.basicConfig(level=logging.DEBUG)

def normalize_url(url):
    if not url:
        return ""
    parsed = urlparse(url)
    netloc = parsed.netloc
    if netloc.startswith('www.'):
        netloc = netloc[4:]
    path = parsed.path.rstrip('/')
    normalized = f"{netloc}{path}"
    if parsed.query:
        normalized += f"?{parsed.query}"
    return normalized

class SearchEngine:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def search(self, query):
        delay = random.uniform(10, 20)
        logging.info(f"Sleeping for {delay:.2f} seconds before searching for '{query}'")
        time.sleep(delay)

        url = self.base_url + '+'.join(query.split())
        logging.info(f"Sending request to {url}")

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Error fetching search results: {e}")
            return []

        logging.debug(f"Response status code: {response.status_code}")
        logging.debug(f"Response headers: {response.headers}")

        content = response.text
        logging.debug(f"Decoded content (first 1000 characters): {content[:1000]}")

        soup = BeautifulSoup(content, "html.parser")

        results = []
        selectors = [
            "div.result__body a.result__a",
            "div.results a.result__a",
            "div.links_main a.result__a",
            "div.results_links a.large",
            "article.result h2 a",
            "div.result__title a"
        ]

        for selector in selectors:
            raw_results = soup.select(selector)
            logging.info(f"Found {len(raw_results)} raw results with selector '{selector}'")

            for result in raw_results:
                link = result.get('href')
                if link:
                    if link.startswith('/'):
                        link = 'https://duckduckgo.com' + link
                    parsed = urlparse(link)
                    if parsed.netloc == 'duckduckgo.com' and parsed.path == '/l/':
                        actual_url = parsed.query.split('uddg=')[-1].split('&')[0]
                        link = unquote(actual_url)

                normalized_url = normalize_url(link)
                logging.debug(f"Processed link: {normalized_url}")
                if normalized_url and normalized_url not in results:
                    results.append(normalized_url)
                    if len(results) == 10:
                        return results

        if not results:
            logging.warning(f"No results found with any selector. Dumping full HTML to 'debug_output.html'")
            with open('debug_output.html', 'w', encoding='utf-8') as f:
                f.write(content)

        logging.info(f"Processed {len(results)} unique results")
        return results

def load_queries(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f]

def load_google_results(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)


def calculate_overlap_and_spearman(engine_results, google_results):
    engine_normalized = [normalize_url(url) for url in engine_results]
    google_normalized = [normalize_url(url) for url in google_results]

    overlap = set(engine_normalized) & set(google_normalized)
    overlap_percent = len(overlap) / len(google_normalized) * 100

    if len(overlap) == 0:
        return overlap_percent, 0
    elif len(overlap) == 1:
        overlapping_url = list(overlap)[0]
        engine_rank = engine_normalized.index(overlapping_url)
        google_rank = google_normalized.index(overlapping_url)
        return overlap_percent, 1 if engine_rank == google_rank else -1

    ranks_engine = {url: engine_normalized.index(url) for url in overlap}
    ranks_google = {url: google_normalized.index(url) for url in overlap}

    n = len(overlap)
    rank_diffs = [(ranks_engine[url] - ranks_google[url]) ** 2 for url in overlap]
    sum_d_squared = sum(rank_diffs)

    rho = 1 - (6 * sum_d_squared) / (n * (n ** 2 - 1))

    return overlap_percent, rho

def main():
    engine = SearchEngine("https://html.duckduckgo.com/html/?q=")

    queries = load_queries("100QueriesSet4.txt")  # Update with your assigned query set
    google_results = load_google_results("Google_Result4.json")  # Update with your assigned Google results file

    results = {}
    stats = []

    for query in queries:
        logging.info(f"Processing query: {query}")
        engine_results = engine.search(query)
        results[query] = engine_results

        overlap_percent, rho = calculate_overlap_and_spearman(engine_results, google_results[query])
        stats.append((query, overlap_percent, rho))

    # Save results to JSON file
    with open("hw1.json", "w") as f:
        json.dump(results, f, indent=2)

    # Save statistics to CSV file
    with open("hw1.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Query", "Percent Overlap", "Spearman Coefficient"])
        for stat in stats:
            writer.writerow(stat)

        # Calculate and write averages
        avg_overlap = sum(s[1] for s in stats) / len(stats)
        avg_rho = sum(s[2] for s in stats) / len(stats)
        writer.writerow(["Average", avg_overlap, avg_rho])

    logging.info("Results saved to hw1.json and hw1.csv")

if __name__ == "__main__":
    main()
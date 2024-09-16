import csv
import json
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from urllib.parse import urlparse, unquote

logging.basicConfig(level=logging.DEBUG)


class SearchEngine:
    def __init__(self, base_url, selectors):
        self.base_url = base_url
        self.selectors = selectors
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
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
        for selector in self.selectors:
            raw_results = soup.select(selector)
            logging.info(f"Found {len(raw_results)} raw results with selector '{selector}'")

            if raw_results:
                for result in raw_results:
                    link = result.get('href')
                    if link:
                        # DuckDuckGo uses a redirect URL, so we need to extract the actual URL
                        parsed = urlparse(link)
                        if parsed.netloc == 'duckduckgo.com' and parsed.path == '/l/':
                            actual_url = parsed.query.split('uddg=')[-1].split('&')[0]
                            link = unquote(actual_url)

                    logging.debug(f"Processed link: {link}")
                    if link and link.startswith('http'):
                        parsed_url = urlparse(link)
                        normalized_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path.rstrip('/')}"
                        if normalized_url not in results:
                            results.append(normalized_url)
                            if len(results) == 10:
                                break

                if results:
                    break  # Stop if we found results with this selector

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
    overlap = set(engine_results) & set(google_results)
    overlap_percent = len(overlap) / len(google_results) * 100

    if len(overlap) == 0:
        return overlap_percent, 0
    elif len(overlap) == 1:
        return overlap_percent, 1 if engine_results.index(list(overlap)[0]) == google_results.index(
            list(overlap)[0]) else 0

    ranks = []
    for url in overlap:
        ranks.append((engine_results.index(url), google_results.index(url)))

    n = len(ranks)
    sum_d_squared = sum((r[0] - r[1]) ** 2 for r in ranks)
    rho = 1 - (6 * sum_d_squared) / (n * (n ** 2 - 1))

    return overlap_percent, rho


def main():
    engine = SearchEngine(
        "https://html.duckduckgo.com/html/?q=",
        [
            "div.result__body a.result__a",
            "div.results a.result__a",
            "div.links_main a.result__a",
            "div.results_links a.large",
        ]
    )

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

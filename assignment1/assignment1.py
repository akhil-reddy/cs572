import requests
from bs4 import BeautifulSoup
import json
import time
import random
import csv
from urllib.parse import urlparse

USER_AGENT = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15'
}

class SearchEngine:
    def __init__(self, base_url, selector):
        self.base_url = base_url
        self.selector = selector

    def search(self, query):
        time.sleep(random.uniform(10, 100))
        url = self.base_url + '+'.join(query.split())
        response = requests.get(url, headers=USER_AGENT)
        soup = BeautifulSoup(response.text, "html.parser")
        raw_results = soup.select(self.selector)
        results = []
        for result in raw_results:
            link = result.get('href')
            if link and link.startswith('http'):
                parsed_url = urlparse(link)
                normalized_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path.rstrip('/')}"
                if normalized_url not in results:
                    results.append(normalized_url)
                    if len(results) == 10:
                        break
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
    # Initialize the search engine (change these values based on your assigned search engine)
    engine = SearchEngine("https://www.bing.com/search?q=", "li.b_algo a[h2]")

    queries = load_queries("100QueriesSet4.txt")
    google_results = load_google_results("Google_Result4.json")

    results = {}
    stats = []

    for query in queries:
        print(f"Processing query: {query}")
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

    print("Results saved to hw1.json and hw1.csv")


if __name__ == "__main__":
    main()

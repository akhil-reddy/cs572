import csv
from collections import defaultdict
from urllib.parse import urlparse

def read_csv(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return list(csv.reader(f))

def get_domain(url):
    return urlparse(url).netloc

def process_fetch_csv(fetch_data):
    attempted = len(fetch_data) - 1  # Subtract 1 to account for header
    succeeded = sum(1 for row in fetch_data[1:] if row[1].startswith('2'))
    failed_or_aborted = attempted - succeeded
    status_codes = defaultdict(int)
    for row in fetch_data[1:]:  # Skip header row
        status_codes[row[1]] += 1
    return attempted, succeeded, failed_or_aborted, status_codes

def process_visit_csv(visit_data):
    file_sizes = defaultdict(int)
    content_types = defaultdict(int)
    total_urls = 0
    for row in visit_data[1:]:  # Skip header row
        size = int(row[1])
        total_urls += int(row[2])
        content_types[row[3]] += 1
        if size < 1024:
            file_sizes['< 1KB'] += 1
        elif size < 10240:
            file_sizes['1KB ~ <10KB'] += 1
        elif size < 102400:
            file_sizes['10KB ~ <100KB'] += 1
        elif size < 1048576:
            file_sizes['100KB ~ <1MB'] += 1
        else:
            file_sizes['>= 1MB'] += 1
    return file_sizes, content_types, total_urls

def process_urls_csv(urls_data, news_site):
    unique_urls = set()
    unique_within = set()
    unique_outside = set()
    for row in urls_data[1:]:  # Skip header row
        url = row[0]
        unique_urls.add(url)
        if news_site in get_domain(url):
            unique_within.add(url)
        else:
            unique_outside.add(url)
    return len(unique_urls), len(unique_within), len(unique_outside)

def generate_report(news_site, num_threads, fetch_stats, visit_stats, urls_stats):
    report = f"""Name: Akhil Krishna Reddy
USC ID: 7423463185
News site crawled: {news_site}
Number of threads: {num_threads}

Fetch Statistics
================
# fetches attempted: {fetch_stats[0]}
# fetches succeeded: {fetch_stats[1]}
# fetches failed or aborted: {fetch_stats[2]}

Outgoing URLs:
==============
Total URLs extracted: {visit_stats[2]}
# unique URLs extracted: {urls_stats[0]}
# unique URLs within News Site: {urls_stats[1]}
# unique URLs outside News Site: {urls_stats[2]}

Status Codes:
=============
"""
    for code, count in sorted(fetch_stats[3].items()):
        report += f"{code}: {count}\n"

    report += f"""
File Sizes:
===========
"""
    for size, count in visit_stats[0].items():
        report += f"{size}: {count}\n"

    report += f"""
Content Types:
==============
"""
    for content_type, count in visit_stats[1].items():
        report += f"{content_type}: {count}\n"

    return report

def main():
    news_site = input("Enter the news site name (e.g., nytimes): ")
    num_threads = int(input("Enter the number of threads used: "))

    fetch_data = read_csv(f'fetch_{news_site}.csv')
    visit_data = read_csv(f'visit_{news_site}.csv')
    urls_data = read_csv(f'urls_{news_site}.csv')

    fetch_stats = process_fetch_csv(fetch_data)
    visit_stats = process_visit_csv(visit_data)
    urls_stats = process_urls_csv(urls_data, news_site)

    report = generate_report(news_site, num_threads, fetch_stats, visit_stats, urls_stats)

    with open(f'CrawlReport_{news_site}.txt', 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"Report generated: CrawlReport_{news_site}.txt")

if __name__ == "__main__":
    main()
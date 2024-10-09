#!/usr/bin/env python3
import sys
import re
from collections import defaultdict

SELECTED_BIGRAMS = {
    "computer science", "information retrieval", "power politics",
    "los angeles", "bruce willis"
}


def mapper():
    for line in sys.stdin:
        try:
            doc_id, content = line.strip().split('\t', 1)
        except ValueError:
            continue

        # Clean the content
        content = re.sub(r'[^a-zA-Z\s]', ' ', content.lower())
        words = content.split()

        # Emit bigram, doc_id pairs for selected bigrams
        for i in range(len(words) - 1):
            bigram = f"{words[i]} {words[i + 1]}"
            if bigram in SELECTED_BIGRAMS:
                print(f"{bigram}\t{doc_id}:1")


def reducer():
    current_bigram = None
    bigram_counts = defaultdict(int)

    for line in sys.stdin:
        bigram, doc_id_count = line.strip().split('\t')
        doc_id, count = doc_id_count.split(':')
        count = int(count)

        if current_bigram == bigram:
            bigram_counts[doc_id] += count
        else:
            if current_bigram:
                output = ' '.join(f"{doc_id}:{count}" for doc_id, count in bigram_counts.items())
                print(f"{current_bigram}\t{output}")
            current_bigram = bigram
            bigram_counts = defaultdict(int)
            bigram_counts[doc_id] = count

    if current_bigram:
        output = ' '.join(f"{doc_id}:{count}" for doc_id, count in bigram_counts.items())
        print(f"{current_bigram}\t{output}")


if __name__ == "__main__":
    if sys.argv[1] == "mapper":
        mapper()
    elif sys.argv[1] == "reducer":
        reducer()
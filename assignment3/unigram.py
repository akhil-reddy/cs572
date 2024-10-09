#!/usr/bin/env python3
import sys
import re
from collections import defaultdict


def mapper():
    for line in sys.stdin:
        try:
            doc_id, content = line.strip().split('\t', 1)
        except ValueError:
            continue

        # Clean the content
        content = re.sub(r'[^a-zA-Z\s]', ' ', content.lower())

        # Emit word, doc_id pairs
        for word in content.split():
            print(f"{word}\t{doc_id}:1")


def reducer():
    current_word = None
    word_counts = defaultdict(int)

    for line in sys.stdin:
        word, doc_id_count = line.strip().split('\t')
        doc_id, count = doc_id_count.split(':')
        count = int(count)

        if current_word == word:
            word_counts[doc_id] += count
        else:
            if current_word:
                output = ' '.join(f"{doc_id}:{count}" for doc_id, count in word_counts.items())
                print(f"{current_word}\t{output}")
            current_word = word
            word_counts = defaultdict(int)
            word_counts[doc_id] = count

    if current_word:
        output = ' '.join(f"{doc_id}:{count}" for doc_id, count in word_counts.items())
        print(f"{current_word}\t{output}")


if __name__ == "__main__":
    if sys.argv[1] == "mapper":
        mapper()
    elif sys.argv[1] == "reducer":
        reducer()
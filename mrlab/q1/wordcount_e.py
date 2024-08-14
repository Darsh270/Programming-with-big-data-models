from mrjob.job import MRJob
import re

class WordCountWithE(MRJob):
    def mapper(self, _, line):
        # Use regular expression to split the line into words, keeping punctuation
        words = re.findall(r"\S+", line)
        for w in words:
            if 'e' in w:  # Check for lowercase 'e' in each word
                yield w, 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    WordCountWithE.run()


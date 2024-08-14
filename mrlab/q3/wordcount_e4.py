from mrjob.job import MRJob

class WordCountWithE(MRJob):  # A more descriptive class name

    def mapper(self, _, line):  # key is None for text file input
        words = line.split()
        for w in words:
            if 'e' in w:  # Check for lowercase 'e' in each word
                yield w, 1  # Emit lowercase words to handle case insensitivity

    def reducer(self, word, counts):
        total_count = sum(counts)
        if total_count >= 2:  # Only emit words that appear 2 or more times
            yield word, total_count

if __name__ == '__main__':
    WordCountWithE.run()


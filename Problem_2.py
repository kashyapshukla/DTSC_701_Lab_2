# Import necessary libraries
from mrjob.job import MRJob
import re


# Define the MapReduce job
class NonStopWordCount(MRJob):
    # List of stopwords
    stopwords = set(['the', 'and', 'of', 'a', 'to', 'in', 'is', 'it', 'as','be'])

    # Mapper function
    def mapper(self, _, line):
        # Tokenize the line into words using regular expressions
        words = re.findall(r'\b\w+\b', line.lower())

        # Emit key-value pairs for each non-stop word
        for word in words:
            if word not in self.stopwords:
                yield (word, 1)

    # Reducer function
    def reducer(self, word, counts):
        # Sum the counts to get the total count of the word
        total_count = sum(counts)

        # Emit the word and its count
        yield (word, total_count)


if __name__ == '__main__':
    NonStopWordCount.run()

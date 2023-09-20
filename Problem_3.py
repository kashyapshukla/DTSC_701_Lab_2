# Import necessary libraries
from mrjob.job import MRJob
import re


# Define the MapReduce job
class WordBigramCount(MRJob):

    # Mapper function
    def mapper(self, _, line):
        # Tokenize the line into words using regular expressions
        words = re.findall(r"[\w']+", line.lower())

        # Emit word bigrams as key-value pairs
        for i in range(len(words) - 1):
            bigram = f"{words[i]},{words[i + 1]}"
            yield (bigram, 1)

    # Reducer function
    def reducer(self, bigram, counts):
        # Sum the counts to get the total count of the bigram
        total_count = sum(counts)

        # Emit the bigram and its count
        yield (bigram, total_count)


if __name__ == '__main__':
    WordBigramCount.run()

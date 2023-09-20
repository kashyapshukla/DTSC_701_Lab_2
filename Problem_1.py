# Import necessary libraries
import mrjob.job
import re


# Define the MapReduce job
class UniqueWordCount(mrjob.job.MRJob):

    # Mapper function
    def mapper(self, _, line):
        # Tokenize the line into words using regular expressions
        words = re.findall(r'\b\w+\b', line.lower())

        # Emit key-value pairs for each word
        for word in words:
            yield (word, 1)

    # Reducer function
    def reducer(self, word, counts):
        # Sum the counts to get the total count of the word
        total_count = sum(counts)

        # Emit the word and its count
        yield (word, total_count)


if __name__ == '__main__':
    UniqueWordCount.run()

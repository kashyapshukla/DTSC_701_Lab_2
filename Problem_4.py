# Import necessary libraries
from mrjob.job import MRJob
import re

# Define the MapReduce job
class InvertedIndex(MRJob):

    # Mapper function
    def mapper(self, _, line):
        # Tokenize the line into words using regular expressions
        words = re.findall(r'\b\w+\b', line.lower())

        # Extract the document name (assuming the line format is "Document X: ...")
        doc_name = line.split(':')[0].strip()

        # Emit key-value pairs for each word and its corresponding document name
        for word in words:
            yield (word, doc_name)

    # Reducer function
    def reducer(self, word, doc_names):
        # Create a set to store unique document names for the word
        unique_docs = set()

        # Add each document name to the set
        for doc_name in doc_names:
            unique_docs.add(doc_name)

        # Emit the word and the list of document names where it appears
        yield (word, list(unique_docs))

if __name__ == '__main__':
    InvertedIndex.run()

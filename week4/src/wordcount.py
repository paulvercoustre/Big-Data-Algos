"""
Print the 10 most common words from a text. This is a great test to check that
your install of Spark is functional.

Usage:

    ./spark-2.1.0-bin-hadoop2.7/bin/spark-submit ./src/wordcount.py data/homer/iliad.mb.txt
"""
import sys
import pyspark


def main():
    sc = pyspark.SparkContext()

    # "lines" is an RDD (Resilient Distributed Dataset)
    lines = sc.textFile(sys.argv[1])  # creates an RDD where each object is a line
    top_words = lines.flatMap(lambda line: line.split())\
        .map(lambda word: word.lower())\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda c1, c2: c1 + c2)\
        .sortBy(lambda w: w[1], ascending=False)\
        .take(10)

    for word, count in top_words:
        print("{} {}".format(count, word))

#raw_input("press control + C to exit")

if __name__ == "__main__":
    main()

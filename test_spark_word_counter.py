import unittest
import os
from pyspark.sql import SparkSession
from word_count_spark import WordCount

class TestWordCount(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestWordsCount").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        # Creating temporary files
        self.keywords_file = 'test_keywords.txt'
        with open(self.keywords_file, 'w') as f:
            f.write("Name\nDetect\nAI")

        self.text_file = 'test_input_text.txt'
        with open(self.text_file, 'w') as f:
            f.write("Detecting first names is tricky to do even with AI.\n")
            f.write("How do you say a street name is not a first name?\n")

        # Initialize the WordCount
        self.word_counter = WordCount(self.spark, self.keywords_file)

    def tearDown(self):
        # Removing temporary files
        os.remove(self.keywords_file)
        os.remove(self.text_file)

    def test_load_keywords(self):
        expected_keywords = {"name", "detect", "ai"}
        actual_keywords = set(row.keyword for row in self.word_counter.keywords_df.collect())
        self.assertEqual(actual_keywords, expected_keywords)

    def test_count_word_matches(self):
        word_count_df = self.word_counter.count_word_matches(self.text_file)
        result = {row['keyword']: row['match_count'] for row in word_count_df.collect()}
        expected_counts = {"name": 2, "ai": 1}
        self.assertEqual(result, expected_counts)

    def test_empty_keywords_file(self):
        with open(self.keywords_file, 'w') as f:
            f.write("")
        self.word_counter = WordCount(self.spark, self.keywords_file)
        word_count_df = self.word_counter.count_word_matches(self.text_file)
        result = {row['keyword']: row['match_count'] for row in word_count_df.collect()}
        expected_counts = {}
        self.assertEqual(result, expected_counts)

    def test_empty_text_file(self):
        with open(self.text_file, 'w') as f:
            f.write("")
        word_count_df = self.word_counter.count_word_matches(self.text_file)
        result = {row['keyword']: row['match_count'] for row in word_count_df.collect()}
        expected_counts = {}
        self.assertEqual(result, expected_counts)

    def test_case_insensitivity(self):
        with open(self.text_file, 'w') as f:
            f.write("NAME name NaMe nAmE")
        word_count_df = self.word_counter.count_word_matches(self.text_file)
        result = {row['keyword']: row['match_count'] for row in word_count_df.collect()}
        expected_counts = {"name": 4}
        self.assertEqual(result, expected_counts)

    def test_non_matching_text(self):
        with open(self.text_file, 'w') as f:
            f.write("This text does not match any keywords.\n")
        word_count_df = self.word_counter.count_word_matches(self.text_file)
        result = {row['keyword']: row['match_count'] for row in word_count_df.collect()}
        expected_counts = {}
        self.assertEqual(result, expected_counts)

if __name__ == "__main__":
    unittest.main()

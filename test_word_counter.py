import unittest
import os
from word_count_python import WordCounter

class TestWordCounter(unittest.TestCase):

    def setUp(self):
        # Creating temporary test file
        self.keywords_file = 'test_keywords.txt'
        with open(self.keywords_file, 'w') as f:
            f.write("Name\nDetect\nAI")

        self.text_file = 'test_input_text.txt'
        with open(self.text_file, 'w') as f:
            f.write("Detecting first names is tricky to do even with AI.\n")
            f.write("How do you say a street name is not a first name?\n")

        self.word_counter = WordCounter(self.keywords_file, self.text_file)

    def tearDown(self):
        # Removing temporary files
        os.remove(self.keywords_file)
        os.remove(self.text_file)

    def test_load_keywords(self):
        expected_keywords = {"name", "detect", "ai"}
        self.assertEqual(self.word_counter.keywords, expected_keywords)

    def test_count_words_in_line(self):
        self.word_counter.count_words_in_line("Detecting first names is tricky to do even with AI.")
        expected_counts = {"ai": 1}
        self.assertEqual(self.word_counter.get_word_counts(), expected_counts)

    def test_process_text(self):
        self.word_counter.process_text()
        expected_counts = {"name": 2, "ai": 1}
        self.assertEqual(self.word_counter.get_word_counts(), expected_counts)
    
    def test_empty_keywords_file(self):
        with open(self.keywords_file, 'w') as f:
            f.write("")
        self.word_counter = WordCounter(self.keywords_file, self.text_file)
        self.word_counter.process_text()
        expected_counts = {}
        self.assertEqual(self.word_counter.get_word_counts(), expected_counts)

    def test_empty_text_file(self):
        with open(self.text_file, 'w') as f:
            f.write("")
        self.word_counter = WordCounter(self.keywords_file, self.text_file)
        self.word_counter.process_text()
        expected_counts = {}
        self.assertEqual(self.word_counter.get_word_counts(), expected_counts)

    def test_case_insensitivity(self):
        self.word_counter.count_words_in_line("NAME name NaMe nAmE")
        expected_counts = {"name": 4}
        self.assertEqual(self.word_counter.get_word_counts(), expected_counts)

    def test_non_matching_text(self):
        with open(self.text_file, 'w') as f:
            f.write("This text does not match any keywords.\n")
        self.word_counter = WordCounter(self.keywords_file, self.text_file)
        self.word_counter.process_text()
        expected_counts = {}
        self.assertEqual(self.word_counter.get_word_counts(), expected_counts)

    def test_text_with_punctuation(self):
        with open(self.text_file, 'w') as f:
            f.write("Detecting first names is tricky to do, even with AI!\n")
            f.write("How do you say a street name is not a first name?\n")
        self.word_counter = WordCounter(self.keywords_file, self.text_file)
        self.word_counter.process_text()
        expected_counts = {"name": 2, "ai": 1}
        self.assertEqual(self.word_counter.get_word_counts(), expected_counts)

    def test_large_input_text(self):
        with open(self.text_file, 'w') as f:
            for _ in range(1000):
                f.write("Detecting first names is tricky to do even with AI.\n")
                f.write("How do you say a street name is not a first name?\n")
        self.word_counter = WordCounter(self.keywords_file, self.text_file)
        self.word_counter.process_text()
        expected_counts = {"name": 2000, "ai": 1000}
        self.assertEqual(self.word_counter.get_word_counts(), expected_counts)

if __name__ == "__main__":
    unittest.main()

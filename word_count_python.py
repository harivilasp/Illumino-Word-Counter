from collections import defaultdict
import pandas as pd
import re

class WordCounter:
    def __init__(self, keywords_file: str, text_file: str):
        self.keywords_file = keywords_file
        self.text_file = text_file
        self.keywords = self.load_keywords()
        self.word_counts = defaultdict(int)

    def load_keywords(self):
        with open(self.keywords_file, 'r', encoding='ascii') as file:
            keywords = file.read().splitlines()
        return set(keyword.lower() for keyword in keywords)

    def process_text(self):
        with open(self.text_file, 'r', encoding='ascii') as file:
            for line in file:
                self.count_words_in_line(line)

    def count_words_in_line(self, line: str):
        words = re.findall(r'\b\w+\b', line.lower())
        for word in words:
            if word in self.keywords:
                self.word_counts[word] += 1

    def display_results(self):
        df = pd.DataFrame(list(self.word_counts.items()), columns=['Keyword', 'Match Count'])
        print(df)
    
    def get_word_counts(self):
        return dict(self.word_counts)

if __name__ == "__main__":
    keywords_file = 'keywords.txt'
    text_file = 'input_text.txt'

    word_counter = WordCounter(keywords_file, text_file)
    word_counter.process_text()
    word_counter.display_results()

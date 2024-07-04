from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, trim
from pyspark.sql import DataFrame

class WordCount:
    def __init__(self, spark: SparkSession, keywords_file: str):
        self.spark = spark
        self.keywords_file = keywords_file
        self.keywords_df = self.load_keywords()

    def load_keywords(self) -> DataFrame:
        keywords_df = self.spark.read.text(self.keywords_file)
        keywords_df = keywords_df.withColumnRenamed('value', 'keyword')
        keywords_df = keywords_df.withColumn('keyword', lower(col('keyword')))
        return keywords_df

    def count_word_matches(self, input_text_file: str) -> DataFrame:
        input_text_df = self.spark.read.text(input_text_file)
        words_exploded_df = input_text_df.select(explode(split(lower(trim(col('value'))), r'\W+')).alias('word'))
        matched_words_df = words_exploded_df.join(self.keywords_df, words_exploded_df.word == self.keywords_df.keyword, 'inner')
        word_count_df = matched_words_df.groupBy('keyword').count().withColumnRenamed('count', 'match_count')
        return word_count_df

    def display_results(self, word_count_df: DataFrame):
        word_count_df.show(truncate=False)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Words Count").getOrCreate()

    input_text_file = 'input_text.txt'
    keywords_file = 'keywords.txt'

    word_matcher = WordCount(spark, keywords_file)
    word_count_df = word_matcher.count_word_matches(input_text_file)
    word_matcher.display_results(word_count_df)

    spark.stop()

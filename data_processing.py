from spark_functions import init_spark_session
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

from pyspark.sql.functions import udf, col, concat_ws, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from dotenv import load_dotenv
import os


def init_sentiment_pipeline() -> pipeline:
  MODEL_NAME: str = 'cardiffnlp/twitter-roberta-base-sentiment'
  tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
  model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

  return pipeline(
    'sentiment-analysis',
    model=model,
    tokenizer=tokenizer,
    truncation=True,
    max_length=512,
  )

def map_sentiment(label: str, conf: float) -> float:
  MAPPING = {'LABEL_0': -1, 'LABEL_1': 0, 'LABEL_2': 1}
  return MAPPING.get(label, 0) * conf

def map_label(label: str) -> str:
  MAPPING = {'LABEL_0': 'negative', 'LABEL_1': 'neutral', 'LABEL_2': 'positive'}
  return MAPPING.get(label, 'neutral')


sentiment_pipe = None


def get_sentiment(text: str) -> tuple[str, float]:
  global sentiment_pipe
  if sentiment_pipe is None:
    sentiment_pipe = init_sentiment_pipeline()

  result = sentiment_pipe(text)[0]
  return (str(result['label']), float(result['score']))

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def get_sentiment_score(text: str) -> float:
    if text is None:
        return 0.0
    analyzer = SentimentIntensityAnalyzer()

    # compound score is a single metric for positive/negative
    score = analyzer.polarity_scores(text)['compound']
    return score

spark_sentiment_udf = udf(get_sentiment_score, FloatType())

def main():

  spark = init_spark_session()

  base_df = spark.read.json('posts.jsonl')

  try:
    df_combined = base_df.withColumn("full_text", 
        concat_ws(" ", col("title"), col("body"))
    )

    #SENTIMENT_SCHEMA = StructType([
    #  StructField('label', StringType(), True),
    #  StructField('conf', FloatType(), True)
    #])

    #get_sentiment_udf = udf(get_sentiment, SENTIMENT_SCHEMA)
    #map_sentiment_udf = udf(map_sentiment, FloatType())
    #map_label_udf = udf(map_label, StringType())

    sentiment_df = df_combined.withColumn('sentiment_score', spark_sentiment_udf(col('full_text')))
    df_final = sentiment_df.withColumn("post_date", to_date(col("date_created").cast("timestamp")))

    # processed_df = sentiment_df.withColumn('sentiment_label', col('sentiment_struct.label'))\
     #                   .withColumn('sentiment_confidence', col('sentiment_struct.conf'))\
     #                   .drop('sentiment_struct')

    # scored_df = processed_df.withColumn('sentiment_score', map_sentiment_udf(col('sentiment_label'), col('sentiment_confidence')))

    # labeled_df = scored_df.withColumn('sentiment_label', map_label_udf(col('sentiment_label')))
    # labeled_df.show(10)

    load_dotenv()
    df_final.write \
        .format("jdbc") \
        .option("url", os.getenv('DB_URL')) \
        .option("dbtable", "reddit_posts") \
        .option("user", os.getenv('DB_USER')) \
        .option("password", os.getenv('DB_PASSWORD')) \
        .option("driver", os.getenv('DB_DRIVER')) \
        .mode("overwrite") \
        .save()

  except Exception as e:
    print(f"An error occurred: {e}")

  finally:
    spark.stop()
    print("Spark session stopped.")

if __name__ == '__main__':
  main()
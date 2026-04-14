from pyspark.sql import SparkSession, DataFrame

import os
from dotenv import load_dotenv

def init_spark_session() -> SparkSession:
  load_dotenv()
  DRIVER_PATH = os.getenv("DRIVER_PATH")

  spark_session = SparkSession.Builder() \
                  .appName("RedditSentimentAnalysis")\
                  .config("spark.driver.extraClassPath", DRIVER_PATH)\
                  .getOrCreate()

  spark_session.sparkContext.setLogLevel('info')

  return spark_session


def load_mysql_dataframe(session: SparkSession, table_name: str) -> DataFrame:
  load_dotenv()

  DB_URL = os.getenv('DB_URL')
  DB_USER = os.getenv('DB_USER')
  DB_PASSWORD = os.getenv('DB_PASSWORD')
  DB_DRIVER = os.getenv('DB_DRIVER')

  return session.read.jdbc(DB_URL, table_name, properties={
    'user': DB_USER,
    'password': DB_PASSWORD,
    'driver': DB_DRIVER
  })
          
if __name__ == '__main__':
  spark_session = init_spark_session()
  df = load_mysql_dataframe(spark_session, "sentiment_trends")
  df.show()
  
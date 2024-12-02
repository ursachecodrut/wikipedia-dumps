from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
import requests

DUMPS_DIR = "./wikipedia_dumps/*.xml.bz2"
START_DATE = "20241201"
END_DATE = "20241231"

WIKI_API_BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/simple.wikipedia/all-access/all-agents"

def fetch_pageviews(title, start_date = START_DATE, end_date = END_DATE):
    """
    Fetch pageview data for a given page title using Wikimedia Pageviews API.
    """

    url = f"{WIKI_API_BASE_URL}/{title}/monthly/{start_date}/{end_date}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        views = sum(item["views"] for item in data["items"])
        return (title, views)
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for {title}: {e}")
        return (title, None)
        

spark = SparkSession.builder \
    .appName("Wikipedia dump parser") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") \
    .config("spark.hadoop.security.authorization", "false") \
    .config("spark.hadoop.security.authentication", "simple") \
    .getOrCreate()

page_schema = StructType([
    StructField("id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("revision", StructType([
        StructField("id", LongType(), True),
        StructField("parentid", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("contributor", StructType([
            StructField("username", StringType(), True),
            StructField("id", LongType(), True),
        ]), True),
        StructField("model", StringType(), True),
        StructField("format", StringType(), True),
        StructField("text", StringType(), True),
        StructField("sha1", StringType(), True),
    ]), True),
])

pages_df = spark.read \
    .format("xml") \
    .option("rootTag", "pages") \
    .option("rowTag", "page") \
    .load(DUMPS_DIR, schema=page_schema)

pages_df.printSchema()

pages = pages_df.select(
    pages_df.id.cast("long").alias("id"),  
    pages_df.title.alias("title"),        
    pages_df.revision.timestamp.alias("timestamp"),
)

pages.show(10)

# titles_rdd = pages.select("title").rdd.flatMap(lambda row: [row.title])
#
# pageviews_rdd = titles_rdd.map(lambda title: fetch_pageviews(title))
#
# pageviews_schema = StructType([
#     StructField("title", StringType(), True),
#     StructField("views", LongType(), True),
# ])
# pageviews_df = spark.createDataFrame(pageviews_rdd, schema=pageviews_schema)
#
# pageviews_df.show(10)


spark.stop()


import os
import download
import countries
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def dbg(x):
  """ A helper function to print debugging information on RDDs """
  if isinstance(x, pyspark.RDD):
    print([(t[0], list(t[1]) if
            isinstance(t[1], pyspark.resultiterable.ResultIterable) else t[1])
           if isinstance(t, tuple) else t
           for t in x.take(100)])
  else:
    print(x)

try:
  os.environ['PYSPARK_PYTHON'] = 'python3'

  conf = SparkConf().setMaster('local[*]').set('spark.executor.memory', '16g')
  sc = SparkContext(conf=conf)
  sql = SQLContext(sc)
except:
  pass

# Constant list of event codes which correspond to a meeting. While I'm sure there are
# better ways at determining relevance here, this is the best way that wouldn't require
# an insane amount of research and investigate to determine.
MEETING_EVENT_CODES = ['036', '038', '042', '043', '044']

DEVELOPED_COUNTRIES = ['NZL', 'AUS', 'GBR', 'USA', 'CAN']

def process(rdd):
  prevalence_vector = rdd.filter(
    lambda row: row['Actor1CountryCode'] in DEVELOPED_COUNTRIES
  ).map(
    # Store the country name, whether the event type matches what we need, and a 1 to allow
    # computation of the total number of rows in this RDD
    lambda row: (row['Actor1CountryCode'], (int(row['EventCode'] in MEETING_EVENT_CODES), 1))
  ).reduceByKey(
    lambda acc, cur: (acc[0] + cur[0], acc[1] + cur[1])
  ).map(
    lambda row: row[1][0] / row[1][1]
  )

  dbg(prevalence_vector)

def load_file(date):
  formatted = download.date_formatted(date)
  path = os.path.join(download.GDELT_OUTPUT_DIRECTORY, f'{download.GDELT_FILE_PREFIX}{formatted}.csv')
  return sql.read.option('header', 'true').csv(path)

def load_rdds():
  '''Loads data from all dates into an RDD for analysis, takes a long time'''
  # Totals about 64 million records
  all_rdds = [(date, load_file(date).rdd) for date in download.all_dates()]
  for date, rdd in all_rdds:
    print(date)
    process(rdd)
    print()

load_rdds()

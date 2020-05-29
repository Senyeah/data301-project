import os
import download
import countries
import pyspark
import numpy
import pandas as pd
from functools import reduce
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
  conf = SparkConf().setMaster('local[*]').set('spark.executor.memory', '1g')
  sc = SparkContext(conf=conf)
  sql = SQLContext(sc)
except:
  pass

# Constant list of event codes which correspond to a meeting. While I'm sure there are
# better ways at determining relevance here, this is the best way that wouldn't require
# an insane amount of research and investigate to determine.
MEETING_EVENT_CODES = ['036', '038', '043', '044']
DEVELOPED_COUNTRIES = ['NZL', 'AUS', 'GBR', 'USA', 'CAN']

def merge_dicts(a, b):
  '''Merges key–values from both a and b. If the same value exists for a given key in both a
  and b, then the resulting value is the two added.'''
  merged = dict()
  for key in set([*a.keys(), *b.keys()]):
    if key in a and key in b:
      value = a[key] + b[key]
    else:
      value = a[key] if key in a else b[key]
    merged.update({ key: value })
  return merged

def compute_prevalence_vectors(rdd):
  '''Computes prevalence vectors for a given country (that is, the proportion of 'meetings' out of
  a total number of events), grouped by day'''

  return rdd.filter(
    # Only care about countries we've explicitly flagged for analysis
    lambda row: row['Actor1CountryCode'] in DEVELOPED_COUNTRIES
  ).map(
    # Store the country name, whether the event type matches what we need, and a 1 to allow
    # computation of the total number of rows in this RDD
    lambda row: (
      (row['DATEADDED'][:8], row['Actor1CountryCode']), # Composite key (date contains time too so truncate)
      (int(row['EventCode'] in MEETING_EVENT_CODES), 1) # Event occurrence count
    )
  ).reduceByKey(
    # Add together the occurrences of meetings and total events
    lambda acc, curr: (acc[0] + curr[0], acc[1] + curr[1])
  ).mapValues(
    # Convert to a proportion of meetings for that date–country pair.
    # Use safe division, just to make sure nothing blows up: x / y if y > 0 else 0
    lambda row: row[0] / row[1] if row[1] > 0 else 0
  ).map(
    # Now group by only the date, row is in the form ((date, country), prevalence):
    # transform it into (date, (country, prevalence)) so that the dict constructor
    # can map _values_ to the form { country1: prevalence1, country2: prevalence2... }
    lambda row: (row[0][0], (row[0][1], row[1]))
  ).groupByKey().mapValues(dict)

def slice_key(slice):
  '''Represents the beginning date of the slice, therefore representing a mean prevalence until this
  value, plus the number of days per slice.'''
  return slice[-1]

def mean_slice_prevalence(slices, grouped_prevalences):
  for slice in slices:
    slice_result = grouped_prevalences.filter(
      lambda row: row[0] in slice
    ).map(
      # The slice is identified by the lowest date index present (i.e. the last item)
      lambda row: (slice_key(slice), row[1])
    ).mapValues(
      # Convert to a single item in a list to allow for the reduction to add the values
      lambda country_vals: { country: [prev] for country, prev in country_vals.items() }
    ).reduceByKey(
      # Collect multiple prevalences into one to allow the mean of the time slice to be computed
      merge_dicts
    ).mapValues(
      # Now compute the mean of the prevalences for the given time slice
      lambda country_vals: { country: numpy.mean(vals) for country, vals in country_vals.items() }
    ).collectAsMap()

    yield slice_result

def process():
  '''Processes the data in order to quantitatively answer the research question'''
  # Totals about 54 million records, 20 minutes to process on my machine (Core i7-4771, 8 workers)
  dates, slices = download.all_dates()

  # Only keep rows which are of interest
  files = [
    os.path.join(download.GDELT_OUTPUT_DIRECTORY, f'{download.GDELT_FILE_PREFIX}{date}.csv')
    for date in dates
  ]

  # Bizarrely, the event dates don't correspond to the dates of the file. This can make some results
  # inaccurate. Also, the SQLDATE field is hilariously inaccurate – it has articles apparently from
  # 2010 in here?!
  events_rdd = sql.read.csv(files, header=True).rdd

  # A dict of country event prevalences, grouped by day
  day_prevalence_vectors = compute_prevalence_vectors(events_rdd)

  # Now determine slice prevalences by aggregating the result across multiple days:
  #
  # Note: this is often not very useful, as the dates expected just aren't present within the set
  # of events for the days downloaded. That means if I load event data for say 2020-05-28, it is not
  # guaranteed to contain any (relevant) data for 2020-05-28, and in fact might contain relevant
  # data for 2010-01-01. Neither of the date fields (SQLDATE or DATEADDED) make any sort of sense,
  # sometimes directly contradicting the timestamp of the linked article. This is absolutely stupid
  # and has driven me _beyond_ insane.
  #
  # While this _might_ be more accurate when considering every file (as event X for day Y might be
  # present in event data for day Z), I can't test that for each run locally as it takes 20+ minutes
  # to process with every downloaded data set (54M records).
  event_prevalences = day_prevalence_vectors.cache()
  slice_prevalences = reduce(merge_dicts, mean_slice_prevalence(slices, event_prevalences))

  # It will be the case that prevalences for slice data requested did not end up being computed
  # since no rows matched the predicates. Therefore just log what keys didn't make it
  slice_dates = map(slice_key, slices)
  casualties = set(slice_dates) - set(slice_prevalences.keys())
  print('Unable to calculate time slices', liwst(casualties), 'since no relevant data found :(')

  # Now create a dataframe to perform calculations
  prevalence_df = pd.DataFrame.from_dict(slice_prevalences, orient='index')
  pd.set_option('display.max_rows', prevalence_df.shape[0] + 1)
  print(prevalence_df)

process()

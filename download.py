import os
import gdelt
import itertools
import subprocess
import pandas as pd
from concurrent.futures import thread
from datetime import datetime, timezone
from urllib import request

# Number of days to analyse GDELT data from, constituting a slice of time for which analysis is
# performed. Defaults to analysing blocks of 5 days in length.
ANALYSIS_SLICE_PERIOD_DAYS = 5

# How many sequential slices should be computed to form a block of analysis. Defaults to 36,
# which along with the default slice period, corresponds to an analysis block length of 180 days.
ANALYSIS_SLICE_MULTIPLIER = 36

# The frequency in days for which blocks should be analysed
ANALYSIS_FREQUENCY_DAYS = 365

# The amount of historical analysis blocks to compute. With the default frequency of 365 days and
# start date of today, this corresponds to the preceding 2 years.
ANALYSIS_BLOCK_COUNT = 2

# The time from which to begin the analysis
ANALYSIS_START_DATE = datetime.now(timezone.utc)

def analysis_dates():
  for block_index in range(ANALYSIS_BLOCK_COUNT):
    # Determine the day offset within the given block
    block_offset = pd.to_timedelta(ANALYSIS_FREQUENCY_DAYS * block_index, unit='D')

    # Now compute each period of days inside a block
    for slice_index in range(ANALYSIS_SLICE_MULTIPLIER):
      # Determine the start date based on the total number of slices to compute
      slice_offset = pd.to_timedelta(ANALYSIS_SLICE_PERIOD_DAYS * slice_index, unit='D')

      # Determine the dates within this block, given the slice and block offsets
      slice_idx = pd.date_range(
        ANALYSIS_START_DATE - slice_offset - block_offset,
        freq='-1D', periods=ANALYSIS_SLICE_PERIOD_DAYS
      )

      # Return the index of the block so its correponding block of dates have some context
      yield (slice_idx, block_index)

# Event download parameters
GDELT_FILE_PREFIX = 'gdelt_events_'
GDELT_OUTPUT_DIRECTORY = 'gdelt_download'

# How many GDELT event files to download in parallel
GDELT_DOWNLOAD_WORKERS = os.cpu_count()

def date_formatted(date):
  return date.strftime('%Y%m%d')

def download_date(date, gd):
  out_path = os.path.join(GDELT_OUTPUT_DIRECTORY, f'{GDELT_FILE_PREFIX}{date_formatted(date)}.csv')

  # Don't pointlessly re-download data
  if not os.path.exists(out_path):
    print('Downloading data for date', formatted)
    try:
      # Download every 15 minute segment for every day, not just the latest (very slow...)
      data = gd.Search(formatted, table='events', coverage=True)
      data.to_csv(out_path, encoding='utf-8', index=False)
    except Exception as e:
      print(f'Error downloading {formatted}, exception {e}')

def all_dates():
  # Stop polluting the working directory by creating an download folder
  if not os.path.exists(GDELT_OUTPUT_DIRECTORY):
    # I have already downloaded a majority of this data and compressed it, so attempt to
    # restore most of what is needed from the bucket (this will save about an hour)
    CACHED_ARCHIVE = 'gdelt.tar.gz'
    BUCKET_URL = f'https://storage.googleapis.com/data301-bucket-9n5z0ph0/{CACHED_ARCHIVE}'

    # Place to store all the data, needs about 30 GB disk space
    os.mkdir(GDELT_OUTPUT_DIRECTORY)

    try:
      # Download the file, then just call tar to do the extraction (sorry Windows)
      print('Downloading cached archive from', BUCKET_URL)
      request.urlretrieve(BUCKET_URL, CACHED_ARCHIVE)
      print('Extracting compressed archive...')
      subprocess.run(['tar', 'zxf', CACHED_ARCHIVE, '-C', GDELT_OUTPUT_DIRECTORY])
      print('Extraction complete!')
      os.remove(CACHED_ARCHIVE)
    except:
      # Bucket won't exist forever :(
      print('Failed downloading URL', BUCKET_URL)

  # Initialise gdelt so its API can be queried
  gd = gdelt.gdelt(version=2)
  downloaded_dates = []

  # Parallelize the download, as lots of event data is needed
  with thread.ThreadPoolExecutor(max_workers=GDELT_DOWNLOAD_WORKERS) as executor:
    # Pull every date out of the slice...
    slices = [slice for slice, _ in analysis_dates()]
    dates = (date for dates in slices for date in dates)

    # ...log how many are to be downloaded
    download_cnt = ANALYSIS_SLICE_PERIOD_DAYS * ANALYSIS_SLICE_MULTIPLIER * ANALYSIS_BLOCK_COUNT
    print('Downloading', download_cnt, 'event files...')

    # ...and then download the corresponding GDELT event data for that day
    for date in dates:
      executor.submit(download_date, date, gd)
      downloaded_dates.append(date_formatted(date))

  print('Download complete')
  # All dates flattened, along with every (formatted) date in each slice of dates
  return downloaded_dates, [[date_formatted(date) for date in dates] for dates in slices]

if __name__ == '__main__':
  # Raw time to download approx 2 hours – around 25 GB(!) when downloaded, I have cached ~360 files
  dates, slices = all_dates()
  print(dates, slices)

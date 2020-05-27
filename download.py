import os
import gdelt
import itertools
import pandas as pd
from concurrent.futures import thread
from datetime import datetime, timezone

# Number of days to analyse GDELT data from, constituting a slice of time for which analysis is
# performed. Defaults to analysing blocks of 5 days in length.
ANALYSIS_SLICE_PERIOD_DAYS = 5

# How many sequential slices should be computed to form a block of analysis. Defaults to 36,
# which along with the default slice period, corresponds to an analysis block length of 180 days.
ANALYSIS_SLICE_MULTIPLIER = 36

# The frequency in days for which blocks should be analysed
ANALYSIS_FREQUENCY_DAYS = 365

# The amount of historical analysis blocks to compute. With the default frequency of 365 days and
# start date of today, this corresponds to the preceding 5 years.
ANALYSIS_BLOCK_COUNT = 5

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
GDELT_DOWNLOAD_WORKERS = 8

def download_date(date, gd):
  formatted = date.strftime('%Y%m%d')
  out_path = os.path.join(GDELT_OUTPUT_DIRECTORY, f'{GDELT_FILE_PREFIX}{formatted}.csv')

  # Don't pointlessly re-download data
  if not os.path.exists(out_path):
    data = gd.Search(formatted, table='events', coverage=False)
    data.to_csv(out_path, encoding='utf-8', index=False)

def download_all():
  # Stop polluting the working directory by creating an download folder
  if not os.path.exists(GDELT_OUTPUT_DIRECTORY):
    os.mkdir(GDELT_OUTPUT_DIRECTORY)

  gd = gdelt.gdelt(version=2)

  # Parallelize the download, as lots of it is needed
  with thread.ThreadPoolExecutor(max_workers=GDELT_DOWNLOAD_WORKERS) as executor:
    # Pull every date out of the slice...
    dates = (date for slice, _ in analysis_dates() for date in slice)

    # Log how many are to be downloaded
    download_cnt = ANALYSIS_SLICE_PERIOD_DAYS * ANALYSIS_SLICE_MULTIPLIER * ANALYSIS_BLOCK_COUNT
    print('Downloading', download_cnt, 'event files...')

    # ...and then download the corresponding GDELT event data for that day
    for date in dates:
      executor.submit(download_date, date, gd)

  print('Download complete')

if __name__ == "__main__":
  download_all()

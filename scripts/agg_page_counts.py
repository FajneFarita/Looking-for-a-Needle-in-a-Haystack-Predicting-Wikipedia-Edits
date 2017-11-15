import csv
import datetime
import gzip
import os
import random
import time
import urllib.request

from collections import defaultdict
from datetime import timedelta

# To run: python scripts/agg_page_counts.py
# Data from https://dumps.wikimedia.org/other/pagecounts-raw/

# Convenience function to also print elapsed time for simple analysis
def ts_print(s, start_time):
  print('{}:  {}'.format(str(timedelta(seconds=time.time() - start_time)), s))

def create_dir(f):
  # Create the appropriate directory if necessary.
  if not os.path.exists(os.path.dirname(f)):
    os.makedirs(os.path.dirname(f))

# Given a date, download the hourly files of pagecounts, process some stats
# aggregated by the article name and namespace, and write them to a file.
def run(curr_date, start_time):
  try:
    date_str = curr_date.strftime("%Y%m%d")
    # Keep track of view counts per article on that day.
    view_counts = defaultdict(int)
    # Keep track of view article sized on that day.
    sizes = defaultdict(list)

    for hour in range(24):
      # Helper variable to print status updates while running.
      num_articles = 0

      ts_print('Downloading file for date {} hour {}'.format(date_str, hour), start_time)
      local_filename, _ = urllib.request.urlretrieve(
        'https://dumps.wikimedia.org/other/pagecounts-raw/{}/{}/pagecounts-{}-{:02d}0000.gz'.format(
          curr_date.year,
          curr_date.strftime('%Y-%m'),
          date_str,
          hour
      ))
      with gzip.open(local_filename, 'rb') as f:
        # Keep track of whether en rows have been seen to exit file reading early.
        seen_en_rows = False

        for line in f:
          # Only record stats for english-language views.
          if line[:3] == b'en ':
            at_en_rows = True
            num_articles += 1
            # There are ~4 lines of garbage "articles"
            try:
              _, article, view_count, byte_size = line.decode().split()
            except:
              # Print out the failed line for sanity checking.
              # print("Failed to read line: {}".format(line.decode().split()))
              continue

            # Keep data to record total view counts and min/max/avg article size.
            view_counts[article] += int(view_count)
            sizes[article].append(int(byte_size))

            # Randomly print status updates to ensure correct running.
            if random.randint(1, 1000000) == 5:
              ts_print('Processing date {} hour {}, row #{}'.format(
                date_str, hour, num_articles), start_time)
          elif seen_en_rows == True:
            # Done reading `en` stats for this file, skip the rest.
            break
        ts_print('Closing file for date {} hour {}'.format(date_str, hour), start_time)

      local_filename = None
  # Always clean up the downloaded files.
  finally:
    urllib.request.urlcleanup()
  
  ts_print('Writing file for date {}'.format(date_str), start_time)
  # Write results for the day. 
  output_file_name = 'data/raw_data/pageviews/pageviews-{}.csv'.format(date_str)
  create_dir(output_file_name)

  with open(output_file_name, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['Article Name', 'view_count', 'size', 'min_size', 'max_size', 'first_size', 'last_size'])
    for article in view_counts:
      # Grab the min/max/avg article size over the day.
      min_size = min(sizes[article])
      max_size = max(sizes[article]) 
      avg_size = int(sum(sizes[article]) / len(sizes[article]))
      first_size = sizes[article][0]
      last_size = sizes[article[-1]]
      writer.writerow([article, view_counts[article], avg_size, min_size, max_size, first_size, last_size])
  urllib.request.urlcleanup()

if __name__ == '__main__':
  start_time = time.time()

  try:

    one_day = datetime.timedelta(days=1)
    start_date = datetime.date(2016, 4, 1)
    end_date = datetime.date(2016, 4, 4)
    curr_date = start_date
   
    # For every day from start_date to end_date (excluding end_date)
    while curr_date < end_date:
      run(curr_date, start_time)
      curr_date += one_day

    ts_print("Complete!", start_time)
  finally:
    ts_print("cleaning up temporary files", start_time)
    urllib.request.urlcleanup()

import argparse
import csv
import datetime
import gc
import numpy
import os
import pandas
import time

from collections import Counter

# This is very memory intensive, so break processing into chunks by article name.
CHUNK_FNS = [
  lambda name: str(name)[0] <= 'A',
  lambda name: str(name)[0] > 'A' and str(name)[0] <= 'C',
  lambda name: str(name)[0] > 'C' and str(name)[0] <= 'E',
  lambda name: str(name)[0] > 'E' and str(name)[0] <= 'J',
  lambda name: str(name)[0] > 'J' and str(name)[0] <= 'M',
  lambda name: str(name)[0] > 'M' and str(name)[0] <= 'O',
  lambda name: str(name)[0] > 'O' and str(name)[0] <= 'Q',
  lambda name: str(name)[0] > 'Q' and str(name)[0] <= 'T',
  lambda name: str(name)[0] > 'T' and str(name)[0] <= 'V',
  lambda name: str(name)[0] > 'V',
]

# Convenience function to also print elapsed time for simple analysis
def ts_print(s, start_time, chunk_i):
  print('{} chunk {}/{}:  {}'.format(str(datetime.timedelta(seconds=time.time() - start_time)), chunk_i, len(CHUNK_FNS), s))

def create_dir(f):
  # Create the appropriate directory if necessary.
  if not os.path.exists(os.path.dirname(f)):
    os.makedirs(os.path.dirname(f))

# Given a string in YYYYMMDD format, return a datetime object of that date.
def date_arg(s):
  return datetime.datetime.strptime(s, '%Y%m%d')

def memory_usage_psutil():
    # return the memory usage in MB
    import psutil
    process = psutil.Process(os.getpid())
    return process.memory_info()

if __name__ == '__main__':
  if os.getcwd().split('/')[-1] != 'info-251-fall-2017-final-project':
    print('Please run from top-level folder of git project, `info-251-fall-2017-final-project`')
    sys.exit()

  parser = argparse.ArgumentParser(description='Combine edit and pageview data for date range.')
  parser.add_argument('-s', '--start_date', type=date_arg,
                      help='Start date in YYYYMMDD format')
  parser.add_argument('-e', '--end_date', type=date_arg,
                      help='End date (exclusive) in YYYYMMDD format')
  args = parser.parse_args()

  start_time = time.time()
  one_day = datetime.timedelta(days=1)
  three_days = datetime.timedelta(days=3)
  seven_days = datetime.timedelta(days=7)
  start_date = args.start_date
  end_date = args.end_date
  curr_date = start_date    

  while curr_date < end_date:
    for chunk_i, filter_fn in enumerate(CHUNK_FNS):
      history_date = curr_date - datetime.timedelta(days=30)

      # Lots of data to keep track of!
      articles = Counter({})
      
      thirty_day_view_count = Counter({})
      seven_day_view_count = Counter({})
      three_day_view_count = Counter({})
      one_day_view_count = Counter({})
      thirty_day_edit_count = Counter({})
      seven_day_edit_count = Counter({})
      three_day_edit_count = Counter({})
      one_day_edit_count = Counter({})
      thirty_day_minor_edit_count = Counter({})
      seven_day_minor_edit_count = Counter({})
      three_day_minor_edit_count = Counter({})
      one_day_minor_edit_count = Counter({})
      avg_size_total = Counter({})
      avg_size_count = Counter({})
      thirty_day_size_total = Counter({})
      thirty_day_size_count = Counter({})
      seven_day_size_total = Counter({})
      seven_day_size_count = Counter({})
      three_day_size_total = Counter({})
      three_day_size_count = Counter({})
      one_day_size_total = Counter({})
      one_day_size_count = Counter({})
      most_recent_size = {}

      thirty_day_view_count_talk = Counter({})
      seven_day_view_count_talk = Counter({})
      three_day_view_count_talk = Counter({})
      one_day_view_count_talk = Counter({})
      thirty_day_edit_count_talk = Counter({})
      seven_day_edit_count_talk = Counter({})
      three_day_edit_count_talk = Counter({})
      one_day_edit_count_talk = Counter({})
      thirty_day_minor_edit_count_talk = Counter({})
      seven_day_minor_edit_count_talk = Counter({})
      three_day_minor_edit_count_talk = Counter({})
      one_day_minor_edit_count_talk = Counter({})
      avg_size_total_talk = Counter({})
      avg_size_count_talk = Counter({})
      thirty_day_size_total_talk = Counter({})
      thirty_day_size_count_talk = Counter({})
      seven_day_size_total_talk = Counter({})
      seven_day_size_count_talk = Counter({})
      three_day_size_total_talk = Counter({})
      three_day_size_count_talk = Counter({})
      one_day_size_total_talk = Counter({})
      one_day_size_count_talk = Counter({})
      most_recent_size_talk = {}

      gc.collect()

      # Todo: Be more time-efficient and don't reread dates of data (if it can
      # all fit in memory)
      while history_date < curr_date:
        date_str = history_date.strftime("%Y%m%d")
        ts_print(
          'Reading data for aggregation date {} from date {}'.format(
            curr_date.strftime("%Y%m%d"), date_str),
          start_time, chunk_i + 1)
        # print(len(thirty_day_view_count), memory_usage_psutil())
        combined_file_name = 'data/intermediate_data/combined-{}.csv'.format(date_str)
        combined_table = pandas.read_csv(combined_file_name)
        # Only process one chunk of articles at a time to save on memory
        combined_table = combined_table[combined_table['Article Name'].apply(filter_fn)]

        ts_print(
          'Processing data for aggregation date {} from date {}'.format(
            curr_date.strftime("%Y%m%d"), date_str),
          start_time, chunk_i + 1)
       
        # Note: We can't default size to 0, as it's possible, for example, 
        # that a talk page was viewed but the main page wasn't for a 
        # certain day.
        columns_to_fill = ['view_count', 'edits', 'minor_edits', 'view_count_talk', 'edits_talk', 'minor_edits_talk']
        combined_table[columns_to_fill] = combined_table[columns_to_fill].fillna(value=0)
        main_views_table = combined_table[pandas.notnull(combined_table['size'])]
        talk_views_table = combined_table[pandas.notnull(combined_table['size_talk'])]

        articles += pandas.Series(1, index=combined_table['Article Name']).to_dict()

        if curr_date - history_date > seven_days:
          thirty_day_view_count += pandas.Series(combined_table['view_count'].values, index=combined_table['Article Name']).to_dict()
          thirty_day_edit_count += pandas.Series(combined_table['edits'].values, index=combined_table['Article Name']).to_dict()
          thirty_day_minor_edit_count += pandas.Series(combined_table['minor_edits'].values, index=combined_table['Article Name']).to_dict()
          thirty_day_size_total += pandas.Series(main_views_table['size'].values, index=main_views_table['Article Name']).to_dict()
          thirty_day_size_count += pandas.Series(1, index=main_views_table['Article Name']).to_dict()
          thirty_day_view_count_talk += pandas.Series(combined_table['view_count_talk'].values, index=combined_table['Article Name']).to_dict()
          thirty_day_edit_count_talk += pandas.Series(combined_table['edits_talk'].values, index=combined_table['Article Name']).to_dict()
          thirty_day_minor_edit_count_talk += pandas.Series(combined_table['minor_edits_talk'].values, index=combined_table['Article Name']).to_dict()
          thirty_day_size_total_talk += pandas.Series(talk_views_table['size_talk'].values, index=talk_views_table['Article Name']).to_dict()
          thirty_day_size_count_talk += pandas.Series(1, index=talk_views_table['Article Name']).to_dict()
        elif curr_date - history_date > three_days:
          seven_day_view_count += pandas.Series(combined_table['view_count'].values, index=combined_table['Article Name']).to_dict()
          seven_day_edit_count += pandas.Series(combined_table['edits'].values, index=combined_table['Article Name']).to_dict()
          seven_day_minor_edit_count += pandas.Series(combined_table['minor_edits'].values, index=combined_table['Article Name']).to_dict()
          seven_day_size_total += pandas.Series(main_views_table['size'].values, index=main_views_table['Article Name']).to_dict()
          seven_day_size_count += pandas.Series(1, index=main_views_table['Article Name']).to_dict()
          seven_day_view_count_talk += pandas.Series(combined_table['view_count_talk'].values, index=combined_table['Article Name']).to_dict()
          seven_day_edit_count_talk += pandas.Series(combined_table['edits_talk'].values, index=combined_table['Article Name']).to_dict()
          seven_day_minor_edit_count_talk += pandas.Series(combined_table['minor_edits_talk'].values, index=combined_table['Article Name']).to_dict()
          seven_day_size_total_talk += pandas.Series(talk_views_table['size_talk'].values, index=talk_views_table['Article Name']).to_dict()
          seven_day_size_count_talk += pandas.Series(1, index=talk_views_table['Article Name']).to_dict()
        elif curr_date - history_date > one_day:
          three_day_view_count += pandas.Series(combined_table['view_count'].values, index=combined_table['Article Name']).to_dict()
          three_day_edit_count += pandas.Series(combined_table['edits'].values, index=combined_table['Article Name']).to_dict()
          three_day_minor_edit_count += pandas.Series(combined_table['minor_edits'].values, index=combined_table['Article Name']).to_dict()
          three_day_size_total += pandas.Series(main_views_table['size'].values, index=main_views_table['Article Name']).to_dict()
          three_day_size_count += pandas.Series(1, index=main_views_table['Article Name']).to_dict()
          three_day_view_count_talk += pandas.Series(combined_table['view_count_talk'].values, index=combined_table['Article Name']).to_dict()
          three_day_edit_count_talk += pandas.Series(combined_table['edits_talk'].values, index=combined_table['Article Name']).to_dict()
          three_day_minor_edit_count_talk += pandas.Series(combined_table['minor_edits_talk'].values, index=combined_table['Article Name']).to_dict()
          three_day_size_total_talk += pandas.Series(talk_views_table['size_talk'].values, index=talk_views_table['Article Name']).to_dict()
          three_day_size_count_talk += pandas.Series(1, index=talk_views_table['Article Name']).to_dict()
        else:
          one_day_view_count += pandas.Series(combined_table['view_count'].values, index=combined_table['Article Name']).to_dict()
          one_day_edit_count += pandas.Series(combined_table['edits'].values, index=combined_table['Article Name']).to_dict()
          one_day_minor_edit_count += pandas.Series(combined_table['minor_edits'].values, index=combined_table['Article Name']).to_dict()
          one_day_size_total += pandas.Series(main_views_table['size'].values, index=main_views_table['Article Name']).to_dict()
          one_day_size_count += pandas.Series(1, index=main_views_table['Article Name']).to_dict()
          one_day_view_count_talk += pandas.Series(combined_table['view_count_talk'].values, index=combined_table['Article Name']).to_dict()
          one_day_edit_count_talk += pandas.Series(combined_table['edits_talk'].values, index=combined_table['Article Name']).to_dict()
          one_day_minor_edit_count_talk += pandas.Series(combined_table['minor_edits_talk'].values, index=combined_table['Article Name']).to_dict()
          one_day_size_total_talk += pandas.Series(talk_views_table['size_talk'].values, index=talk_views_table['Article Name']).to_dict()
          one_day_size_count_talk += pandas.Series(1, index=talk_views_table['Article Name']).to_dict()

        avg_size_total += pandas.Series(main_views_table['size'].values, index=main_views_table['Article Name']).to_dict()
        avg_size_count += pandas.Series(1, index=main_views_table['Article Name']).to_dict()
        avg_size_total_talk += pandas.Series(talk_views_table['size_talk'].values, index=talk_views_table['Article Name']).to_dict()
        avg_size_count_talk += pandas.Series(1, index=talk_views_table['Article Name']).to_dict()

        # There was a bug in the aggreagtion script. For now, we'll grab the average size over the
        # last day the article was viewed instead of the last size on the last day.
        most_recent_size.update(pandas.Series(main_views_table['size'].values, index=main_views_table['Article Name']).to_dict())
        most_recent_size_talk.update(pandas.Series(talk_views_table['size_talk'].values, index=talk_views_table['Article Name']).to_dict())

        history_date += one_day

      combined_file_name = 'data/intermediate_data/combined-{}.csv'.format(curr_date.strftime("%Y%m%d"))
      combined_table = pandas.read_csv(combined_file_name)
      num_edits = Counter(pandas.Series(combined_table['edits'].values, index=combined_table['Article Name']).to_dict())

      ts_print('Writing final data to file for date {}'.format(
        curr_date.strftime("%Y%m%d")),
        start_time,
        chunk_i + 1)
      output_file_name = 'data/aggregate_data/aggregate-{}.csv'.format(curr_date.strftime("%Y%m%d"))
      create_dir(output_file_name)
      
      if chunk_i == 0:
        # Only write the CSV header if this is the first chunk.
        with open(output_file_name, 'w') as f:
          writer = csv.writer(f)
          writer.writerow([
            'article_name', 'num_edits',
            'views_30d', 'views_7d', 'views_3d', 'views_1d',
            'edits_30d', 'edits_7d', 'edits_3d', 'edits_1d',
            'minor_edits_30d', 'minor_edits_7d', 'minor_edits_3d', 'minor_edits_1d',
            'avg_size_30d', 'avg_size_7d', 'avg_size_3d', 'avg_size_1d',
            'avg_size', 'latest_size',
            'talk_views_30d', 'talk_views_7d', 'talk_views_3d', 'talk_views_1d',
            'talk_edits_30d', 'talk_edits_7d', 'talk_edits_3d', 'talk_edits_1d',
            'talk_minor_edits_30d', 'talk_minor_edits_7d', 'talk_minor_edits_3d', 'talk_minor_edits_1d',
            'talk_avg_size_30d', 'talk_avg_size_7d', 'talk_avg_size_3d', 'talk_avg_size_1d',
            'talk_avg_size', 'talk_latest_size'])

      # Write to a new file if this is the first chunk, otherwise append.
      with open(output_file_name, 'a') as f:
        writer = csv.writer(f)
        for article in articles:
          writer.writerow([
            article,
            num_edits[article],
            thirty_day_view_count[article], seven_day_view_count[article], three_day_view_count[article], one_day_view_count[article],
            thirty_day_edit_count[article], seven_day_edit_count[article], three_day_edit_count[article], one_day_edit_count[article],
            thirty_day_minor_edit_count[article], seven_day_minor_edit_count[article], thirty_day_minor_edit_count[article], one_day_minor_edit_count[article],
            thirty_day_size_total[article] / thirty_day_size_count[article] if thirty_day_size_count[article] else numpy.nan,
            seven_day_size_total[article] / seven_day_size_count[article] if seven_day_size_count[article] else numpy.nan,
            three_day_size_total[article] / three_day_size_count[article] if three_day_size_count[article] else numpy.nan,
            one_day_size_total[article] / one_day_size_count[article] if one_day_size_count[article] else numpy.nan,
            avg_size_total[article] / avg_size_count[article] if avg_size_count[article] else numpy.nan,
            most_recent_size[article] if article in most_recent_size else numpy.nan,
            thirty_day_view_count_talk[article], seven_day_view_count_talk[article], three_day_view_count_talk[article], one_day_view_count_talk[article],
            thirty_day_edit_count_talk[article], seven_day_edit_count_talk[article], three_day_edit_count_talk[article], one_day_edit_count_talk[article],
            thirty_day_minor_edit_count_talk[article], seven_day_minor_edit_count_talk[article], three_day_minor_edit_count_talk[article], one_day_minor_edit_count_talk[article],
            thirty_day_size_total_talk[article] / thirty_day_size_count_talk[article] if thirty_day_size_count_talk[article] else numpy.nan,
            seven_day_size_total_talk[article] / seven_day_size_count_talk[article] if seven_day_size_count_talk[article] else numpy.nan,
            three_day_size_total_talk[article] / three_day_size_count_talk[article] if three_day_size_count_talk[article] else numpy.nan,
            one_day_size_total_talk[article] / one_day_size_count_talk[article] if one_day_size_count_talk[article] else numpy.nan,
            avg_size_total_talk[article] / avg_size_count_talk[article] if avg_size_count_talk[article] else numpy.nan,
            most_recent_size_talk[article] if article in most_recent_size_talk else numpy.nan,
            ])

      ts_print('Wrote final data to file for date {}'.format(
        curr_date.strftime("%Y%m%d")),
        start_time,
        chunk_i + 1)

    curr_date += one_day
    

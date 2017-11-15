import argparse
import datetime
from hashlib import md5
import os
import pandas
import sys
import time
import urllib

import agg_page_counts
from agg_page_counts import ts_print


# Data from running queries like below on https://quarry.wmflabs.org
# SELECT page_namespace, page_title, date, edits, minor_edits
# FROM (
#   SELECT rev_page,
#        LEFT(rev_timestamp, 8) as date,
#          COUNT(*) AS edits,
#            SUM(rev_minor_edit) as minor_edits
#     FROM revision
#     WHERE rev_timestamp BETWEEN "20160401" AND "20160402"
#     GROUP BY rev_page, date
# ) as page_edits
# JOIN page ON rev_page = page_id
# WHERE page_namespace IN (0, 1)

# Extract article name from (NAMESPACE:?)ARTICLE string
def extract_article_name(s):
  return s.split(':')[-1]

# Extract namespace id from (NAMESPACE:?)ARTICLE string when present.
# 0 is the main namespace, 1 the talk namespace. Others are currently grouped into 999.
def extract_namespace(s):
  if ':' not in s:
    return 0
  namespace = s.split(':')[0]
  if namespace == 'Talk':
    return 1
  return 999

def consistent_hash(s):
  return md5(str.encode(s)).hexdigest()

# Given a string in YYYYMMDD format, return a datetime object of that date.
def date_arg(s):
  return datetime.datetime.strptime(s, '%Y%m%d')

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
  start_date = args.start_date#datetime.date(2016, 4, 1)
  end_date = args.end_date
  curr_date = start_date

  while curr_date < end_date:
    date_str = curr_date.strftime("%Y%m%d")
    view_counts_file_name = 'data/raw_data/pageviews/pageviews-{}.csv'.format(date_str)
    # Download pageview data if necessary
    if not os.path.exists(view_counts_file_name):
      ts_print("{}: Downloading view counts".format(date_str), start_time)
      agg_page_counts.run(curr_date, start_time)
    ts_print("{}: Reading view counts".format(date_str), start_time)
    view_counts = pandas.read_table(view_counts_file_name, sep=',', keep_default_na=False, na_values=[''])
    ts_print("{}: Reading revision data".format(date_str), start_time)
    # Update paths to acutal data :)
    revisions_file_name = 'data/raw_data/edits/edits-{}.csv'.format(date_str)
    revisions = pandas.read_table(revisions_file_name, sep=',', keep_default_na=False, na_values=[''])

    ts_print("{}: Cleaning up columns".format(date_str), start_time)
    revisions['page_title'] = revisions['page_title'].apply(urllib.parse.quote)
    view_counts['Article Namespace'] = view_counts['Article Name'].apply(extract_namespace)
    # Remove all non-main/talk namespaces
    view_counts = view_counts[view_counts['Article Namespace'] != 999]
    view_counts['Article Name'] = view_counts['Article Name'].apply(extract_article_name)


    ts_print("{}: Merging tables".format(date_str), start_time)
    merged_ns_split = pandas.merge(
      view_counts,
      revisions,
      # Keep views even if they weren't editted
      how='left',
      left_on=['Article Name', 'Article Namespace'],
      right_on=['page_title', 'page_namespace'],
      suffixes=('_l', '_r'))

    # Drop redundant info that's the primary key for pageview and revision tables.
    del merged_ns_split['page_title']
    del merged_ns_split['page_namespace']

    merged_main = merged_ns_split[merged_ns_split['Article Namespace'] == 0]
    merged_talk = merged_ns_split[merged_ns_split['Article Namespace'] == 1]

    merged = pandas.merge(
      merged_main,
      merged_talk,
      how='outer',
      left_on=['Article Name'],
      right_on=['Article Name'],
      suffixes=('', '_talk'))

    del merged['Article Namespace_talk']
    del merged['date_talk']

    ## double commented lines can be uncommented to get data only on edited files.
    ## TODO: Delete this, move to another file, or add a parameter
    ## edited_ns_split = pandas.merge(
    ##   view_counts,
    ##   revisions,
    ##   how='inner',
    ##   left_on=['Article Name', 'Article Namespace'],
    ##   right_on=['page_title', 'page_namespace'],
    ##   suffixes=('_l', '_r'))
    ## edited_main = edited_ns_split[edited_ns_split['Article Namespace'] == 0]
    ## edited_talk = edited_ns_split[edited_ns_split['Article Namespace'] == 1]
    ## edited = pandas.merge(
    ##   edited_main,
    ##   edited_talk,
    ##   how='outer',
    ##   left_on=['Article Name'],
    ##   right_on=['Article Name'],
    ##   suffixes=('', '_talk'))

    ts_print("{}: Table clean-up".format(date_str), start_time)
    merged[['edits', 'minor_edits']] = merged[['edits', 'minor_edits']].fillna(value=0)
    merged[['date']] = merged[['date']].fillna(value=date_str)

    ts_print("{}: Writing table".format(date_str), start_time)
    if not os.path.exists('data/intermediate_data'):
      os.makedirs('data/intermediate_data')
    merged.to_csv('data/intermediate_data/combined-{}.csv'.format(date_str))
    # Grab roughly 3% (1/32) of articles arbitrarily but consistently
    merged_sample = merged[merged['Article Name'].apply(consistent_hash) <= '08']
    merged_sample.to_csv('data/intermediate_data/combined-sample-{}.csv'.format(date_str))
    ## edited.to_csv('data/intermediate_data/combined-{}-edits-only.csv'.format(date_str))

    # Delete viewcounts files as they're large (~3GB each), and we don't want to assume
    # 90GB+60GB free to run this on 1 month's worth of data :)
    os.remove(view_counts_file_name)
    curr_date += one_day
 

import datetime
import pandas
import urllib

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

def extract_article_name(s):
  return s.split(':')[-1]

def extract_namespace(s):
  if ':' not in s:
    return 0
  namespace = s.split(':')[0]
  if namespace == 'Talk':
    return 1
  return 999

if __name__ == '__main__':
  one_day = datetime.timedelta(days=1)
  start_date = datetime.date(2016, 4, 1)
  end_date = datetime.date(2016, 4, 2)
  curr_date = start_date

  while curr_date < end_date:
    date_str = curr_date.strftime("%Y%m%d")
    # Update paths to acutal data :)
    print("{}: Reading view counts".format(date_str))
    view_counts_file_name = '../../Wiki/pageviews-{}.csv'.format(date_str)
    view_counts = pandas.read_table(view_counts_file_name, sep=',', keep_default_na=False, na_values=[''])
    print("{}: Reading revision data".format(date_str))
    # Update paths to acutal data :)
    revisions_file_name = '../../Wiki/edits-{}.csv'.format(date_str)
    revisions = pandas.read_table(revisions_file_name, sep=',', keep_default_na=False, na_values=[''])

    print("{}: Cleaning up columns".format(date_str))
    # TODO: Update revisions 'page_title' to transform things like Anne_Brontë into Anne_Bront%C3%AB
    revisions['page_title'] = revisions['page_title'].apply(urllib.parse.quote)
    view_counts['Article Namespace'] = view_counts['Article Name'].apply(extract_namespace)
    # Remove all non-main/talk namespaces
    view_counts = view_counts[view_counts['Article Namespace'] != 999]
    view_counts['Article Name'] = view_counts['Article Name'].apply(extract_article_name)


    print("{}: Merging tables".format(date_str))
    merged_ns_split = pandas.merge(
      view_counts,
      revisions,
      # Keep views even if they weren't editted
      how='left',
      left_on=['Article Name', 'Article Namespace'],
      right_on=['page_title', 'page_namespace'],
      suffixes=('_l', '_r'))

    # Drop redundant info (since it's merged in)
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

    ## double commented lines can be uncommented to get sample data.
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

    print("{}: Table clean-up".format(date_str))
    merged[['edits', 'minor_edits']] = merged[['edits', 'minor_edits']].fillna(value=0)
    merged[['date']] = merged[['date']].fillna(value=date_str)

    print("{}: Writing table".format(date_str))
    merged.to_csv('../../Wiki/combined-{}.csv'.format(date_str))
    ##
    ## merged_sample = merged.sample(frac=0.003, replace=False)
    ## merged_sample.to_csv('../../Wiki/combined-sample-{}.csv'.format(date_str))
    ## edited.to_csv('../../Wiki/combined-{}-edits-only.csv'.format(date_str))

    curr_date += one_day
 

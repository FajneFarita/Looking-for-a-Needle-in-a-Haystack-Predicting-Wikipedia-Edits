import datetime
import pandas
import urllib

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
    view_counts = pandas.read_table(view_counts_file_name, sep=',')
    print("{}: Reading revision data".format(date_str))
    # Update paths to acutal data :)
    revisions_file_name = '../../Wiki/edits-{}.csv'.format(date_str)
    revisions = pandas.read_table(revisions_file_name, sep=',')

    # TODO: Update revisions 'page_title' to transform things like Anne_Brontë into Anne_Bront%C3%AB
    revisions['page_title'] = revision['page_title'].apply(urllib.parse.quote)

    print("{}: Merging tables".format(date_str))
    merged = pandas.merge(
      view_counts,
      revisions,
      # Keep views even if they weren't editted
      how='left',
      left_on=['Article Name'],
      right_on=['page_title'],
      suffixes=('l', 'r'))

    ## double commented lines can be uncommented to get sample data.
    ## TODO: Delete this, move to another file, or add a parameter
    ## edited = pandas.merge(
    ##   view_counts,
    ##   revisions,
    ##   how='inner',
    ##   left_on=['Article Name'],
    ##   right_on=['page_title'],
    ##   suffixes=('l', 'r'))

    print("{}: Table clean-up".format(date_str))
    merged[['edits', 'minor_edits']] = merged[['edits', 'minor_edits']].fillna(value=0)
    merged[['date']] = merged[['date']].fillna(value=date_str)
    del merged['page_len']
    del merged['page_counter']
    ## del edited['page_len']
    ## del edited['page_counter']

    print("{}: Writing table".format(date_str))
    merged.to_csv('../../Wiki/combined-{}.csv'.format(date_str))
    ## merged_sample = merged.sample(frac=0.003, replace=False)
    ## merged_sample_.to_csv('../../Wiki/combined-sample-{}.csv'.format(date_str))
    ## edited.to_csv('../../Wiki/combined-{}-edits-only.csv'.format(date_str))

    curr_date += one_day
 

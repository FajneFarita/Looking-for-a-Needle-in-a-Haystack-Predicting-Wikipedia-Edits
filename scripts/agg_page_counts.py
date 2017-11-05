from collections import defaultdict
import csv
import datetime
import gzip
import random #Remove

# Data from https://dumps.wikimedia.org/other/pagecounts-raw/

if __name__ == '__main__':

  one_day = datetime.timedelta(days=1)
  start_date = datetime.date(2016, 4, 1)
  end_date = datetime.date(2016, 4, 2)
  curr_date = start_date
 
  # For every day from start_date to end_date (excluding end_date)
  while curr_date < end_date:
    # Keep track of view counts per article on that day.
    view_counts = defaultdict(int)
    # Keep track of view article sized on that day.
    sizes = defaultdict(list)

    for hour in range(24):
      # Helper variable to print status updates while running.
      num_articles = 0

      input_file_name = 'pagecounts-{}-{:02d}0000.gz'.format(
        curr_date.strftime("%Y%m%d"), hour)
      with gzip.open(input_file_name, 'rb') as f:
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
              print("Failed to read line: {}".format(line.decode().split()))
              continue

            # Keep data to record total view counts and min/max/avg article size.
            view_counts[article] += int(view_count)
            sizes[article].append(int(byte_size))

            # Randomly print status updates to ensure correct running.
            if random.randint(1, 100000) == 5:
              print('Processing hour {}, row #{}'.format(hour, num_articles))
          elif seen_en_rows == True:
            # Done reading `en` stats for this file, skip the rest.
            break
      
    # Write results for the day. 
    output_file_name = 'pageviews-{}.csv'.format(curr_date.strftime("%Y%m%d"))
    with open(output_file_name, 'w') as f:
      writer = csv.writer(f)
      writer.writerow(['Article Name', 'view_count', 'size', 'min_size', 'max_size'])
      for article in view_counts:
        # Grab the min/max/avg article size over the day.
        min_size = min(sizes[article])
        max_size = max(sizes[article]) 
        avg_size = int(sum(sizes[article]) / len(sizes[article]))
        writer.writerow([article, view_counts[article], avg_size, min_size, max_size])
    
    curr_date += one_day

  print("Complete!")

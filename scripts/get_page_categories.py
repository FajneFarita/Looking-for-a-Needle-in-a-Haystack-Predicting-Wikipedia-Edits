import csv
import json
import os
import random
import sys
import urllib.request
import urllib.parse

# 4:19 start
# 
categories = ['Arts', 'Culture', 'Events', 'Geography', 'Health', 'History',
  'Humanities', 'Law', 'Life', 'Mathematics', 'Matter', 'Nature', 'People',
  'Philosophy', 'Politics', 'Reference Works', 'Religion',
  'Science and technology', 'Society', 'Sports', 'World']
seen_cats = set()
pages_to_category = set()

def create_dir(f):
  # Create the appropriate directory if necessary.
  if not os.path.exists(os.path.dirname(f)):
    os.makedirs(os.path.dirname(f))

def recurse_category_members(orig_cat, curr_cat, trail):
  if curr_cat in seen_cats:
    return
  seen_cats.add(curr_cat)
  url = 'https://en.wikipedia.org/w/api.php?action=query&format=json&list=categorymembers&cmtitle=Category%3A{}&cmlimit=max'.format(
    urllib.parse.quote_plus(curr_cat))
  if random.randint(1, 10) == 5:
    print(orig_cat, ':', curr_cat, '({})'.format(len(pages_to_category)), trail)
  req = urllib.request.Request(url, headers={'User-Agent': 'nathaniel_weinman@berkeley.edu'})
  with urllib.request.urlopen(req) as response:
    json_res = response.read()
    members = json.loads(json_res)['query']['categorymembers']
    if len(members) >= 500:
      print(curr_cat, len(members))
    for member in members:
      if member['ns'] == 0:
        pages_to_category.add(member['title'])
      elif member['ns'] == 14:
        recurse_category_members(orig_cat, member['title'].split(':')[-1], trail + [curr_cat])

if __name__ == '__main__':
  if os.getcwd().split('/')[-1] != 'info-251-fall-2017-final-project':
    print('Please run from top-level folder of git project, `info-251-fall-2017-final-project`')
    sys.exit()


  output_file_name = 'data/raw_data/category_map.csv'
  create_dir(output_file_name)
  with open(output_file_name, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['article_name', 'top_level_category'])

  for category in categories:
    recurse_category_members(category, category, [])

    with open(output_file_name, 'a') as f:
      writer = csv.writer(f)
      for article in pages_to_category:
        writer.writerow([article, category])
    pages_to_category = set()

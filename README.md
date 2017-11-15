# info-251-fall-2017-final-project
## Downloading data
1) Unzip `data/raw_data.edits.zip`
2) From the top level directory, run `python
scripts/merge_page_counts_and_revisions.py -s 20160401 -e 20160501`
  Note: You can split this command by running it multiple times and updating the
    start and end dates. Depending on your internet connection, each date takes
    roughly 45-75 minutes to process (or perhaps even shorter or longer).


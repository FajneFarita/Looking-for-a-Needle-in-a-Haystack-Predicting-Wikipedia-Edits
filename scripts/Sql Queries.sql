-- Run on Quarry: https://quarry.wmflabs.org

-- To filter out any pages with restrictions (TBD).
SELECT page_namespace, page_title
FROM page
WHERE page_namespace IN (0, 1) and page_restrictions != ''

-- To get edit data for a given date.
SELECT page_namespace, page_title, date, edits, minor_edits
FROM (
  SELECT rev_page,
       LEFT(rev_timestamp, 8) as date,
         COUNT(*) AS edits,
           SUM(rev_minor_edit) as minor_edits
    FROM revision
    WHERE rev_timestamp BETWEEN "20160401" AND "20160402"
    GROUP BY rev_page, date
) as page_edits
JOIN page ON rev_page = page_id
WHERE page_namespace IN (0, 1)

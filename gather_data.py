#!/usr/bin/env python
# coding: utf-8

# This notebook gathers all extra data required by https://phabricator.wikimedia.org/T286562 and writes it to a parquet file

import re

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Pass in the full snapshot date

# In[2]:


snapshot = '2021-08-23'
reg = r'^([\w]+-[\w]+)'
short_snapshot = re.match(reg, snapshot).group()


# We use wmfdata boilerplate to init a spark session.
# Under the hood the library uses findspark to initialise
# Spark's environment. pyspark imports will be available
# after initialisation

# In[3]:


spark = get_session(type='regular', app_name="T286562")
import pyspark
import pyspark.sql


# Images containing these substrings are probably icons or placeholders

# In[4]:


disallowed_substrings = ['flag','noantimage','no_free_image','image_manquante',
    'replace_this_image','disambig','regions','map','default',
    'defaut','falta_imagem_','imageNA','noimage','noenzyimage']

disallowed_sql = []
for d in disallowed_substrings:
    disallowed_sql.append("p.page_title like '%" + d + "%'")

query="""
SELECT p.page_id,p.page_title
FROM wmf_raw.mediawiki_page p
WHERE p.page_namespace=6
AND page_is_redirect=0
AND p.wiki_db='commonswiki'
AND p.snapshot='"""+short_snapshot+"""'
AND (""" + " OR ".join(disallowed_sql) + """)
ORDER BY page_id
"""

disallowedDF = spark.sql(query)
disallowedDF.createOrReplaceTempView("files_with_disallowed_substrings")


# An image linked to from more than N pages is likely to be an icon or a placeholder, where the value of N depends on the size of the wiki

# In[5]:


query="""WITH wiki_sizes as (
    SELECT wiki_db,COUNT(*) as size,
    IF (
        COUNT(*) >= 50000,
        CEILING((log10(COUNT(*)/50000)+1)*15),
        CEILING(
            IF(
                ((COUNT(*)/50000 * 15) > 15/4),
                COUNT(*)/50000 * 15,
                15/4
            )
        )
    ) as threshold
    FROM wmf_raw.mediawiki_page
    WHERE page_namespace=0
    AND page_is_redirect=0
    AND snapshot='"""+short_snapshot+"""'
    GROUP BY wiki_db
),
commons_file_pages as
 (
 SELECT p.page_id,p.page_title
 FROM wmf_raw.mediawiki_page p
 WHERE p.page_namespace=6
 AND page_is_redirect=0 AND p.wiki_db='commonswiki'
 AND p.snapshot='"""+short_snapshot+"""'
 ORDER BY page_id
 )
SELECT cfp.page_id,cfp.page_title
FROM wmf_raw.mediawiki_imagelinks il
JOIN wmf_raw.mediawiki_page p
ON (p.page_id=il.il_from and p.wiki_db=il.wiki_db)
JOIN commons_file_pages cfp
ON cfp.page_title=il.il_to
JOIN wiki_sizes ws
ON ws.wiki_db=p.wiki_db
WHERE il.il_from_namespace=0
AND p.snapshot='"""+short_snapshot+"""'
AND il.snapshot='"""+short_snapshot+"""'
GROUP BY ws.threshold,cfp.page_id,cfp.page_title
HAVING COUNT(il.il_to)>ws.threshold"""
disallowedDF = spark.sql(query)
disallowedDF.createOrReplaceTempView("files_with_too_many_linkages")


# We also have a parquet containing `page_title` for commons files that are in placeholder categories (see https://petscan.wmflabs.org/?psid=18699732&format=json).
# Load those into another temp view

# In[6]:


placeholder_images = spark.read.parquet('hdfs:/user/gmodena/image_placeholders')
placeholder_images.createOrReplaceTempView('files_in_placeholder_categories')


# Gather all commons files that aren't placeholders or icons, with relevant linked wikidata items for each
#
# `commons_file_pages_with_reverse_p18` has every allowed commons File page that is linked to from wikidata via the P18 (image) property.
# Each row contains
# - the page_id of the commons page
# - a set of the wikidata ids that link to the commons page via P18 i.e. the Qid of every wikidata item that has statement 'P18=<the title of the commons page>'
#
# `commons_file_pages_with_reverse_p373` has every allowed commons File page that is contained in a commons category where
# - the commons category is linked to a wikidata item via the P373 (commons category) property AND
# - the wikidata item corresponds to a page in the main namespace in any non-commons wiki AND
# - the category on commons contains fewer than 100k pages
# Each row contains
# - the page_id of the commons page
# - a set of the wikidata ids that link to the commons category the commons page is in, concatenated with the number of pages in the category
#     i.e.
#     - the Qid of every wikidata item linked to a main-namespace page that has statement 'P373=<the title of a commons category the commons page is in>'
#     - then the pipe symbol
#     - then the number of pages in the category
#
# `commons_file_pages_with_container_page_qids` has every allowed commons File page that is used on a main-namespace page on a non-commons wiki.
# Each row contains
# - the page_id of the commons page
# - a set of the wikidata ids of pages that contain the commons image, concatenated with the wiki_db of the page, concatenated with the number of links to the page
#     i.e.
#     - the Qid of every wikidata item linked to a main-namespace page that includes the commons File page
#     - then the pipe symbol
#     - then the wiki the-page-that-includes-the-commons-File-page in on
#     - then the pipe symbol
#     - then the number of pagelinks to the-page-that-includes-the-commons-File-page
#
# `commons_file_pages` contains every File page on commons *excluding* those disallowed above
#
# `non_commons_main_pages` contains every main-namespace page on every wiki EXCEPT commons
#
# `qid_props` contains every wikidata item with its P18 (image) and P373 (commons category) values

# In[ ]:


query="""WITH commons_file_pages as
 (
 SELECT p.page_id,p.page_title
 FROM wmf_raw.mediawiki_page p
 LEFT ANTI JOIN files_with_disallowed_substrings dsub
 ON dsub.page_id=p.page_id
 LEFT ANTI JOIN files_with_too_many_linkages fml
 ON fml.page_id=p.page_id
 LEFT ANTI JOIN files_in_placeholder_categories fpc
 ON fpc.page_title=p.page_title
 WHERE p.page_namespace=6
 AND page_is_redirect=0 AND p.wiki_db='commonswiki'
 AND p.snapshot='"""+short_snapshot+"""'
 ORDER BY page_id
 ),
 non_commons_main_pages as
 (
 SELECT p.page_id,p.wiki_db,p.page_title,count(pl.pl_from) as incoming_links
 FROM wmf_raw.mediawiki_page p
 JOIN wmf_raw.mediawiki_pagelinks pl
 ON (pl.pl_title=p.page_title AND pl.wiki_db=p.wiki_db)
 WHERE p.page_namespace=0
 AND page_is_redirect=0
 AND p.wiki_db!='commonswiki'
 AND p.snapshot='"""+short_snapshot+"""'
 AND pl.snapshot='"""+short_snapshot+"""'
 GROUP BY p.wiki_db,p.page_id,p.page_title
 ORDER BY p.page_id
 ),
 qid_props AS
 (
 SELECT we.id as item_id,
 MAX(CASE WHEN claim.mainSnak.property = 'P18' THEN claim.mainSnak.datavalue.value ELSE NULL END) AS hasimage,
 MAX(CASE WHEN claim.mainSnak.property = 'P373' THEN REPLACE(claim.mainSnak.datavalue.value,' ','_') ELSE NULL END) AS commonscategory
 FROM wmf.wikidata_entity we
 LATERAL VIEW OUTER explode(claims) c AS claim
 WHERE typ='item'
 AND snapshot='"""+snapshot+"""'
 AND claim.mainSnak.property in ('P18','P373')
 GROUP BY item_id
 ),
 commons_file_pages_with_reverse_p18 AS
 (
 SELECT p.page_id,collect_set(qid_props.item_id) as reverse_p18
 FROM commons_file_pages p
 JOIN qid_props on concat('"',p.page_title,'"')=qid_props.hasimage
 GROUP BY p.page_id
 ),
 commons_file_pages_with_reverse_p373 AS
 (
 SELECT cl.cl_from as page_id,collect_set(concat_ws('|',qid_props.item_id,c.cat_pages)) as reverse_p373
 FROM wmf_raw.mediawiki_categorylinks cl
 JOIN wmf_raw.mediawiki_category c
 ON (c.cat_title=cl.cl_to AND c.wiki_db='commonswiki' AND c.cat_pages<100000)
 JOIN qid_props
 ON qid_props.commonscategory=concat('"',cl.cl_to,'"')
 JOIN wmf.wikidata_item_page_link wipl
 ON wipl.item_id=qid_props.item_id
 WHERE cl.snapshot='"""+short_snapshot+"""'
 AND c.snapshot='"""+short_snapshot+"""'
 AND cl.wiki_db ='commonswiki'
 AND cl.cl_type='file'
 AND wipl.page_namespace=0
 AND wipl.wiki_db!='commonswiki'
 group by cl.cl_from
 ),
 commons_file_pages_with_container_page_qids AS
 (
 SELECT cfp.page_id,collect_set(concat_ws('|',wipl.item_id,ncmp.wiki_db,ncmp.incoming_links)) as container_page_qids
 FROM non_commons_main_pages ncmp
 JOIN wmf.wikidata_item_page_link wipl
 ON wipl.page_id=ncmp.page_id
 AND wipl.wiki_db=ncmp.wiki_db
 JOIN wmf_raw.mediawiki_page_props pp
 ON ncmp.page_id=pp.pp_page
 AND ncmp.wiki_db=pp.wiki_db
 AND pp.pp_propname in ('page_image','page_image_free')
 JOIN commons_file_pages cfp
 ON cfp.page_title=pp.pp_value
 WHERE wipl.snapshot='"""+snapshot+"""'
 AND pp.snapshot='"""+short_snapshot+"""'
 GROUP BY cfp.page_id
 )
 select cfp.page_id,reverse_p18,reverse_p373,container_page_qids
 FROM commons_file_pages cfp
 LEFT JOIN commons_file_pages_with_reverse_p18 rp18
 ON cfp.page_id=rp18.page_id
 LEFT JOIN commons_file_pages_with_reverse_p373 rp373
 ON cfp.page_id=rp373.page_id
 LEFT JOIN commons_file_pages_with_container_page_qids cpq
 ON cfp.page_id=cpq.page_id
 WHERE
 (reverse_p18 IS NOT NULL OR reverse_p373 IS NOT NULL or container_page_qids IS NOT NULL)
 GROUP BY cfp.page_id,reverse_p18,reverse_p373,container_page_qids
 """
commonsDF = spark.sql(query)


# ## [T299408](https://phabricator.wikimedia.org/T299408)
#
# ### Step 1
#
# Gather lead image Wikidata IDs as per [T299408](https://phabricator.wikimedia.org/T299408), first 3 bullet points.
# - Input = `commonsDF` from the previous step
# - Output = `lead_images` Spark data frame. Fields:
#   - `commons_id` -  a Commons page ID
#   - `qid` - a Wikidata QID of wiki pages having the Commons page ID as the lead image
#   - `pages_with_lead_image` - the amount of those wiki pages

# In[ ]:


commonsDF.createOrReplaceTempView('commonsDF')

query = f"""
WITH
commons AS (
    SELECT commonsDF.page_id, p.page_title
    FROM wmf_raw.mediawiki_page AS p
    JOIN commonsDF ON commonsDF.page_id=p.page_id
    WHERE p.page_namespace=6 AND p.page_is_redirect=0 AND p.wiki_db='commonswiki' AND p.snapshot='{short_snapshot}'
),
page AS (
    SELECT commons.page_id, pp.wiki_db, pp.pp_page
    FROM wmf_raw.mediawiki_page_props AS pp
    JOIN commons ON commons.page_title=pp.pp_value
    WHERE pp.snapshot='{short_snapshot}' AND pp.pp_propname IN ('page_image_free', 'page_image')
)
SELECT page.page_id AS commons_id, pp.pp_value AS qid, COUNT(pp.pp_page) AS pages_with_lead_image
FROM wmf_raw.mediawiki_page_props AS pp
JOIN page ON page.wiki_db=pp.wiki_db AND page.pp_page=pp.pp_page
WHERE pp.snapshot='{short_snapshot}' AND pp.pp_propname='wikibase_item'
GROUP BY commons_id, qid
ORDER BY commons_id
"""

lead_images = spark.sql(query)


# ### Step 2
#
# Gather the amount of incoming links for each QID as above.
# - Input = `lead_images` from the previous step
# - Output = `counts` Spark data frame. Same fields as `lead_images`, plus `incoming_links` - the amount of incoming links per QID
#
# The `wmf_raw.mediawiki_pagelinks` table doesn't seem to hold IDs for target pages: it has titles instead.
# Therefore, we need to retrieve titles from `wmf_raw.mediawiki_page`, see `title` in the `WITH` statement below.

# In[ ]:


lead_images.createOrReplaceTempView('lead_images')

query = f"""
WITH
pwq AS (
    SELECT lead_images.commons_id, lead_images.qid, lead_images.pages_with_lead_image, pp.pp_page, pp.wiki_db
    FROM wmf_raw.mediawiki_page_props AS pp
    JOIN lead_images ON lead_images.qid=pp.pp_value
    WHERE pp.snapshot='{short_snapshot}' AND pp.pp_propname='wikibase_item'
),
title AS (
    SELECT pwq.commons_id, pwq.qid, pwq.pages_with_lead_image, p.page_title, p.wiki_db
    FROM wmf_raw.mediawiki_page AS p
    JOIN pwq ON pwq.wiki_db=p.wiki_db AND pwq.pp_page=p.page_id
    WHERE p.snapshot='{short_snapshot}'
)
SELECT title.commons_id, title.qid, title.pages_with_lead_image, COUNT(pl.pl_from) AS incoming_links
FROM wmf_raw.mediawiki_pagelinks AS pl
JOIN title ON title.wiki_db=pl.wiki_db AND title.page_title=pl.pl_title
WHERE pl.snapshot='{short_snapshot}'
GROUP BY title.commons_id, title.qid, title.pages_with_lead_image
ORDER BY title.commons_id
"""

counts = spark.sql(query)


# ### Step 3
#
# Compute the Wikidata ID score as per [T299408](https://phabricator.wikimedia.org/T299408), 4th bullet point.
# - Input = `counts` from the previous step
# - Output = `final` Spark data frame. Same fields as `commonsDF`, plus `lead_image_qids` - the array of (QID, score) pairs, separated by `|`, or an empty array in case of no QIDs
#
# The score is computed through the following proportion:
#
# > **all Wiki pages : incoming links per QID = order of magnitude of all Wiki pages : score**
#
# Using `1000` instead of the order of magnitude is likely to yield very small scores, due to a low amount of incoming links per QID.
#
# **N.B.:** this was encountered on a random sample of 360 rows from `commonsDF`, so the estimation may be biased.

# In[ ]:


counts.createOrReplaceTempView('counts')

grand_total = spark.sql(
    f"SELECT count(page_id) FROM wmf_raw.mediawiki_page WHERE snapshot='{short_snapshot}'"
).collect()[0]['count(page_id)']
order_of_magnitude = 10**len(list(str(grand_total)))
score_formula = f'round(counts.incoming_links * {order_of_magnitude} / {grand_total})'

query = f"""
SELECT page_id, reverse_p18, reverse_p373, container_page_qids,
collect_set(concat_ws('|', counts.qid, {score_formula})) AS lead_image_qids
FROM commonsDF
LEFT JOIN counts ON counts.commons_id=commonsDF.page_id
GROUP BY page_id, reverse_p18, reverse_p373, container_page_qids
ORDER BY page_id
"""

final = spark.sql(query).cache()


# write the data to a parquet so we can use it later
#
# the fields are:
# - `page_id`: the page id of the file on commons
# - `reverse_p18`: a set of wikidata item-ids from which the commons file is linked via the P18 (image) property
# - `reverse_p373`: a set of wikidata item-ids linked via P373 (commons category) to any commons category that the commons file belongs to, plus the total number of
#    files in the category
# - `container_page_qids`: a set of wikidata item-ids of wiki pages the commons file is used in, plus the wiki, plus the number of incoming links to the wiki page
# - `lead_image_qids`: a set of (QID, lead image score) pairs

# In[ ]:


final.write.parquet('commons_files_related_wikidata_items', mode='overwrite')


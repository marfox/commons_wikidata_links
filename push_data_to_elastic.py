# Wikidata items that commons files are linked from are stored in hdfs
# 
# This script reads the data from hdfs, and pushes it to the commons elasticsearch index
#
# For more info on the data, and the fields to which it is being pushed, see https://phabricator.wikimedia.org/T286562
#
# Run this script from the cmd line on one of the stat servers like this:
#     spark2-submit --driver-memory 2G --executor-memory 4G --master yarn </path/to/commonsFilesWithWikidataItems> 
#     --py-files /srv/deployment/wikimedia/discovery/analytics/spark/wmf_spark.py </path/to/this_script>

import json
import sys
import numpy as np
import requests
import logging
import pyspark
import pyspark.sql
spark = pyspark.sql.SparkSession.builder.getOrCreate()

# The script expects this file to already have been generated (using gather_data.ipynb) and pushed onto hdfs
commonsFilesWithWikidataItems = 'hdfs:/user/cparle/commons_files_related_wikidata_items'
searchIndexBulkEndpoint = 'https://relforge1003.eqiad.wmnet:9243/commonswiki_file_t286562/_bulk'

def main():
    commonsDF = spark.read.load(commonsFilesWithWikidataItems)
    count = 0
    allData = []

    for row in commonsDF.toLocalIterator():
        data1 = json.loads('{"update":{"_type":"page"}}');
        data1['update']['_id'] = str(row['page_id'])
        allData.append(json.dumps(data1))

        weightedTags = []
        if row['reverse_p18'] is not None:
            for p18 in row['reverse_p18']:
                weightedTags.append( 'image.linked.from.wikidata.p18/' + p18 )
        if row['reverse_p373'] is not None:
            for p373 in row['reverse_p373']:
                split = p373.split('|')
                # first part of field contains a wikidata item-id linked via P373 (commons category) to any commons category that the commons file belongs to
                qid = split[0]
                # second part of field contains the number of articles in the commons category
                pages_in_category = int(split[1])
                if ( pages_in_category > 0 ):
                    # we want the score of this field to be inversely proportional to the number of pages in category
                    # i.e. if an image is one of 5 in a category, then we care more about that category for searching purposes than a category with 10k images
                    # ... when calculating the score we take a log of the number of pages + 1 (adding 1 to make sure the score is positive), 
                    # and multiply by 1000/1.443 to give us a maximum score of 1000
                    pages_in_category_score = int( round( 1/np.log( pages_in_category + 1 ) * 1000/1.443) )
                    weightedTags.append( 'image.linked.from.wikidata.p373/' + qid + '|' + str( pages_in_category_score ) )
        if row['container_page_qids'] is not None:
            # we want the score of this field to be proportional to how important the page containing the image is
            # we measure importance by summing all the incoming links to pages with the relevant Q-id across all wikis
            qids_with_incoming_links = {}
            for sitelink in row['container_page_qids']:
                split = sitelink.split('|')
                # first part of field contains a wikidata item-id for a page that contains the commons file
                qid = split[0]
                # second part of field contains the wiki the page is on
                # third part of field contains the number of incoming links to the page
                incoming_link_count = int(split[2])
                if not qid in qids_with_incoming_links:
                    qids_with_incoming_links[qid] = incoming_link_count
                else:
                    qids_with_incoming_links[qid] += incoming_link_count
    
            for qid in qids_with_incoming_links:
                    # take a log to get the final score, and make sure the max score is 1000
                    score =  min( 1000, int( 100 * round( np.log( qids_with_incoming_links[qid]), 3 ) ) )
                    weightedTags.append( 'image.linked.from.wikidata.sitelink/' + qid + '|' + str( score ) )
    
        allData.append('{"doc":{"weighted_tags":' + json.dumps(weightedTags) + '}}')
        count += 1
        if ( count % 100 == 0):
            dataAsJson = "\n".join(allData) + "\n"
            response = requests.post( searchIndexBulkEndpoint, data=dataAsJson, headers={"Content-Type": "application/x-ndjson"} )
            logging.info('commons_wikidata_links: data sent to ' + str(count) + ' documents')
            logging.info('commons_wikidata_links: latest ' + str(row['page_id']))
            logging.info('commons_wikidata_links: ' + json.dumps(weightedTags))
            allData = []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())

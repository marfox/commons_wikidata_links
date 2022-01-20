# commons_wikidata_links
Gather data from wikidata that is relevant to files on wikimedia commons, in order to improve media search on commons

* `gather_data.ipynb` gathers data relevant to commons images from wikidata, and writes it to a parquet file.
* `push_data_to_elastic.py` takes the data from the parquet file and pushes it to an elastic search index

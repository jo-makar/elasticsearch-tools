# elasticsearch-tools

## download-index.py
Download an (entire) Elasticsearch index

Each file is named as timestamp (including microsecond) of the first record.  Supports automatic
(xz) compression and continuing interrupted downloads.

## get-all-fields.py
Get all (including unmapped) fields from an Elasticsearch index.  By default recursively retrieves
all nested keys.

Refs:
- https://dev.to/ddreier/finding-un-mapped-fields-in-elasticsearch-4ejl
- https://alexmarquardt.com/category/painless/

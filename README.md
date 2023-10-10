# project05_GCP

1. Create resources
  -  Create VM: install mongoDB
  -  Create GCS: bucket-products-tiki
  -  Create BigQuery databases products
  -  Create a Google Cloud Function 

2. ETL data
  - Extract data from mongoDB local, upload to GCP mongoDB file productsTiki.json
  - Transform productsTiki.json to productsTiki.jsonl
  - Move file productsTiki.jsonl to GCS
  - Load data from file productsTiki.jsonl GCS to BigQuery databases products, table productsTiki using Google Cloud Function
  
3. Create a datamart to contain seller and product information
4. Analyze some data 
   

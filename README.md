# Information

1. The free API from [currency API](https://currencyapi.net) will be used to pull necessary exchange rates into BigQuery tables. This application will run with Google Cloud Functions and will be set to run daily at 02:00 UTC with Google Cloud Scheduler. Daily retrieved data will be kept historically in the target table. The target BigQuery table (currency_rate) will have date, currency code, and rates columns. These are used to convert non-USD amounts to USD.

2. "Ad Network" extracts daily advertising data to the relevant bucket in Google Cloud Storage in parquet format every day.

File schema:
| Attribute Name |           Definition          |
|:---------------|------------------------------:|
|       dt       |         Operation date        |
|    network     |       Advertising network     |
|   currency     |         Cost currency         |
|   platform     | Device platform (ios/android) |
|     cost       |      Advertising spend cost   |


The script 2 follows the below guidelines and information:
1. One parquet file comes daily.
2. The app checks all the files in the bucket every day.
3. If there is a new file that has not been imported into BigQuery before, the application imports that file into BigQuery.
4. One record in the breakdown of dt, network, currency, platform is be added to the target table. If there is more than one record with that breakdown in the file, the record with the highest cost is included.
5. There is at most one record in the target table in terms of dt, network, currency, platform. If the cost of the record in the new incoming file is higher than the cost in the target table, the record in the target table is updated with the one in the file. If the new cost is low, no updates is made.
6. A field named cost_usd is added for each row when transferring records to the target table. The data to be transferred to this field is calculated using the latest exchange rate information from the currency_rate table and the cost field in the row. The most recent currency rate is taken into consideration.
7. If there is a previously processed file and this file is no longer in the bucket, if there are records processed with this file (inserted and updated), these records is deleted from the target table.
8. Files previously processed with the same name are not processed again in new runs. Ad Network sometimes refreshes the file with the same name but does not change its content, so this file is ignored and not processed again. However if the file name changes, the records processed from the previous file is deleted, the records in the new file is processed.
9. This application runs with Google Cloud Functions and runs daily at 03:00 UTC with Google Cloud Scheduler.
# Google Migration Center C2C Data Import

This application can automatically create a [Google Sheets](https://sheets.google.com/) from a Migration Center generated pricing report OR import the Migration Center Report and/or AWS CUR into [Big Query](https://cloud.google.com/bigquery). If imported into BQ, a [Looker Studio](https://lookerstudio.google.com/) report can also be created.

**NOTE** - Google Sheets has a limitation of 5 million cells and this size limit prevents the import of large (multi-gigabyte) Migration Center pricing reports. If you hit the cell limitation, consider using the -b argument to import the MC data into Big Query instead. However, Google Sheets will not be created with the '-b' option and you must *manually* connect to Biq Query through the Google Sheets Data Connector. 

Further Instuctions on using the Google Sheets Data Connector with Big Query can be found [here](https://support.google.com/docs/answer/9702507).


---
### Google Cloud SDK

In order for the code to authenticate against Google, you must have the Google Cloud SDK (gcloud) installed.

Instructions for installing the Google Cloud SDK can be found in the [Install the gcloud CLI](https://cloud.google.com/sdk/docs/install) guide.

---
### Python Environment

This application requires Python3 to be installed. Once it is installed, you can install the required Python modules using the included `requirements.txt` file:

```shell
$ cd google-mc-c2c-data-import/python
$ pip3 install -r requirements.txt
```
#### Using with virtual environment 

If you wish to run the application inside of a python virtual environment, you can run the following:

```shell
$ sudo apt install python3.11-venv
$ cd google-mc-c2c-data-import/python/
$ python3 -m venv ../venv
$ source ../venv/bin/activate
(venv) $ pip3 install -r requirements.txt
```

Now you can run the python script anytime by switching to the virtual environment:

```shell
$ cd google-mc-c2c-data-import/python
$ source ../venv/bin/activate
(venv) $ python google-mc-c2c-data-import.py ....
```

---
### Authenticate to Google

In order to use the Google Drive/Sheets API, you must have a Google project setup.
Once you have a project setup, you can run the following to authenticate against the Google project using your Google account & run the application:

```shell
$ gcloud auth login
$ gcloud config set project <PROJECT-ID>
$ gcloud services enable drive.googleapis.com sheets.googleapis.com
$ gcloud auth application-default login --scopes='https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/cloud-platform'
```

If you need to save the data to Big Query, then you can authenticate using the following commands:

```shell
$ gcloud auth login
$ gcloud config set project <PROJECT-ID>
$ gcloud services enable bigquery.googleapis.com
$ gcloud auth application-default login --scopes='https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/cloud-platform'
```

---
#### Application Arguments
```shell 
$ cd google-mc-c2c-data-import/python
 python google-mc-c2c-data-import.py -h
usage: google-mc-c2c-data-import.py -d <mc report directory>
This creates an instance mapping between cloud providers and GCP

options:
  -h, --help           show this help message and exit
  -d Data Directory    Directory containing MC report output or AWS CUR data.
  -c Customer Name     Customer Name
  -e Email Addresses   Emails to share Google Sheets with (comma separated)
  -s Google Sheets ID  Use existing Google Sheets instead of creating a new one. Takes Sheets ID. Both Drive & Sheets API in GCP Project must be enabled!
  -k SA JSON Keyfile   Google Service Account JSON Key File. 
  -b                   Import Migration Center data files into Biq Query Dataset. GCP BQ API must be enabled!
  -a                   Import AWS CUR file into Biq Query Dataset. GCP BQ API must be enabled!
  -l                   Display Looker Report URL. Migration Center or AWS CUR BQ Import must be enabled!
  -r Looker Templ ID   Replaces Default Looker Report Template ID
  -i BQ Connect Info   BQ Connection Info: Format is <GCP Project ID>.<BQ Dataset Name>.<BQ Table Prefix>, i.e. googleproject.bqdataset.bqtable_prefix

```

---
#### Example Run: Google Sheets Creation


```shell 
$ cd google-mc-c2c-data-import/python
$ python3 google-mc-c2c-data-import.py -d ~/mc-reports/ -c "Demo Customer, Inc"
$ cd google-mc-c2c-data-import/python
$ python3 google-mc-c2c-data-import.py -d ~/mc-reports/ -c "Demo Customer, Inc"
Migration Center Pricing Report to Google sheets, v0.2
Customer: Demo Customer, Inc
Migration Center Reports directory: ~/mc-reports/
Checking CSV sizes...
Creating new Google Sheets...
Importing pricing report files...
Migration Center Pricing Report: Demo Customer, Inc: https://docs.google.com/spreadsheets/d/1234567890
```

---
#### Example Run: Big Query Import with Looker Report

```shell 
$ cd google-mc-c2c-data-import/python
$ python google-mc-c2c-data-import.py  -d ~/mc-reports/ -c "Demo Customer, Inc" -b -l -i test-project-id.test-customer.mc_report_
Migration Center Pricing Report to Google sheets, v0.2
Customer: Demo Customer, Inc
Migration Center Reports directory: /Users/test-user/mc-reports/
Importing data into Big Query...
GCP Project ID: test-project-id
BQ Dataset Name: test-customer
BQ Table Prefix: mc_report_

IMPORTANT: All Big Query tables will be REPLACED! Please Ctrl-C in the next 5 seconds if you wish to abort.

NOTE: Using this option will NOT automatically create a Google Sheets with your Migration Center Data.
Once the BQ import is complete, you will need to manually connect a Google Sheets to the Big Query tables using 'Data' -> 'Data Connectors' -> 'Connect to Biq Query'.
Complete Data Connector instructions can be found here: https://support.google.com/docs/answer/9702507

Migration Center Data import...
Importing pricing report files...
Dataset test-project-id.test-customer already exists.
Importing mapped.csv into BQ Table: test-project-id.test-customer.mc_report_mapped
Loaded 6288 rows and 27 columns to test-project-id.test-customer.mc_report_mapped
Importing unmapped.csv into BQ Table: test-project-id.test-customer.mc_report_unmapped
Loaded 50863 rows and 14 columns to test-project-id.test-customer.mc_report_unmapped
Importing discount.csv into BQ Table: test-project-id.test-customer.mc_report_discount
Loaded 1751 rows and 14 columns to test-project-id.test-customer.mc_report_discount
Completed loading of Migration Center Data into Big Query.

Looker URL: https://lookerstudio.google.com/reporting/create?c.reportId=421c8150-e7ad-4190-b044-6a18ecdbd391&r.reportName=AWS+-%3E+GCP+Pricing+Analysis%3A+Demo+Customer%2C+Inc%2C+2025-01-03+13%3A46&ds.ds0.connector=bigQuery&ds.ds0.datasourceName=mapped&ds.ds0.projectId=test-project-id&ds.ds0.type=TABLE&ds.ds0.datasetId=test-customer&ds.ds0.tableId=mc_report_mapped&ds.ds1.connector=bigQuery&ds.ds1.datasourceName=unmapped&ds.ds1.projectId=test-project-id&ds.ds1.type=TABLE&ds.ds1.datasetId=test-customer&ds.ds1.tableId=mc_report_unmapped
```

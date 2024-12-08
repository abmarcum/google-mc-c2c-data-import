# Google Migration Center Pricing to Google Sheets

This application will automatically create a Google Sheets from a Migration Center generated pricing report.

**NOTE** - Google Sheets has a limitation of 5 million cells and this size limit prevents the import of large (multi-gigabyte) Migration Center pricing reports. 
Consider using the -b argument to import the MC data into Big Query instead. However, Google Sheets will not be created with the '-b' option and you must *manually* connect to Biq Query through the Google Sheets Data Connector. 

Further Instuctions on using the Google Sheets Data Connector with Big Query can be found [here](https://support.google.com/docs/answer/9702507).


---
### Google Cloud SDK

In order for the code to authenticate against Google, you must have the Google Cloud SDK (gcloud) installed.

Instructions for installing the Google Cloud SDK can be found in the [Install the gcloud CLI](https://cloud.google.com/sdk/docs/install) guide.

---
### Python Environment

This application requires Python3 to be installed. Once it is installed, you can install the required Python modules using the included `requirements.txt` file:

```shell
$ cd google-mc-sheets/python
$ pip3 install -r requirements.txt
```
#### Using with virtual environment 

If you wish to run the application inside of a python virtual environment, you can run the following:

```shell
$ sudo apt install python3.11-venv
$ cd google-mc-sheets/python/
$ python3 -m venv ../venv
$ source ../venv/bin/activate
(venv) $ pip3 install -r requirements.txt
```

Now you can run the python script anytime by switching to the virtual environment:

```shell
$ cd google-mc-sheets/python
$ source ../venv/bin/activate
(venv) $ python google-mc-sheets.py ....
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

---
#### Application Arguments
```shell 
$ cd google-mc-sheets/python
python google-mc-sheets.py -h
usage: google-mc-sheets.py -d <mc report directory>
This creates an instance mapping between cloud providers and GCP

options:
  -h, --help            show this help message and exit
  -d MC Data Directory  Directory containing mc report output. Contains mapped.csv, unmapped.csv, etc
  -c Customer Name      Customer Name
  -e Email Addresses    Emails to share Google Sheets with (comma separated)
  -s Google Sheets ID   Use existing Google Sheets instead of creating a new one. Takes Sheets ID
  -k SA JSON Keyfile    Google Service Account JSON Key File. Both Drive & Sheets API in GCP Project must be enabled!
  -b                    Import Migration Center CSV files into Biq Query Dataset. GCP BQ API must be enabled!
  -n BQ Dataset Name    Biq Query Dataset Name. Default dataset name is 'mc_import_dataset'.
  -t BQ Table Prefix    Biq Query Table Name. Default table prefix is 'mc_import_'.
```

---
#### Example Run: Google Sheets Creation


```shell 
$ cd google-mc-sheets/python
$ python3 google-mc-sheets.py -d ~/mc-reports/ -c "Demo Customer, Inc"
$ cd google-mc-sheets/python
$ python3 google-mc-sheets.py -d ~/mc-reports/ -c "Demo Customer, Inc"
Migration Center Pricing Report to Google sheets, v0.2
Customer: Demo Customer, Inc
Migration Center Reports directory: ~/mc-reports/
Checking CSV sizes...
Creating new Google Sheets...
Importing pricing report files...
Migration Center Pricing Report: Demo Customer, Inc: https://docs.google.com/spreadsheets/d/1234567890
```

---
#### Example Run: Big Query Import


```shell 
$ cd google-mc-sheets/python
$ python google-mc-sheets.py -d ~/mc-reports/ -c "Demo Customer, Inc" -n cur_test_customer -t dec_2024_ -b 
Migration Center Pricing Report to Google sheets, v0.2
Customer: Demo Customer, Inc
Migration Center Reports directory: ~/mc-reports/
Importing Migration Center data into Big Query...
IMPORTANT: All Big Query tables will be REPLACED! Please Ctrl-C in the next 5 seconds if you wish to abort.
NOTE: Using the option will NOT automatically create a Google Sheets with Migration Center Data.
Once the BQ import is complete, you will need to connect a Google Sheets to the Big Query tables using 'Data' -> 'Data Connectors' -> 'Connect to Biq Query'.
Complete Data Connector instructions can be found here: https://support.google.com/docs/answer/9702507
BQ Dataset Name: cur_test_customer
BQ Table Prefix: dec_2024_
Importing pricing report files...
Dataset gcp-project-id.cur_test_customer created.
Importing mapped.csv into BQ Table: gcp-project-id.cur_test_customer.dec_2024_mapped
Loaded 1090900 rows and 27 columns to gcp-project-id.cur_test_customer.dec_2024_mapped
Importing unmapped.csv into BQ Table: gcp-project-id.cur_test_customer.dec_2024_unmapped
Loaded 4988 rows and 17 columns to gcp-project-id.cur_test_customer.dec_2024_unmapped
Importing discount.csv into BQ Table: gcp-project-id.cur_test_customer.dec_2024_discount
Loaded 0 rows and 17 columns to gcp-project-id.cur_test_customer.dec_2024_discount
Completed loading of Migration Center Data into Big Query.
```

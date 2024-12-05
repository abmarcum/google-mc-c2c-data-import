# Google Migration Center Pricing to Google Sheets

This application will automatically create a Google Sheets from a Migration Center generated pricing report.

**NOTE** - Google Sheets has a limitation of 5 million cells and this size limit prevents the import of large (multi-gigabyte) Migration Center pricing reports. A work-around is to import the pricing reports into Big Query *manually* and connect a Google Sheet through the Biq Query connector. 


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
(venv) $ python3 google-mc-sheets.py ....
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
#### Example Run


```shell 
$ cd google-mc-sheets/python
$ python3 google-mc-sheets.py -d ~/mc-reports/ -c "Demo Customer, Inc"
Pricing Report to Google sheets, v0.1
Customer: Demo Customer, Inc
Pricing reports directory: /Users/demo/reports/
Creating new Google Sheets...
Importing MC csv files...
Migration Center Pricing Report: Demo Customer, Inc: https://docs.google.com/spreadsheets/d/1234567890
```

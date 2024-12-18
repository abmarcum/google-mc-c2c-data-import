#################################################################
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Migration Pricing Reports to Google Sheets

# v0.2
# Google
# amarcum@google.com
#################################################################

import pandas as pd
import urllib
import gspread
import csv
import datetime
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import google.auth
from gspread_formatting import *
import argparse
import time
import re
import os
import json

version = "v0.2"
datetime = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
username = os.environ['USER']
if username == 'root':
    print("User root not allowed to run this application! Exiting...")
    exit()

# Order to sort worksheets and new names to use
f = open('mappings.json', )
mappings_file = json.load(f)
mc_names = mappings_file["mc_names"]
mc_column_names = mappings_file["mc_column_names"]
f.close()


# Check number of rows & columns in CSV file
def check_csv_size(mc_reports_directory):
    print("Checking CSV sizes...")
    mc_file_list = os.listdir(mc_reports_directory)
    if any('.csv' in file for file in mc_file_list):
        for file in mc_file_list:
            if file.endswith(".csv"):
                file_fullpath = (mc_reports_directory + "/" + file)
                csv_data = pd.read_csv(file_fullpath, nrows=1)
                try:
                    number_of_columns = len(csv_data.values[0].tolist())
                except:
                    number_of_columns = 0

                with open(file_fullpath, "rb") as f:
                    number_of_rows = sum(1 for _ in f)

                total_cells = number_of_rows * number_of_columns
                if total_cells > 5000000:
                    print(file + " exceeds the 5 million cell Google Sheets limit (" + str(
                        total_cells) + ") and therefor cannot be imported through the Google Sheets API. Consider using the -b argument to import into Big Query instead. NOTE: Google Sheets will not be created with the -b option. Exiting now due to Google Sheets size limitations.")
                    exit()
    else:
        print("No CSV files found in " + mc_reports_directory + "! Exiting!")
        exit()


# Create Initial Google Sheets
def create_google_sheets(customer_name, sheets_email_addresses, service_account_key, sheets_id):
    if sheets_id == "":
        print("Creating new Google Sheets...")
    else:
        print("Updating Google Sheets: " + sheets_id)

    scope = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
    sheets_title = ("Migration Center Pricing Report: " + customer_name + ' - ' + datetime)

    # Use provided Google Service Account Key, otherwise try to use gcloud auth key to authenticate

    credentials = google_auth(service_account_key, scope)

    client = gspread.authorize(credentials)

    # Depending on CLI Args - create new sheet or update existing
    if sheets_id == '':
        spreadsheet = client.create(sheets_title)
        spreadsheet = client.open(sheets_title)

    else:
        spreadsheet = client.open_by_key(sheets_id)

    # If any emails are provided, share sheets with them
    for shared_user in sheets_email_addresses:
        spreadsheet.share(shared_user, perm_type='user', role='writer', notify=False)

    return spreadsheet, credentials


# Create Pivot table with sums for Google Sheets
def generate_pivot_table_request_sum(data_spreadsheet, location_spreadsheet, last_data_column, last_data_row,
                                     location_data,
                                     rows_column_offset, second_rows_column_offset, values_column_offset,
                                     values_column_offset_2nd):
    # Google Sheets Pivot Table API: https://developers.google.com/sheets/api/samples/pivot-tables

    # If 2nd rows for Pivot Table is empty, don't include it. Otherwise, do include.
    if second_rows_column_offset is None:
        if values_column_offset_2nd == 0:
            pivot_table_body = {
                "requests": [
                    {
                        'updateCells': {
                            'rows': [
                                {
                                    'values': [
                                        {
                                            'pivotTable': {
                                                'source': {
                                                    'sheetId': data_spreadsheet,
                                                    'startRowIndex': 0,
                                                    'startColumnIndex': 0,
                                                    'endRowIndex': last_data_row,
                                                    'endColumnIndex': last_data_column
                                                },
                                                'rows': [
                                                    {
                                                        'sourceColumnOffset': rows_column_offset,
                                                        'showTotals': True,
                                                        "sortOrder": "DESCENDING",
                                                        "valueBucket": {}
                                                    }
                                                ],
                                                'values': [
                                                    {
                                                        "name": "Total Cost",
                                                        'summarizeFunction': 'SUM',
                                                        'sourceColumnOffset': values_column_offset
                                                    }
                                                ],
                                                'valueLayout': 'HORIZONTAL'
                                            }
                                        }
                                    ]
                                },
                            ],
                            'start': {
                                'sheetId': location_spreadsheet,
                                'rowIndex': location_data[1],
                                'columnIndex': location_data[0]
                            },
                            'fields': 'pivotTable'
                        }
                    }
                ]
            }
        else:
            pivot_table_body = {
                "requests": [
                    {
                        'updateCells': {
                            'rows': [
                                {
                                    'values': [
                                        {
                                            'pivotTable': {
                                                'source': {
                                                    'sheetId': data_spreadsheet,
                                                    'startRowIndex': 0,
                                                    'startColumnIndex': 0,
                                                    'endRowIndex': last_data_row,
                                                    'endColumnIndex': last_data_column
                                                },
                                                'rows': [
                                                    {
                                                        'sourceColumnOffset': rows_column_offset,
                                                        'showTotals': True,
                                                        "sortOrder": "DESCENDING",
                                                        "valueBucket": {}
                                                    }
                                                ],
                                                'values': [
                                                    {
                                                        "name": "GCP Cost",
                                                        'summarizeFunction': 'SUM',
                                                        'sourceColumnOffset': values_column_offset
                                                    },
                                                    {
                                                        "name": "AWS Cost",
                                                        'summarizeFunction': 'SUM',
                                                        'sourceColumnOffset': values_column_offset_2nd
                                                    },
                                                ],
                                                'valueLayout': 'HORIZONTAL'
                                            }
                                        }
                                    ]
                                },
                            ],
                            'start': {
                                'sheetId': location_spreadsheet,
                                'rowIndex': location_data[1],
                                'columnIndex': location_data[0]
                            },
                            'fields': 'pivotTable'
                        }
                    }
                ]
            }
    else:
        pivot_table_body = {
            "requests": [
                {
                    'updateCells': {
                        'rows': [
                            {
                                'values': [
                                    {
                                        'pivotTable': {
                                            'source': {
                                                'sheetId': data_spreadsheet,
                                                'startRowIndex': 0,
                                                'startColumnIndex': 0,
                                                'endRowIndex': last_data_row,
                                                'endColumnIndex': last_data_column
                                            },
                                            'rows': [
                                                {
                                                    'sourceColumnOffset': rows_column_offset,
                                                    'showTotals': True,
                                                    "sortOrder": "DESCENDING",
                                                    "valueBucket": {}
                                                },
                                                {
                                                    'sourceColumnOffset': second_rows_column_offset,
                                                    'showTotals': False,
                                                    "sortOrder": "DESCENDING",
                                                    "valueBucket": {}
                                                }
                                            ],
                                            'values': [
                                                {
                                                    "name": "Total Cost",
                                                    'summarizeFunction': 'SUM',
                                                    'sourceColumnOffset': values_column_offset
                                                }
                                            ],
                                            'valueLayout': 'HORIZONTAL'
                                        }
                                    }
                                ]
                            },
                        ],
                        'start': {
                            'sheetId': location_spreadsheet,
                            'rowIndex': location_data[1],
                            'columnIndex': location_data[0]
                        },
                        'fields': 'pivotTable'
                    }
                }
            ]
        }

    return pivot_table_body


# Create Pivot table with counts for Google Sheets
def generate_pivot_table_request_count(data_spreadsheet, location_spreadsheet, last_data_column, last_data_row,
                                       location_data,
                                       rows_column_offset, second_rows_column_offset, values_column_offset,
                                       show_totals):
    # Google Sheets Pivot Table API: https://developers.google.com/sheets/api/samples/pivot-tables

    # If 2nd rows for Pivot Table is empty, don't include it. Otherwise, do include.
    if second_rows_column_offset is None:
        pivot_table_body = {
            "requests": [
                {
                    'updateCells': {
                        'rows': [
                            {
                                'values': [
                                    {
                                        'pivotTable': {
                                            'source': {
                                                'sheetId': data_spreadsheet,
                                                'startRowIndex': 0,
                                                'startColumnIndex': 0,
                                                'endRowIndex': last_data_row,
                                                'endColumnIndex': last_data_column
                                            },
                                            'rows': [
                                                {
                                                    'sourceColumnOffset': rows_column_offset,
                                                    'showTotals': show_totals,
                                                    "sortOrder": "DESCENDING",
                                                    "valueBucket": {}
                                                }
                                            ],
                                            'values': [
                                                {
                                                    "name": "Totals",
                                                    'summarizeFunction': 'COUNTA',
                                                    'sourceColumnOffset': values_column_offset
                                                }
                                            ],
                                            'valueLayout': 'HORIZONTAL'
                                        }
                                    }
                                ]
                            },
                        ],
                        'start': {
                            'sheetId': location_spreadsheet,
                            'rowIndex': location_data[1],
                            'columnIndex': location_data[0]
                        },
                        'fields': 'pivotTable'
                    }
                }
            ]
        }
    else:
        pivot_table_body = {
            "requests": [
                {
                    'updateCells': {
                        'rows': [
                            {
                                'values': [
                                    {
                                        'pivotTable': {
                                            'source': {
                                                'sheetId': data_spreadsheet,
                                                'startRowIndex': 0,
                                                'startColumnIndex': 0,
                                                'endRowIndex': last_data_row,
                                                'endColumnIndex': last_data_column
                                            },
                                            'rows': [
                                                {
                                                    'sourceColumnOffset': rows_column_offset,
                                                    'showTotals': show_totals,
                                                    "sortOrder": "DESCENDING",
                                                    "valueBucket": {}
                                                },
                                                {
                                                    'sourceColumnOffset': second_rows_column_offset,
                                                    'showTotals': False,
                                                    "sortOrder": "DESCENDING",
                                                    "valueBucket": {}
                                                }
                                            ],
                                            'values': [
                                                {
                                                    "name": "Total",
                                                    'summarizeFunction': 'COUNTA',
                                                    'sourceColumnOffset': values_column_offset
                                                }
                                            ],
                                            'valueLayout': 'HORIZONTAL'
                                        }
                                    }
                                ]
                            },
                        ],
                        'start': {
                            'sheetId': location_spreadsheet,
                            'rowIndex': location_data[1],
                            'columnIndex': location_data[0]
                        },
                        'fields': 'pivotTable'
                    }
                }
            ]
        }

    return pivot_table_body


def generate_pie_table_request(spreadsheet, chart_title, first_column, second_column, position_data):
    # Google Sheets Charts API: https://developers.google.com/sheets/api/samples/charts

    pie_chart_body = {
        "requests": [
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": chart_title,
                            "pieChart": {
                                "legendPosition": "LABELED_LEGEND",
                                "domain": {
                                    "sourceRange": {
                                        "sources": [
                                            {
                                                "sheetId": spreadsheet,
                                                "startRowIndex": 0,
                                                "startColumnIndex": first_column,
                                                "endColumnIndex": first_column + 1
                                            }
                                        ]
                                    }
                                },
                                "series": {
                                    "sourceRange": {
                                        "sources": [
                                            {
                                                "sheetId": spreadsheet,
                                                "startRowIndex": 0,
                                                "startColumnIndex": second_column,
                                                "endColumnIndex": second_column + 1
                                            }
                                        ]
                                    }
                                },
                                "threeDimensional": True
                            }
                        },
                        "position": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": spreadsheet,
                                    "columnIndex": position_data[0],
                                    "rowIndex": position_data[1],
                                }
                            }
                        }
                    }
                }
            }
        ]
    }
    return pie_chart_body


def autosize_worksheet(sheet_id, first_col, last_col):
    # Autoresize Worksheet - Body values
    body = {
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": first_col,  # Please set the column index.
                        "endIndex": last_col  # Please set the column index.
                    }
                }
            }
        ]
    }

    time.sleep(1)
    return body


# Import mc data from provided reports directory
def import_mc_data(mc_reports_directory, spreadsheet, credentials):
    sheets_id = spreadsheet.id
    mc_data = {}
    # Grabbing a list of files from the provided mc directory
    try:
        mc_file_list = os.listdir(mc_reports_directory)
        print("Importing pricing report files...")
    except:
        print("Unable to access directory: " + mc_reports_directory)
        exit()

    client = gspread.authorize(credentials)
    sh = client.open_by_key(sheets_id)

    # Importing all CSV files into a dictionary of dataframes
    for file in mc_file_list:
        if file.endswith(".csv"):
            file_fullpath = (mc_reports_directory + "/" + file)
            file_name, _ = file.rsplit(".csv")
            sheet_name = mc_names[file_name]
            mc_data[file_name] = pd.read_csv(file_fullpath)

            worksheet = sh.add_worksheet(title=sheet_name, rows=100, cols=20)
            sh.values_update(
                # file.rsplit(".csv"),
                sheet_name,
                params={'valueInputOption': 'USER_ENTERED'},
                body={'values': list(csv.reader(open(file_fullpath)))})

    # Delete default worksheet
    worksheet = sh.worksheet("Sheet1")
    sh.del_worksheet(worksheet)

    mc_names_list = []
    # Create GCP Overview Sheets Pivot Table
    overview_worksheets_name = "GCP Overview"
    overview_worksheet = spreadsheet.add_worksheet(overview_worksheets_name, 60, 15)
    overview_worksheet_id = overview_worksheet._properties['sheetId']
    mc_names_list.append(overview_worksheet)

    # Create Unmapped Overview Sheets Pivot Table
    unmapped_overview_worksheets_name = "Unmapped Overview"
    unmapped_overview_worksheet = spreadsheet.add_worksheet(unmapped_overview_worksheets_name, 60, 15)
    unmapped_overview_worksheet_id = unmapped_overview_worksheet._properties['sheetId']
    mc_names_list.append(unmapped_overview_worksheet)

    # Get worksheet names & add them to a list to reorder
    for name in mc_names.keys():
        current_worksheet = spreadsheet.worksheet(mc_names[name])
        mc_names_list.append(current_worksheet)

        # Set Filter on all column headers
        current_worksheet.set_basic_filter()

        # Freeze First Row / Column Headers
        set_frozen(current_worksheet, rows=1)

        # Autosize All Cols
        worksheet_id = current_worksheet._properties['sheetId']
        first_col = 0
        last_col = 14
        res = spreadsheet.batch_update(autosize_worksheet(worksheet_id, first_col, last_col))

    # mapped.csv Data Information
    mapped_worksheet_id = spreadsheet.worksheet(mc_names["mapped"])
    mapped_csv_header_length = 25 + 1
    mapped_csv_num_rows = len(mc_data["mapped"]) + 1

    # unmapped.csv Data Information
    unmapped_worksheet_id = spreadsheet.worksheet(mc_names["unmapped"])
    unmapped_csv_header_length = 25 + 1
    unmapped_csv_num_rows = len(mc_data["unmapped"]) + 1

    # Create pivot table of GCP Services & Total cost for each
    rows_column_offset = 5  # Data, Column F
    values_column_offset = 23  # Data, Column X, GCP Cost
    values_column_offset_2nd = 19  # Data, Column T, AWS Cost
    location_data = [0, 0]  # Cell: Column A, Row 1
    res = spreadsheet.batch_update(
        generate_pivot_table_request_sum(mapped_worksheet_id.id, overview_worksheet_id, mapped_csv_header_length,
                                         mapped_csv_num_rows, location_data,
                                         rows_column_offset, None, values_column_offset, values_column_offset_2nd))

    # Create pivot table of GCP Machine types & totals for each
    rows_column_offset = 10  # Data, Column K
    values_column_offset = 10  # Data, Column K
    location_data = [4, 0]  # Cell: Column E, Row 1
    res = spreadsheet.batch_update(
        generate_pivot_table_request_count(mapped_worksheet_id.id, overview_worksheet_id, mapped_csv_header_length,
                                           mapped_csv_num_rows, location_data,
                                           7, rows_column_offset, values_column_offset, False))
    # Add Piechart of GCP Machine types in GCP Overview
    chart_title = "GCP Instance Breakdown"
    first_column = 5  # Col F
    second_column = 6  # Col G

    position_data = [
        8,  # Column I
        0  # Row 1
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(overview_worksheet_id, chart_title, first_column, second_column, position_data))

    # Autosize first cols in GCP Overview worksheet
    first_col = 0
    last_col = 10
    res = spreadsheet.batch_update(autosize_worksheet(overview_worksheet_id, first_col, last_col))

    # Change AWS Cost Totals to Currency format
    overview_worksheet.format("B", {
        "numberFormat": {"type": "CURRENCY"}
    })
    # Change GCP Cost Totals to Currency format
    overview_worksheet.format("C", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Create pivot table of Unmapped Services & Total cost for each
    rows_column_offset = 2  # Data, Column C
    values_column_offset = 9  # Data, Column V
    values_column_offset_2nd = 0  # Ignore
    location_data = [0, 0]  # Cell: Column A, Row 1
    res = spreadsheet.batch_update(
        generate_pivot_table_request_sum(unmapped_worksheet_id.id, unmapped_overview_worksheet_id,
                                         unmapped_csv_header_length,
                                         unmapped_csv_num_rows, location_data,
                                         rows_column_offset, None, values_column_offset, values_column_offset_2nd))

    # Add Piechart of Unmapped Services Unmapped Overview
    chart_title = "Unmapped Services Breakdown"
    first_column = 0  # Col A
    second_column = 1  # Col B

    position_data = [
        3,  # Column D
        0  # Row 1
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(unmapped_overview_worksheet.id, chart_title, first_column, second_column,
                                   position_data))

    # Change Unmapped Cost Totals to Currency format
    unmapped_overview_worksheet.format("B", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Autosize first cols in Unmapped Overview worksheet
    first_col = 0
    last_col = 10
    res = spreadsheet.batch_update(autosize_worksheet(unmapped_overview_worksheet_id, first_col, last_col))

    # Reorder worksheet tabs based on mc_names order
    spreadsheet.reorder_worksheets(mc_names_list)


def google_auth(service_account_key, scope):
    # Use provided Google Service Account Key, otherwise try to use gcloud auth key to authenticate
    if service_account_key != "":
        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(service_account_key, scope)
        except IOError:
            print("Google Service account key: " + service_account_key + " does not appear to exist! Exiting...")
            exit()
    else:
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            os.environ[
                "GOOGLE_APPLICATION_CREDENTIALS"] = (os.path.expanduser(
                '~' + username) + "/.config/gcloud/application_default_credentials.json")

        try:
            credentials, _ = google.auth.default(scopes=scope)
        except:
            print("Unable to auth against Google...")
            exit()
    return credentials


def import_mc_into_bq(mc_reports_directory, gcp_project_id, bq_dataset_name, bq_table_prefix, service_account_key, customer_name):
    ##CHANGE
    # gcp_project_id = "amarcum-argolis-pricing-ewuu3b"

    # GCP Scope for auth
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    #Google Auth
    credentials = google_auth(service_account_key, scope)
    client = gspread.authorize(credentials)

    # Looker Settings
    looker_url_prefix = "https://lookerstudio.google.com/reporting/create?c.reportId="
    looker_template_id = "421c8150-e7ad-4190-b044-6a18ecdbd391"
    looker_report_name = f"AWS -> GCP Pricing Analysis: {customer_name}, {datetime}"
    looker_report_name = urllib.parse.quote_plus(looker_report_name)

    looker_ds0_project_id = gcp_project_id  # Mapped BQ Project ID
    looker_ds0_bq_datasource_name = "mapped"  # Mapped BQ Looker Name
    looker_ds0_bq_dataset = bq_dataset_name  # Mapped BQ Dataset
    looker_ds0_bq_table = f"{bq_table_prefix}mapped"  # Mapped BQ Table

    looker_ds1_project_id = gcp_project_id  # Unmapped BQ Project ID
    looker_ds1_bq_datasource_name = "unmapped"  # Unmapped BQ Looker Name
    looker_ds1_bq_dataset = bq_dataset_name  # Unmapped BQ Dataset
    looker_ds1_bq_table = f"{bq_table_prefix}unmapped"  # Unmapped BQ Table

    looker_ds2_project_id = ""  # Discount BQ Project ID
    looker_ds2_bq_datasource_name = ""  # Discount BQ Looker Name
    looker_ds2_bq_dataset = ""  # Discount BQ Dataset
    looker_ds2_bq_table = ""  # Discount BQ Table

    looker_report_url = f"{looker_url_prefix}{looker_template_id}&r.reportName={looker_report_name}&ds.ds0.connector=bigQuery&ds.ds0.datasourceName={looker_ds0_bq_datasource_name}&ds.ds0.projectId={looker_ds0_project_id}&ds.ds0.type=TABLE&ds.ds0.datasetId={looker_ds0_bq_dataset}&ds.ds0.tableId={looker_ds0_bq_table}&ds.ds1.connector=bigQuery&ds.ds1.datasourceName={looker_ds1_bq_datasource_name}&ds.ds1.projectId={looker_ds1_project_id}&ds.ds1.type=TABLE&ds.ds1.datasetId={looker_ds1_bq_dataset}&ds.ds1.tableId={looker_ds1_bq_table}"


    mc_data = {}
    mc_file_list = []
    # Grabbing a list of files from the provided mc directory
    try:
        print("Importing pricing report files...")
        for file in mappings_file["mc_names"].keys():
            if os.path.isfile(f"{mc_reports_directory}{file}.csv"):
                mc_file_list.append(f"{file}")
        # mc_file_list = os.listdir(f"{mc_reports_directory}/*.csv")
    except:
        print("Unable to access directory: " + mc_reports_directory)
        exit()
    # Verify MC files exist
    if len(mc_file_list) != len(mappings_file["mc_names"].keys()):
        print("Required MC data files do not exist! Exiting!")
        exit()

    # Create BQ dataset
    client = bigquery.Client()
    dataset_id = f"{gcp_project_id}.{bq_dataset_name}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    try:
        client.get_dataset(dataset_id)  # Check if dataset exists
        print(f"Dataset {dataset_id} already exists.")
    except:
        dataset.location = "US"
        try:
            dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        except:
            print(f"Unable to create dataset: {dataset_id}")
            exit()

        print(f"Dataset {dataset_id} created.")

    # Importing all CSV files into a dictionary of dataframes
    for file in mc_file_list:
        bq_table_name = (f"{bq_table_prefix}{file.replace('.csv', '')}")
        table_id = (f"{gcp_project_id}.{bq_dataset_name}.{bq_table_name}")
        print(f"Importing {file} into BQ Table: {table_id}")
        set_gcp_project = f"gcloud config set project {gcp_project_id} >/dev/null 2>&1"

        # file_name, _ = file.rsplit(".csv")
        schema = ""
        for column in mc_column_names[file].keys():
            schema = schema + f"\"{column}\":{mc_column_names[file][column]},"

        # Remove last comma
        schema = schema[:-1]
        try:
            os.system(set_gcp_project)
        except Exception as e:
            print(f"error: {e}")

        # if file.endswith(".csv"):
        file_fullpath = (f"{mc_reports_directory}{file}.csv")

        # # Having to use bq load CLI due to apparent bug in python BQ library
        # bq_cli_load_command = f"bq load --autodetect --skip_leading_rows=1 --allow_quoted_newlines --column_name_character_map=V2 --source_format=CSV --replace=true {bq_dataset_name}.{bq_table_name} {file_fullpath} {schema}"
        # # print(bq_cli_load_command)
        # try:
        #     os.system(bq_cli_load_command)
        #     print("Completed.")
        # except Exception as e:
        #     print(f"error: {e}")
        #     exit()

        # Apparent bug with python BQ library not recognizing column character map options - having to create schema file manually
        # file_name, _ = file.rsplit(".csv")
        sheet_name = mc_names[file]
        mc_data[file] = pd.read_csv(file_fullpath, low_memory=False)
        # Replacing column names since BQ doesn't like them with () & the python library "column character map" version doesn't appear to work.
        mc_data[file].rename(columns={
            "Memory (GB)": "Memory_GB",
            "External Memory (GB)": "External_Memory_GB",
            "Sub-Type_1": "Sub_Type_1",
            "Sub-Type_2": "Sub_Type_2",
            "Dest_Series": "Destination_Series",
            "Extended_Memory_GB": "External_Memory_GB",
            "Dest_Shape": "Destination_Shape",
            "OS_or_Licenses_Cost": "OS_Licenses_Cost"
        }, inplace=True)
        mc_data[file].rename(columns=lambda x: x.replace(" ", "_"), inplace=True)

        # print(mc_data[file].columns)
        dataframe = pd.DataFrame(
            mc_data[file],
            # In the loaded table, the column order reflects the order of the
            # columns in the DataFrame.
            columns=mc_column_names[file]
        )

        schema = []
        # col_count = 0
        # Create Schema Fields for BQ
        for column in mc_column_names[file].keys():
            if mc_column_names[file][column] == 'STRING':
                schema.append(bigquery.SchemaField(column, bigquery.enums.SqlTypeNames.STRING))
            elif mc_column_names[file][column] == 'FLOAT64':
                schema.append(bigquery.SchemaField(column, bigquery.enums.SqlTypeNames.FLOAT64))

            # col_count += 1

        # print("Schema Length: " + str(len(schema)))
        # print("DF Cols: " + str(len(mc_column_names[file].keys())))
        job_config = bigquery.LoadJobConfig(

            autodetect=True,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            column_name_character_map="V2",
            allow_quoted_newlines=True,
            #quote_character="",
            schema=schema,
            source_format=bigquery.SourceFormat.CSV
        )

        job = client.load_table_from_dataframe(
            mc_data[file], table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    print("Completed loading of Migration Center Data into Big Query.")

    print(f"Looker URL: {looker_report_url}")


# Parse CLI Arguments
def parse_cli_args():
    parser = argparse.ArgumentParser(prog='google-mc-sheets.py',
                                     usage='%(prog)s -d <mc report directory>\nThis creates an instance mapping between cloud providers and GCP')
    parser.add_argument('-d', metavar='MC Data Directory',
                        help='Directory containing mc report output. Contains mapped.csv, unmapped.csv, etc',
                        required=True, )
    parser.add_argument('-c', metavar='Customer Name', help='Customer Name',
                        required=False, )
    parser.add_argument('-e', metavar='Email Addresses', help='Emails to share Google Sheets with (comma separated)',
                        required=False, )
    parser.add_argument('-s', metavar='Google Sheets ID', required=False,
                        help='Use existing Google Sheets instead of creating a new one. Takes Sheets ID')
    parser.add_argument('-k', metavar='SA JSON Keyfile', required=False,
                        help='Google Service Account JSON Key File. Both Drive & Sheets API in GCP Project must be enabled! ')
    parser.add_argument('-b', action='store_true', required=False,
                        help='Import Migration Center CSV files into Biq Query Dataset. GCP BQ API must be enabled! ')
    parser.add_argument('-i', metavar='BQ Connect Info', required=False,
                        help='BQ Connection Info: Format is <GCP Project ID>.<BQ Dataset Name>.<BQ Table Prefix>, i.e. googleproject.bqdataset.bqtable_prefix')
    # parser.add_argument('-t', metavar='BQ Table Prefix', required=False,
    #                     help='Biq Query Table Name. Default table prefix is \'mc_import_\'.')
    return parser.parse_args()


def main():
    args = parse_cli_args()

    mc_reports_directory = args.d
    enable_bq_import = args.b

    print(f"Migration Center Pricing Report to Google sheets, {version}")

    if args.c is not None:
        customer_name = args.c
    else:
        customer_name = "No Name Customer, Inc."

    print("Customer: " + customer_name)

    if mc_reports_directory is not None:
        print("Migration Center Reports directory: " + mc_reports_directory)
    else:
        print("Migration Center Reports directory not defined, exiting!")
        exit()

    if enable_bq_import is not True:
        check_csv_size(mc_reports_directory)

        if args.e is not None:
            sheets_emails = args.e
            sheets_email_addresses = sheets_emails.split(",")
            print("Sharing Sheets with: ")
            for email in sheets_email_addresses:
                print(email)
        else:
            sheets_email_addresses = ""

        if args.k is not None:
            service_account_key = args.k
            print("Using Google Service Account key: " + service_account_key)
        else:
            service_account_key = ""

        if args.s is not None:
            sheets_id = args.s
        else:
            sheets_id = ""

        spreadsheet, credentials = create_google_sheets(customer_name, sheets_email_addresses, service_account_key,
                                                        sheets_id)

        import_mc_data(mc_reports_directory, spreadsheet, credentials)

        spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % spreadsheet.id

        print("Migration Center Pricing Report for " + customer_name + ": " + spreadsheet_url)
    else:
        bq_connection_info = args.i
        (gcp_project_id, bq_dataset_name, bq_table_prefix) = bq_connection_info.split(".")

        print("Importing Migration Center data into Big Query...")
        print(f"GCP Project ID: {gcp_project_id}")
        print(f"BQ Dataset Name: {bq_dataset_name}")
        print(f"BQ Table Prefix: {bq_table_prefix}")
        print(
            "IMPORTANT: All Big Query tables will be REPLACED! Please Ctrl-C in the next 5 seconds if you wish to abort.")
        time.sleep(5)
        print("NOTE: Using this option will NOT automatically create a Google Sheets with your Migration Center Data.")
        print(
            "Once the BQ import is complete, you will need to manually connect a Google Sheets to the Big Query tables using 'Data' -> 'Data Connectors' -> 'Connect to Biq Query'.")
        print("Complete Data Connector instructions can be found here: https://support.google.com/docs/answer/9702507")
        # if args.n is not None:
        #     bq_dataset_name = args.n
        # else:
        #     bq_dataset_name = "mc_import_dataset"
        #
        # if args.t is not None:
        #     bq_table_prefix = args.t
        # else:
        #     bq_table_prefix = "mc_import_"

        if args.k is not None:
            service_account_key = args.k
            print("Using Google Service Account key: " + service_account_key)
        else:
            service_account_key = ""

        if args.c is not None:
            customer_name = args.c
        else:
            customer_name = "No Name Customer, Inc."

        import_mc_into_bq(mc_reports_directory, gcp_project_id, bq_dataset_name, bq_table_prefix, service_account_key, customer_name)


if __name__ == "__main__":
    main()

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

# v0.1
# Google
# amarcum@google.com
#################################################################

import pandas as pd
import pytz
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

datetime = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
username = os.environ['USER']
if username == 'root':
    print("User root not allowed to run this application! Exiting...")
    exit()

# Order to sort worksheets and new names to use
mc_names = {
    "mapped": "GCP Mapped Data (mapped)",
    "unmapped": "Unmapped Data (unmapped)",
    # "source-merged": "Original Data, Merged (source-merged)",
    # "tax": "Tax",
    "discount": "Discounts"
}

mc_column_names = {
    "mapped": ["ID", "Item Type", "Account Or Subscription", "Source Product", "Source Product Name", "GCP Service",
               "Description", "Region", "Source Shape", "Destination Series", "Destination Shape", "Type", "Sub Type 1",
               "Sub Type 2", "vCPUs", "Memory - GB", "External Memory - GB", "Quantity", "Quantity Type", "Source Cost",
               "Source Currency", "Infra Cost", "OS Licenses Cost", "GCP Cost", "GCP Currency", "Warning",
               "Warning Messages"],
    "unmapped": ["identity_LineItemId", "lineItem_LineItemType", "lineItem_ProductCode", "lineItem_ProductName",
                 "lineItem_UsageType", "lineItem_Operation", "lineItem_ResourceId", "lineItem_UsageAmount",
                 "lineItem_CurrencyCode", "lineItem_UnblendedRate", "lineItem_UnblendedCost",
                 "lineItem_LineItemDescription", "Error", "ErrorMessage"],
    # "source-merged": [""],,
    # "tax": [""],,
    "discount": ["identity_LineItemId", "lineItem_LineItemType", "lineItem_ProductCode", "lineItem_ProductName",
                 "lineItem_UsageType", "lineItem_Operation", "lineItem_ResourceId", "lineItem_UsageAmount",
                 "lineItem_CurrencyCode", "lineItem_UnblendedRate", "lineItem_UnblendedCost",
                 "lineItem_LineItemDescription", "Error", "ErrorMessage"]
}

mc_column_datatypes = {
    "mapped": ["STRING", "STRING", "INT64", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING",
               "STRING", "STRING", "STRING", "STRING", "INT64", "INT64", "INT64", "NUMERIC", "STRING", "NUMERIC",
               "STRING", "NUMERIC", "NUMERIC", "NUMERIC", "STRING", "STRING", "STRING"],
    "unmapped": ["STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "NUMERIC", "STRING", "NUMERIC",
                 "NUMERIC", "STRING", "STRING", "STRING"],
    # "source-merged": [""],,
    # "tax": [""],,
    "discount": ["STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "NUMERIC", "STRING", "NUMERIC",
                 "NUMERIC", "STRING", "STRING", "STRING"],
}


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
                        total_cells) + ") and therefor cannot be imported through the Google Sheets API. A workaround is to import the pricing reports into Biq Query manually and connecting a Google Sheet through the Biq Query Connector. Exiting now due to Google Sheets size limitations.")
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
    # if service_account_key != "":
    #     try:
    #         credentials = ServiceAccountCredentials.from_json_keyfile_name(service_account_key, scope)
    #     except IOError:
    #         print("Google Service account key: " + service_account_key + " does not appear to exist! Exiting...")
    #         exit()
    # else:
    #     if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    #         os.environ[
    #             "GOOGLE_APPLICATION_CREDENTIALS"] = (os.path.expanduser(
    #             '~' + username) + "/.config/gcloud/application_default_credentials.json")
    #
    #     try:
    #         credentials, _ = google.auth.default(scopes=scope)
    #     except:
    #         print("Unable to auth against Google...")
    #         exit()

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

            # mc_df[file.rstrip('.csv')] = pd.read_csv(file_fullpath, index_col=None, low_memory=False)

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


def import_mc_into_bq(mc_reports_directory, bq_dataset_name, bq_table_prefix, service_account_key):
    gcp_project_id = "amarcum-argolis-pricing-ewuu3b"
    # GCP Scope for auth
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    #Google Auth
    credentials = google_auth(service_account_key, scope)
    client = gspread.authorize(credentials)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    mc_data = {}
    # Grabbing a list of files from the provided mc directory
    try:
        mc_file_list = os.listdir(mc_reports_directory)
        print("Importing pricing report files...")
    except:
        print("Unable to access directory: " + mc_reports_directory)
        exit()

    # Importing all CSV files into a dictionary of dataframes
    for file in mc_file_list:
        bq_table_name = (f"{bq_table_prefix}{file.replace('.csv', '')}")
        table_id = (f"{gcp_project_id}.{bq_dataset_name}.{bq_table_name}")
        print(f"Importing {file} into BQ Table: {table_id}")
        if file.endswith(".csv"):
            file_fullpath = (mc_reports_directory + "/" + file)
            file_name, _ = file.rsplit(".csv")
            sheet_name = mc_names[file_name]
            mc_data[file_name] = pd.read_csv(file_fullpath)

            dataframe = pd.DataFrame(
                mc_data[file_name],
                # In the loaded table, the column order reflects the order of the
                # columns in the DataFrame.
                columns=mc_column_names[file_name]
            )

            # print(dataframe)
            # schema = []
            # col_count = 0
            # Create Schema Fields for BQ
            # for column in mc_column_names[file_name]:
            #     if mc_column_datatypes[file_name][col_count] == 'STRING':
            #         schema.append(bigquery.SchemaField(column, bigquery.enums.SqlTypeNames.STRING))
            #     elif mc_column_datatypes[file_name][col_count] == 'NUMERIC':
            #         schema.append(bigquery.SchemaField(column, bigquery.enums.SqlTypeNames.NUMERIC))
            #
            #     col_count += 1

            #print(schema)
            job_config = bigquery.LoadJobConfig(
                # Specify a (partial) schema. All columns are always written to the
                # table. The schema is used to assist in data type definitions.
                # schema=schema,
                # Optionally, set the write disposition. BigQuery appends loaded rows
                # to an existing table by default, but with WRITE_TRUNCATE write
                # disposition it replaces the table with the loaded data.
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                column_name_character_map="V2",
                source_format=bigquery.SourceFormat.CSV
            )

            job = client.load_table_from_dataframe(
                mc_data[file_name], table_id, job_config=job_config
            )  # Make an API request.
            job.result()  # Wait for the job to complete.

            table = client.get_table(table_id)  # Make an API request.
            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )


# Parse CLI Arguments
def parse_cli_args():
    parser = argparse.ArgumentParser(prog='google-mc-sheets.py',
                                     usage='%(prog)s -d <mc report directory> ./\nThis creates an instance mapping between cloud providers and GCP')
    parser.add_argument('-d', metavar='Migration Center Pricing Reports Directory',
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
    parser.add_argument('-n', metavar='BQ Dataset Name', required=False,
                        help='Biq Query Dataset Name. Default dataset name is \'mc_import_dataset\'.')
    parser.add_argument('-t', metavar='BQ Table Prefix', required=False,
                        help='Biq Query Table Name. Default table prefix is \'mc_import_\'.')
    return parser.parse_args()


def main():
    args = parse_cli_args()

    mc_reports_directory = args.d
    enable_bq_import = args.b

    print("Migration Center Pricing Report to Google sheets, v0.1")

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
        print("Importing Migration Center data into Big Query...")

        if args.n is not None:
            bq_dataset_name = args.n
        else:
            bq_dataset_name = "mc_import_dataset"

        if args.t is not None:
            bq_table_prefix = args.t
        else:
            bq_table_prefix = "mc_import_"

        if args.k is not None:
            service_account_key = args.k
            print("Using Google Service Account key: " + service_account_key)
        else:
            service_account_key = ""

        print(f"BQ Dataset Name: {bq_dataset_name}")
        print(f"BQ Table Prefix: {bq_table_prefix}")

        import_mc_into_bq(mc_reports_directory, bq_dataset_name, bq_table_prefix, service_account_key)


if __name__ == "__main__":
    main()

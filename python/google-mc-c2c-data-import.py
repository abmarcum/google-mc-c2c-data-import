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
# Migration Pricing Reports C2C Data Import

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
import os
import json

version = "v0.2"
datetime = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
username = os.environ['USER']
if username == 'root':
    print("User root not allowed to run this application! Exiting...")
    exit()

# Order to sort worksheets and new names to use
f = open('settings.json', )
settings_file = json.load(f)
mc_names = settings_file["mc_names"]
mc_column_names = settings_file["mc_column_names"]
refresh_data_sources_body = settings_file["refresh_data_sources"]
f.close()

default_mc_looker_template_id = "421c8150-e7ad-4190-b044-6a18ecdbd391"
default_cur_looker_template_id = "c4e0ccbc-907a-4bc4-85f1-1711ee47c345"


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
                        total_cells) + ") and therefor cannot be imported through the Google Sheets API. Consider using the -b & -n argument to import into Big Query & Sheets instead.")
                    exit()
    else:
        print("No CSV files found in " + mc_reports_directory + "! Exiting!")
        exit()


# Create Initial Google Sheets
def create_google_sheets(customer_name, sheets_email_addresses, service_account_key, sheets_id):
    if sheets_id == "":
        # print("\nCreating new Google Sheets...")
        test = 0
    else:
        print("\nUpdating Google Sheets: " + sheets_id)

    scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
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


def generate_pie_table_request(spreadsheet, chart_title, ref_column, value_column, position_data):
    # Google Sheets Charts API: https://developers.google.com/sheets/api/samples/charts

    f = open('settings.json', )
    template_file = json.load(f)
    new_pie_chart_request = template_file["pie_chart_request"]
    f.close()

    new_pie_chart_request["requests"][0]["addChart"]["chart"]["spec"]["title"] = chart_title
    new_pie_chart_request["requests"][0]["addChart"]["chart"]["spec"]["pieChart"]["domain"]["sourceRange"]["sources"][
        0]["sheetId"] = spreadsheet
    new_pie_chart_request["requests"][0]["addChart"]["chart"]["spec"]["pieChart"]["domain"]["sourceRange"]["sources"][
        0]["startColumnIndex"] = ref_column
    new_pie_chart_request["requests"][0]["addChart"]["chart"]["spec"]["pieChart"]["domain"]["sourceRange"]["sources"][
        0]["endColumnIndex"] = ref_column + 1

    new_pie_chart_request["requests"][0]["addChart"]["chart"]["spec"]["pieChart"]["series"]["sourceRange"]["sources"][
        0]["sheetId"] = spreadsheet
    new_pie_chart_request["requests"][0]["addChart"]["chart"]["spec"]["pieChart"]["series"]["sourceRange"]["sources"][
        0]["startColumnIndex"] = value_column
    new_pie_chart_request["requests"][0]["addChart"]["chart"]["spec"]["pieChart"]["series"]["sourceRange"]["sources"][
        0]["endColumnIndex"] = value_column + 1

    new_pie_chart_request["requests"][0]["addChart"]["chart"]["position"]["overlayPosition"]["anchorCell"][
        "sheetId"] = spreadsheet
    new_pie_chart_request["requests"][0]["addChart"]["chart"]["position"]["overlayPosition"]["anchorCell"][
        "columnIndex"] = position_data[0]
    new_pie_chart_request["requests"][0]["addChart"]["chart"]["position"]["overlayPosition"]["anchorCell"]["rowIndex"] = \
        position_data[1]

    return new_pie_chart_request


def generate_repeat_cell_formula_request(sheet_id, formula, start_column, start_row):
    # "startColumnIndex": 2 and "endColumnIndex": 3 of range means the column "C".
    # "startRowIndex": 1 of range and no endRowIndex means that the formula is put from the row 2 to end of row.

    # Currently only support 1 column
    end_column = start_column + 1

    body = {
        "requests": [
            {
                "repeatCell": {
                    "cell": {
                        "userEnteredValue": {
                            "formulaValue": formula
                        }
                    },
                    "range": {
                        "sheetId": sheet_id,
                        "startColumnIndex": start_column,
                        "endColumnIndex": end_column,
                        "startRowIndex": start_row
                    },
                    "fields": "userEnteredValue.formulaValue"
                }
            }
        ]
    }
    return body


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


def apply_conditional_color_rule(sheet_id, grid_range, boolean_condition, boolean_value, colors):
    # Set up conditional rules (Red/Green) to GCP Overview Differences
    rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(grid_range, sheet_id)],
        booleanRule=BooleanRule(
            condition=BooleanCondition(boolean_condition, [boolean_value]),
            format=CellFormat(textFormat=textFormat(foregroundColor=Color(colors[0], colors[1], colors[2])))

        )
    )

    sheet_rules = get_conditional_format_rules(sheet_id)
    sheet_rules.append(rule)
    sheet_rules.save()


# Create API Request to Connect BQ Table to Google Sheets
def connect_bq_to_sheets(gcp_project_id, bq_dataset_name, bq_table):
    body = {
        "requests": [
            {
                "addDataSource": {
                    "dataSource": {
                        "spec": {
                            "bigQuery": {
                                "projectId": gcp_project_id,
                                "tableSpec": {
                                    "tableProjectId": gcp_project_id,
                                    "datasetId": bq_dataset_name,
                                    "tableId": bq_table
                                }
                            }
                        }
                    }
                }
            }
        ]
    }

    time.sleep(1)
    return body


# Create Pivot table with sums for Google Sheets
def generate_pivot_table_request(source, data_source, row_col, value_col, location_spreadsheet,
                                 pivot_table_location, summarize_function, row_name, row_col_2nd, row_name_2nd,
                                 value_name, value_col_2nd, value_name_2nd, filter_col, show_diff):
    # Google Sheets Pivot Table API: https://developers.google.com/sheets/api/samples/pivot-tables
    f = open('settings.json', )
    template_file = json.load(f)
    new_pivot_table_request = template_file["pivot_table_request"]
    f.close()

    if source == "BQ":
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
            "dataSourceId"] = data_source[0]
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
            "dataSourceId"] = data_source[0]

        # If defined, add a 2nd column source for the Pivot Table
        if row_col_2nd is not None:
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"]["dataSourceColumnReference"]["name"] = row_col

            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"] = []
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"].append(
            #     {})
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"].append(
            #     {})

            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
            #     "showTotals"] = False
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
            #     "sortOrder"] = "DESCENDING"
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
            #     "valueBucket"] = {}

            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
                "dataSourceColumnReference"] = {}
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
                "dataSourceColumnReference"]["name"] = row_col

            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"].append(
                {})
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "showTotals"] = False
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "sortOrder"] = "DESCENDING"
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "valueBucket"] = {}
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "dataSourceColumnReference"] = {}
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "dataSourceColumnReference"]["name"] = row_col_2nd
        else:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
                "dataSourceColumnReference"]["name"] = row_col

        # If defined, add a 2nd column values for the Pivot Table
        if value_col_2nd is not None:
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"] = []
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
            #     "values"].append({})
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
            #     "values"].append({})

            # 1st Values Column
            # new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][0][
            #     "summarizeFunction"] = "SUM"
            if value_name is not None:
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    0]["name"] = value_name
            else:
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    0]["name"] = "Total"

            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][0][
                "dataSourceColumnReference"] = {}
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][0][
                "dataSourceColumnReference"]["name"] = value_col

            # 2nd Values Column
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
                "values"].append({})
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][1][
                "summarizeFunction"] = "SUM"

            if value_name_2nd is not None:
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    1]["name"] = value_name_2nd
            else:
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    1]["name"] = "2nd Total"

            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][1][
                "dataSourceColumnReference"] = {}
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][1][
                "dataSourceColumnReference"]["name"] = value_col_2nd

            if show_diff is True:
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
                    "values"].append({})
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    2]["summarizeFunction"] = "SUM"
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    2]["name"] = "Cost Difference"
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    2]["formula"] = "=(GCP_Cost - Source_Cost)"

                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
                    "values"].append({})
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    3]["summarizeFunction"] = "SUM"
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    3]["name"] = "% Difference"
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    3]["formula"] = "=((GCP_Cost - Source_Cost) / Source_Cost)"

        else:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][0][
                "dataSourceColumnReference"]["name"] = value_col

            if show_diff is True:
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    1]["summarizeFunction"] = "SUM"
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    1]["name"] = "Cost Difference"
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    1]["formula"] = "=(GCP_Cost - Source_Cost)"

                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    2]["summarizeFunction"] = "SUM"
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    2]["name"] = "% Difference"
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    2]["formula"] = "=((GCP_Cost - Source_Cost) / Source_Cost)"

        if filter_col is None:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0][
                "dataSourceColumnReference"]["name"] = value_col
        else:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0][
                "dataSourceColumnReference"]["name"] = filter_col
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0][
                "filterCriteria"]["condition"]["type"] = "NOT_BLANK"
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0][
                "filterCriteria"]["condition"]["values"] = []

    if source == "SHEETS":
        # Clean up template table and remove BQ references
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"].pop("dataSourceId",
                                                                                                        None)
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"].pop("rows", None)

        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"].pop("values", None)

        # Create Filter for Pivot Table - default is to not show anything less than zero
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"].pop('filterSpecs',
                                                                                                        None)
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"] = []
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
            "filterSpecs"].append({})

        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][0][
            "filterCriteria"] = {}
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][0][
            "filterCriteria"]["visibleByDefault"] = True
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][0][
            "filterCriteria"]["condition"] = {}

        if filter_col is None:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0]["columnOffsetIndex"] = value_col
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0]["filterCriteria"]["condition"]["type"] = "NUMBER_GREATER"
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0]["filterCriteria"]["condition"]["values"] = []
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0]["filterCriteria"]["condition"]["values"].append({})
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0]["filterCriteria"]["condition"]["values"][0]["userEnteredValue"] = "0"
        else:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0]["columnOffsetIndex"] = filter_col
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["filterSpecs"][
                0]["filterCriteria"]["condition"]["type"] = "NOT_BLANK"

        # Change templated Pivot Table source to use cells from worksheet
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["source"] = {}
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["source"][
            "sheetId"] = data_source[0]
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["source"][
            "startRowIndex"] = 0
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["source"][
            "startColumnIndex"] = 0
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["source"][
            "endColumnIndex"] = data_source[1]
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["source"][
            "endRowIndex"] = data_source[2]

        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"] = []
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"].append({})

        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
            "sourceColumnOffset"] = row_col
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
            "showTotals"] = False
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
            "sortOrder"] = "DESCENDING"
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][0][
            "valueBucket"] = {}

        # If defined, add a 2nd column source for the Pivot Table
        if row_col_2nd is not None:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"].append(
                {})

            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "sourceColumnOffset"] = row_col_2nd
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "showTotals"] = False
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "sortOrder"] = "DESCENDING"
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["rows"][1][
                "valueBucket"] = {}

        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"] = []
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"].append({})

        if value_name is not None:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][0][
                "name"] = value_name

        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][0][
            "sourceColumnOffset"] = value_col
        new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][0][
            "summarizeFunction"] = summarize_function

        if value_col_2nd is not None:
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"][
                "values"].append({})

            if value_name_2nd is not None:
                new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][
                    1]["name"] = value_name_2nd

            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][1][
                "sourceColumnOffset"] = value_col_2nd
            new_pivot_table_request["requests"][0]["updateCells"]["rows"][0]["values"][0]["pivotTable"]["values"][1][
                "summarizeFunction"] = summarize_function

    new_pivot_table_request["requests"][0]["updateCells"]["start"]["sheetId"] = location_spreadsheet
    new_pivot_table_request["requests"][0]["updateCells"]["start"]["rowIndex"] = pivot_table_location[1]
    new_pivot_table_request["requests"][0]["updateCells"]["start"]["columnIndex"] = pivot_table_location[0]

    return new_pivot_table_request


def generate_mc_sheets(spreadsheet, worksheet_names, data_source_type, data_source_ids):
    exec_overview_worksheets_name = "Executive Overview"
    gcp_overview_worksheets_name = "GCP Detailed Overview"
    unmapped_worksheets_name = "AWS Unmapped Overview"

    unmapped_row_col_name = "lineItem_ProductCode"
    unmapped_value_col_name = "lineItem_UnblendedCost"

    # Create Executive Overview Worksheet in Sheets
    exec_overview_worksheet = spreadsheet.add_worksheet(exec_overview_worksheets_name, 60, 25)
    exec_overview_worksheet_id = exec_overview_worksheet._properties['sheetId']

    # Create GCP Overview Worksheet in Sheets
    gcp_overview_worksheet = spreadsheet.add_worksheet(gcp_overview_worksheets_name, 125, 25)
    gcp_overview_worksheet_id = gcp_overview_worksheet._properties['sheetId']

    # Create AWS Unmapped Worksheet in Sheets
    unmapped_worksheet = spreadsheet.add_worksheet(unmapped_worksheets_name, 60, 25)
    unmapped_worksheet_id = unmapped_worksheet._properties['sheetId']

    worksheet_names.append(exec_overview_worksheet)
    worksheet_names.append(gcp_overview_worksheet)
    worksheet_names.append(unmapped_worksheet)

    exec_overview_worksheet.batch_update([{
        'range': "A1:A3",
        'values': [["AWS Spend (GCP Matched)"], ["AWS Spend (Unmatched)"], ["AWS Total Spend"]],
    }, {
        'range': "B1:B3",
        'values': [["=SUM(E2:E)"], ["=SUM(calcctl_unmapped!lineItem_UnblendedCost)"], ["=B1+B2"]],
    }]
        , value_input_option="USER_ENTERED"
    )

    exec_overview_worksheet.batch_update([{
        'range': "A5:A7",
        'values': [["GCP Spend (AWS Matched)"], ["GCP Difference"], ["GCP % Difference"]],
    }, {
        'range': "B5:B7",
        'values': [["=SUM(F2:F)"], ["=B5-B1"], ["=B6/B1"]],
    }]
        , value_input_option="USER_ENTERED"
    )

    exec_overview_worksheet.batch_update([{
        'range': "G1:I1",
        'values': [
            ["% of AWS Total Spend", "GCP Cost Difference", "% GCP Difference"]],
    }]
    )

    # Insert Formulas off of pivot tables for above columns

    # % of AWS Total Spend
    response = spreadsheet.batch_update(
        generate_repeat_cell_formula_request(exec_overview_worksheet_id, "=IF(ISBLANK($E2), \"\", $E2/$B$3)", 6, 1)
    )

    # Exec Overview - GCP Cost Difference
    response = spreadsheet.batch_update(
        generate_repeat_cell_formula_request(exec_overview_worksheet_id, "=IF(ISBLANK($E2), \"\", $F2-$E2)", 7, 1)
    )

    # Exec Overview - % GCP Difference
    response = spreadsheet.batch_update(
        generate_repeat_cell_formula_request(exec_overview_worksheet_id,
                                             "=IF(ISBLANK($E2), \"\", if(F2<=0,\"No Google Cost\",($F2-$E2)/$E2))", 8,
                                             1)
    )

    gcp_overview_worksheet.batch_update([{
        'range': "E1:F1",
        'values': [
            ["GCP Cost Difference", "% GCP Difference"]],
    }]
    )

    # GCP Details - GCP Cost Difference
    response = spreadsheet.batch_update(
        generate_repeat_cell_formula_request(gcp_overview_worksheet_id, "=IF(ISBLANK($C2), \"\", $D2-$C2)", 4, 1)
    )

    # GCP Details - % GCP Difference
    response = spreadsheet.batch_update(
        generate_repeat_cell_formula_request(gcp_overview_worksheet_id,
                                             "=IF(ISBLANK($C2), \"\", if(D2<=0,\"No Google Cost\",($D2-$C2)/$C2))", 5,
                                             1)
    )

    # Add Cost sums to Overview Worksheet. Filter on GCP Cost column being greater than 0.
    pivot_table_location = [
        0,  # Column A
        0  # Row 1
    ]

    if data_source_type == "BQ":
        data_source = [data_source_ids[0]]

    data_row_col_name = "Source_Product"
    data_value_col_name = "Source_Cost"
    data_value_2nd_col_name = "GCP_Cost"
    value_name = "AWS Cost"
    value_name_2nd = "GCP Cost"

    response = spreadsheet.batch_update(
        generate_pivot_table_request(data_source_type, data_source, data_row_col_name, data_value_col_name,
                                     gcp_overview_worksheet_id,
                                     pivot_table_location, "SUM", None, "GCP_Service", None, value_name,
                                     data_value_2nd_col_name,
                                     value_name_2nd, "Source_Cost", False
                                     ),

    )

    pivot_table_location = [
        7,  # Column H
        0  # Row 1
    ]

    data_row_col_name = "Region"
    data_value_col_name = "GCP_Cost"

    # AWS Region Cost
    response = spreadsheet.batch_update(
        generate_pivot_table_request(data_source_type, data_source, data_row_col_name, data_value_col_name,
                                     gcp_overview_worksheet_id,
                                     pivot_table_location, "SUM", None, "GCP_Service", None, None, None, None, "Region",
                                     False
                                     ))

    # Add Instance Cost to Overview Worksheet. Filter on Destination_Shape column being not None.
    pivot_table_location = [
        11,  # Column L
        0  # Row 1
    ]

    data_row_col_name = "Region"
    data_value_col_name = "GCP_Cost"
    filter_col = "Destination_Shape"

    response = spreadsheet.batch_update(
        generate_pivot_table_request(data_source_type, data_source, data_row_col_name, data_value_col_name,
                                     gcp_overview_worksheet_id,
                                     pivot_table_location, "SUM", None, "Destination_Shape", None, None, None, None,
                                     filter_col, False
                                     ))

    # Add Cost sums to AWS Unmapped Worksheet. Filter on AWS Cost column being greater than 0.
    if data_source_type == "BQ":
        data_source = [data_source_ids[1]]

    pivot_table_location = [
        0,  # Column D
        0  # Row 1
    ]

    response = spreadsheet.batch_update(
        generate_pivot_table_request(data_source_type, data_source, unmapped_row_col_name, unmapped_value_col_name,
                                     unmapped_worksheet_id,
                                     pivot_table_location, "SUM", None, None, None, None, None, None, None, False
                                     ))

    # Add Instance Region Usage Breakdown.
    unmapped_row_col_name = "lineItem_ProductCode"
    unmapped_value_col_name = "lineItem_UnblendedCost"
    pivot_table_location = [
        3,  # Column D
        0  # Row 1
    ]

    response = spreadsheet.batch_update(
        generate_pivot_table_request(data_source_type, data_source, unmapped_row_col_name, unmapped_value_col_name,
                                     unmapped_worksheet_id,
                                     pivot_table_location, "SUM", None, "lineItem_UsageType", None, None, None, None,
                                     None, False
                                     ))

    # Exec Overview Pivot Table
    pivot_table_location = [
        3,  # Column D
        0  # Row 1
    ]

    if data_source_type == "BQ":
        data_source = [data_source_ids[0]]

    data_row_col_name = "Source_Product"
    data_value_col_name = "Source_Cost"
    data_value_2nd_col_name = "GCP_Cost"
    value_name = "AWS Cost"
    value_name_2nd = "GCP Cost"

    response = spreadsheet.batch_update(
        generate_pivot_table_request(data_source_type, data_source, data_row_col_name, data_value_col_name,
                                     exec_overview_worksheet_id,
                                     pivot_table_location, "SUM", None, None, None, value_name,
                                     data_value_2nd_col_name,
                                     value_name_2nd, "Source_Cost", False
                                     ),

    )

    # Add Piechart for GCP Services
    chart_title = "GCP Services Breakdown"
    piechart_row_col = 1  # Column A
    piechart_value_col = 2  # Column B
    position_data = [
        15,  # Column P
        0  # Row 1
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(gcp_overview_worksheet_id, chart_title, piechart_row_col, piechart_value_col,
                                   position_data))

    # Add Piechart for GCP Services
    chart_title = "GCP Regions Breakdown"
    piechart_row_col = 7  # Column H
    piechart_value_col = 9  # Column J
    position_data = [
        15,  # Column P
        21  # Row 1
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(gcp_overview_worksheet_id, chart_title, piechart_row_col, piechart_value_col,
                                   position_data))

    # Add Piechart for Instances
    chart_title = "GCP Instance Breakdown"
    piechart_row_col = 12  # Column M
    piechart_value_col = 13  # Column N
    position_data = [
        15,  # Column P
        42  # Row 20
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(gcp_overview_worksheet_id, chart_title, piechart_row_col, piechart_value_col,
                                   position_data))

    # Add Piechart for AWS Unmapped Services
    chart_title = "AWS Unmapped Services Breakdown"
    piechart_row_col = 0
    piechart_value_col = 1
    position_data = [
        7,  # Column H
        0  # Row 1
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(unmapped_worksheet_id, chart_title, piechart_row_col, piechart_value_col,
                                   position_data))

    # Set Exec Overview AWS Titles to Bold
    exec_overview_worksheet.format("A1:A7", {
        "textFormat": {"bold": True}
    })

    # Set Exec Overview AWS Unmapped to Italic
    exec_overview_worksheet.format("B2", {
        "textFormat": {"italic": True}
    })

    # Set Exec Overview Formulas Titles to Bold
    exec_overview_worksheet.format("G1:M1", {
        "textFormat": {"bold": True}
    })

    # Set Overview Formulas Totals to Percent
    exec_overview_worksheet.format("G", {
        "numberFormat": {"type": "PERCENT", "pattern": "0.0000%"}
    })

    # Set Overview Formulas Totals to Currency
    exec_overview_worksheet.format("H", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Set Overview Formulas Totals to Percent
    exec_overview_worksheet.format("I", {
        "numberFormat": {"type": "PERCENT", "pattern": "0.0000%"}
    })

    # Set Overview Cost Totals to Currency
    exec_overview_worksheet.format("B", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Set Overview Cost Totals to Currency
    exec_overview_worksheet.format("E:F", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Set Overview Cost Totals to Currency
    exec_overview_worksheet.format("B7", {
        "numberFormat": {"type": "PERCENT", "pattern": "0.0000%"}
    })

    gcp_overview_worksheet.format("E1:F1", {
        "textFormat": {"bold": True}
    })

    # Change Overview Cost Totals to Currency format
    gcp_overview_worksheet.format("C:E", {
        "numberFormat": {"type": "CURRENCY"}
    })

    gcp_overview_worksheet.format("F", {
        "numberFormat": {"type": "PERCENT", "pattern": "0.0000%"}
    })

    # Change Region Cost Totals to Currency format
    gcp_overview_worksheet.format("J", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Instance Cost Totals to Currency format
    gcp_overview_worksheet.format("N", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Unmapped Cost Totals to Currency format
    unmapped_worksheet.format("B", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Unmapped Services Details Totals to Currency format
    unmapped_worksheet.format("F", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Set up conditional rules (Red/Green) to GCP Overview Differences
    apply_conditional_color_rule(gcp_overview_worksheet, "E2:F", "NUMBER_GREATER", "0", [1, 0, 0])
    apply_conditional_color_rule(gcp_overview_worksheet, "E2:F", "NUMBER_LESS", "0", [0, 75, 0])

    # Set up conditional rules (Red/Green) to Exec Overview Differences
    apply_conditional_color_rule(exec_overview_worksheet, "B6", "NUMBER_GREATER", "0", [1, 0, 0])
    apply_conditional_color_rule(exec_overview_worksheet, "B6", "NUMBER_LESS", "0", [0, 75, 0])

    # Set up conditional rules (Red/Green) to Exec Overview Differences
    apply_conditional_color_rule(exec_overview_worksheet, "H:I", "NUMBER_GREATER", "0", [1, 0, 0])
    apply_conditional_color_rule(exec_overview_worksheet, "H:I", "NUMBER_LESS", "0", [0, 75, 0])

    # Autosize first cols in Overview worksheet
    first_col = 0
    last_col = 30
    res = spreadsheet.batch_update(autosize_worksheet(gcp_overview_worksheet_id, first_col, last_col))

    # Autosize first cols in Unmapped worksheet
    res = spreadsheet.batch_update(autosize_worksheet(unmapped_worksheet_id, first_col, last_col))

    # Refresh all BQ Data sources (removes 'Apply' button from pivot tables)
    res = spreadsheet.batch_update(refresh_data_sources_body)


def generate_bq_cur_sheets(spreadsheet, worksheet_names, data_source_ids):
    overview_worksheets_name = "AWS Overview"
    details_worksheets_name = "AWS Details"
    overview_row_col_name = "lineItem_ProductCode"
    overview_value_col_name = "lineItem_UnblendedCost"

    # Create Overview Worksheet in Sheets
    overview_worksheet = spreadsheet.add_worksheet(overview_worksheets_name, 60, 25)
    details_worksheet = spreadsheet.add_worksheet(details_worksheets_name, 60, 25)

    overview_worksheet_id = overview_worksheet._properties['sheetId']
    details_worksheet_id = details_worksheet._properties['sheetId']

    source = "BQ"
    data_source = [data_source_ids[0]]

    pivot_table_location = [
        2,  # Column C
        0  # Row 1
    ]

    # AWS Services Cost
    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, overview_row_col_name, overview_value_col_name,
                                     overview_worksheet_id,
                                     pivot_table_location, "SUM", None, None, None, None, None, None, None, False
                                     ))

    pivot_table_location = [
        5,  # Column F
        0  # Row 1
    ]

    overview_row_col_name = "product_region"
    overview_value_col_name = "lineItem_UnblendedCost"
    # AWS Region Cost
    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, overview_row_col_name, overview_value_col_name,
                                     overview_worksheet_id,
                                     pivot_table_location, "SUM", None, None, None, None, None, None, None, False
                                     ))

    pivot_table_location = [
        8,  # Column F
        0  # Row 1
    ]

    overview_row_col_name = "product_instanceType"
    overview_value_col_name = "lineItem_UnblendedCost"
    # AWS Instance Cost
    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, overview_row_col_name, overview_value_col_name,
                                     overview_worksheet_id,
                                     pivot_table_location, "SUM", None, None, None, None, None, None,
                                     "product_instanceType", False
                                     ))

    pivot_table_location = [
        0,  # Column A
        0  # Row 1
    ]

    details_row_col_name = "lineItem_ProductCode"
    details_value_col_name = "lineItem_UnblendedCost"
    # AWS Services Details Cost
    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, details_row_col_name, details_value_col_name,
                                     details_worksheet_id,
                                     pivot_table_location, "SUM", None, "lineItem_UsageType", None, None, None, None,
                                     None, False
                                     ))

    pivot_table_location = [
        4,  # Column E
        0  # Row 1
    ]

    details_row_col_name = "product_region"
    details_value_col_name = "lineItem_UnblendedCost"
    # AWS Regions Details Cost
    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, details_row_col_name, details_value_col_name,
                                     details_worksheet_id,
                                     pivot_table_location, "SUM", None, "product_instanceType", None, None, None, None,
                                     None, False
                                     ))

    # Add Piechart for AWS Services
    chart_title = "AWS Services Breakdown"

    position_data = [
        11,  # Column J
        0  # Row 1
    ]

    res = spreadsheet.batch_update(generate_pie_table_request(overview_worksheet_id, chart_title, 2, 3, position_data))

    # Add Piechart for AWS Regions
    chart_title = "AWS Regions Breakdown"

    position_data = [
        11,  # Column J
        21  # Row 21
    ]

    res = spreadsheet.batch_update(generate_pie_table_request(overview_worksheet_id, chart_title, 5, 6, position_data))

    # Add Piechart for AWS Instances
    chart_title = "AWS Instance Breakdown"

    position_data = [
        11,  # Column J
        42  # Row 42
    ]

    res = spreadsheet.batch_update(generate_pie_table_request(overview_worksheet_id, chart_title, 8, 9, position_data))

    # Add Piechart for AWS Detailed Instances
    chart_title = "AWS Region Instance Breakdown"

    position_data = [
        8,  # Column I
        0  # Row 1
    ]

    res = spreadsheet.batch_update(generate_pie_table_request(details_worksheet_id, chart_title, 4, 6, position_data))

    overview_worksheet.batch_update([{
        'range': "A1",
        'values': [["AWS Total Cost"]],
    }, {
        'range': "A2",
        'values': [["=SUM(D2:D)"]],
    }]
        , value_input_option="USER_ENTERED"
    )

    # Set AWS Cost Totals to Bold
    overview_worksheet.format("A1", {
        "textFormat": {"bold": True}
    })

    # Set AWS Cost Totals to Currency
    overview_worksheet.format("A2", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Services Cost Totals to Currency format
    overview_worksheet.format("D", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Region Cost Totals to Currency format
    overview_worksheet.format("G", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Instance Cost Totals to Currency format
    overview_worksheet.format("J", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Set AWS Cost Totals to Currency
    details_worksheet.format("C", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Set AWS Region Totals to Currency
    details_worksheet.format("G", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Autosize first cols in Overview worksheet
    first_col = 0
    last_col = 30
    res = spreadsheet.batch_update(autosize_worksheet(overview_worksheet_id, first_col, last_col))

    # Refresh all BQ Data sources (removes 'Apply' button from pivot tables)
    res = spreadsheet.batch_update(refresh_data_sources_body)


# Import mc data from provided reports directory
def import_mc_data(mc_reports_directory, spreadsheet, credentials):
    sheets_id = spreadsheet.id
    mc_data = {}
    # Grabbing a list of files from the provided mc directory
    try:
        mc_file_list = os.listdir(mc_reports_directory)
        print("Importing pricing report data...")
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
    overview_worksheet = spreadsheet.add_worksheet(overview_worksheets_name, 60, 25)
    overview_worksheet_id = overview_worksheet._properties['sheetId']
    mc_names_list.append(overview_worksheet)

    # Create Unmapped Overview Sheets Pivot Table
    unmapped_overview_worksheets_name = "Unmapped Overview"
    unmapped_overview_worksheet = spreadsheet.add_worksheet(unmapped_overview_worksheets_name, 60, 25)
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
        last_col = 20
        res = spreadsheet.batch_update(autosize_worksheet(worksheet_id, first_col, last_col))

    # mapped.csv Data Information
    mapped_worksheet_id = spreadsheet.worksheet(mc_names["mapped"])
    mapped_csv_header_length = 25 + 1
    mapped_csv_num_rows = len(mc_data["mapped"]) + 1

    # unmapped.csv Data Information
    unmapped_worksheet_id = spreadsheet.worksheet(mc_names["unmapped"])
    unmapped_csv_header_length = 25 + 1
    unmapped_csv_num_rows = len(mc_data["unmapped"]) + 1

    # Create pivot table of GCP Services & Total cost for each. Filter on values_column_offset/GCP Cost being greater than 0.
    rows_column_offset = 5  # Data, Column F
    values_column_offset = 23  # Data, Column X, GCP Cost
    values_column_offset_2nd = 19  # Data, Column T, AWS Cost
    location_data = [4, 0]  # Cell: Column E, Row 1

    source = "SHEETS"
    data_source = [mapped_worksheet_id.id, mapped_csv_header_length, mapped_csv_num_rows]

    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, rows_column_offset, values_column_offset,
                                     overview_worksheet_id,
                                     location_data, "SUM", None, None, None, "GCP Cost", values_column_offset_2nd,
                                     "AWS Cost", None, False
                                     ))

    # Create pivot table of GCP Regions & Total cost for each. Filter on values_column_offset/GCP Cost being greater than 0.
    rows_column_offset = 7  # Data, Column H, Region
    values_column_offset = 23  # Data, Column X, GCP Cost
    location_data = [8, 0]  # Cell: Column I, Row 1

    source = "SHEETS"
    data_source = [mapped_worksheet_id.id, mapped_csv_header_length, mapped_csv_num_rows]

    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, rows_column_offset, values_column_offset,
                                     overview_worksheet_id,
                                     location_data, "SUM", None, None, None, "GCP Cost", None,
                                     None, None, False
                                     ))

    # Create pivot table of GCP Machine types & totals for each. Filter on rows_column_offset/Destination Shape not being None
    rows_column_offset = 10  # Data, Column K
    values_column_offset = 23  # Data, Column K
    location_data = [11, 0]  # Cell: Column L, Row 1

    data_source = [mapped_worksheet_id.id, mapped_csv_header_length, mapped_csv_num_rows]

    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, rows_column_offset, values_column_offset,
                                     overview_worksheet_id,
                                     location_data, "SUM", None, None, None, "GCP Cost", None,
                                     None, rows_column_offset, False
                                     ))

    # Add Piechart of GCP Machine types in GCP Overview
    chart_title = "GCP Services Breakdown"
    first_column = 4  # Col E
    second_column = 5  # Col F

    position_data = [
        14,  # Column O
        0  # Row 1
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(overview_worksheet_id, chart_title, first_column, second_column, position_data))

    # Add Piechart of GCP Regions
    chart_title = "GCP Regions Breakdown"
    first_column = 8  # Col I
    second_column = 9  # Col J

    position_data = [
        14,  # Column O
        21  # Row 1
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(overview_worksheet_id, chart_title, first_column, second_column, position_data))

    # Add Piechart for Instances
    chart_title = "GCP Instances Breakdown"
    first_column = 11  # Col L
    second_column = 12  # Col M

    position_data = [
        14,  # Column O
        42  # Row 20
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(overview_worksheet_id, chart_title, first_column, second_column, position_data))

    # Create pivot table of Unmapped Services & Total cost for each. Filter on values_column_offset/AWS unblended Cost being greater than 0.
    rows_column_offset = 2  # Data, Column C
    values_column_offset = 9  # Data, Column V
    position_data = [
        0,  # Column A
        0  # Row 1
    ]

    data_source = [unmapped_worksheet_id.id, unmapped_csv_header_length, unmapped_csv_num_rows]

    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, rows_column_offset, values_column_offset,
                                     unmapped_overview_worksheet_id,
                                     position_data, "SUM", None, None, None, "AWS Unmapped Cost", None,
                                     None, None, False
                                     ))

    # Create pivot table of Unmapped Services, Products, & Total cost for each.
    rows_column_offset = 2  # Data, Column C
    values_column_offset = 9  # Data, Column V
    position_data = [
        3,  # Column D
        0  # Row 1
    ]
    data_source = [unmapped_worksheet_id.id, unmapped_csv_header_length, unmapped_csv_num_rows]

    response = spreadsheet.batch_update(
        generate_pivot_table_request(source, data_source, rows_column_offset, values_column_offset,
                                     unmapped_overview_worksheet_id,
                                     position_data, "SUM", None, 4, None, "AWS Unmapped Cost", None,
                                     None, None, False
                                     ))

    # Add Pie chart of Unmapped Services Unmapped Overview
    chart_title = "Unmapped Services Breakdown"
    first_column = 0  # Col A
    second_column = 1  # Col B

    position_data = [
        7,  # Column H
        0  # Row 1
    ]

    res = spreadsheet.batch_update(
        generate_pie_table_request(unmapped_overview_worksheet.id, chart_title, first_column, second_column,
                                   position_data))
    # Autosize first cols in GCP Overview worksheet
    first_col = 0
    last_col = 30
    res = spreadsheet.batch_update(autosize_worksheet(overview_worksheet_id, first_col, last_col))

    overview_worksheet.batch_update([{
        'range': "A1:C1",
        'values': [["GCP Total Cost", "AWS Equivalent Cost", "AWS Unmapped Cost"]],
    }, {
        'range': "A2:C2",
        'values': [["=SUM(F2:F)", "=SUM(G2:G)", "=SUM('Unmapped Overview'!B2:B)"]],
    }]
        , value_input_option="USER_ENTERED"
    )

    # Set Overview Cost Totals to Bold
    overview_worksheet.format("A1:C1", {
        "textFormat": {"bold": True}
    })

    # Set Overview Cost Totals to Currency
    overview_worksheet.format("A2:C2", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Overview Cost Totals to Currency format
    overview_worksheet.format("F", {
        "numberFormat": {"type": "CURRENCY"}
    })

    overview_worksheet.format("G", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Overview Cost Totals to Currency format
    overview_worksheet.format("J", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Overview Cost Totals to Currency format
    overview_worksheet.format("M", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Unmapped Cost Totals to Currency format
    unmapped_overview_worksheet.format("B", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Change Unmapped Cost Totals to Currency format
    unmapped_overview_worksheet.format("F", {
        "numberFormat": {"type": "CURRENCY"}
    })

    # Autosize first cols in Unmapped Overview worksheet
    first_col = 0
    last_col = 30
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


def import_mc_into_bq(mc_reports_directory, gcp_project_id, bq_dataset_name, bq_table_prefix, service_account_key,
                      customer_name, display_looker, looker_template_id):
    # GCP Scope for auth
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    # Google Auth
    credentials = google_auth(service_account_key, scope)
    client = gspread.authorize(credentials)

    mc_data = {}
    mc_file_list = []
    # Grabbing a list of files from the provided mc directory
    try:
        print("Importing pricing report files...")
        for file in settings_file["mc_names"].keys():
            if os.path.isfile(f"{mc_reports_directory}{file}.csv"):
                mc_file_list.append(f"{file}")
        # mc_file_list = os.listdir(f"{mc_reports_directory}/*.csv")
    except:
        print("Unable to access directory: " + mc_reports_directory)
        exit()

    # Verify MC files exist
    if len(mc_file_list) < len(settings_file["mc_names"].keys()):
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
        with open(f"{mc_reports_directory}{file}.csv", "rb") as f:
            num_lines = sum(1 for _ in f)
        if num_lines > 1:
            bq_table_name = (f"{bq_table_prefix}{file.replace('.csv', '')}")
            table_id = (f"{gcp_project_id}.{bq_dataset_name}.{bq_table_name}")
            print(f"Importing {file}.csv into BQ Table: {table_id}")
            set_gcp_project = f"gcloud config set project {gcp_project_id} >/dev/null 2>&1"

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

            sheet_name = mc_names[file]
            mc_data[file] = pd.read_csv(file_fullpath, low_memory=False)
            # Replacing column names since BQ doesn't like them with () & the python library "column character map" version doesn't appear to work.

            # Ensure the various MC & calctl versions have the same column names
            if file == 'mapped':
                mc_data[file].rename(columns={
                    "Memory (GB)": "Memory_GB",
                    "External Memory (GB)": "External_Memory_GB",
                    "Sub-Type 1": "Sub_Type_1",
                    "Sub-Type 2": "Sub_Type_2",
                    "Dest Series": "Destination_Series",
                    "Extended Memory GB": "External_Memory_GB",
                    "Dest Shape": "Destination_Shape",
                    "OS or Licenses Cost": "OS_Licenses_Cost",
                    "Dest. Shape": "Destination_Shape",
                    "Dest. Series": "Destination_Series",
                    "OS / Licenses Cost": "OS_Licenses_Cost",
                    "Account/Subscription": "Account_Or_Subscription",
                    "Ext. Memory (GB)": "External_Memory_GB"
                }, inplace=True)

            # Ensure no spaces exist in any column names
            mc_data[file].rename(columns=lambda x: x.replace(" ", "_"), inplace=True)

            # More ensuring the various MC & calctl versions have the same column names
            mc_data[file].rename(columns=lambda x: x.replace("product_", "lineItem_"), inplace=True)

            schema = []
            # Create Schema Fields for BQ
            for column in mc_column_names[file].keys():
                if mc_column_names[file][column] == 'STRING':
                    schema.append(bigquery.SchemaField(column, bigquery.enums.SqlTypeNames.STRING))
                elif mc_column_names[file][column] == 'FLOAT64':
                    schema.append(bigquery.SchemaField(column, bigquery.enums.SqlTypeNames.FLOAT64))

                # col_count += 1

            job_config = bigquery.LoadJobConfig(

                autodetect=True,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                column_name_character_map="V2",
                allow_quoted_newlines=True,
                schema=schema,
                source_format=bigquery.SourceFormat.CSV
            )

            job = client.load_table_from_dataframe(
                mc_data[file], table_id, job_config=job_config
            )  # Make an API request.
            job.result()  # Wait for the job to complete.

            mc_data[file] = mc_data[file].iloc[0:0]

            table = client.get_table(table_id)  # Make an API request.
            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )
        else:
            print(f"Skipping {file}.csv since there is no Migration Center data in the file.")

    print("Completed loading of Migration Center Data into Big Query.")

    if display_looker is True:
        looker_report_url = create_looker_url("MC", customer_name, datetime, gcp_project_id, bq_dataset_name,
                                              bq_table_prefix)

        print(f"Looker URL: {looker_report_url}")


def import_cur_into_bq(mc_reports_directory, gcp_project_id, bq_dataset_name, bq_table, service_account_key,
                       customer_name, display_looker, looker_template_id):
    # GCP Scope for auth
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    #Google Auth
    credentials = google_auth(service_account_key, scope)
    client = gspread.authorize(credentials)

    cur_data = {}
    cur_file_list = [f for f in os.listdir(mc_reports_directory) if
                     os.path.isfile(os.path.join(mc_reports_directory, f))]

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

    table_id = (f"{gcp_project_id}.{bq_dataset_name}.{bq_table}")
    # Deleting table first if exists

    client.delete_table(table_id, not_found_ok=True)

    # Importing all CSV files into a dictionary of dataframes
    for file in cur_file_list:
        with open(f"{mc_reports_directory}{file}", "rb") as f:
            num_lines = sum(1 for _ in f)
        if num_lines > 1:
            print(f"Importing {file} into BQ Table: {table_id}")
            set_gcp_project = f"gcloud config set project {gcp_project_id} >/dev/null 2>&1"

            # schema = ""
            # for column in mc_column_names[file].keys():
            #     schema = schema + f"\"{column}\":{mc_column_names[file][column]},"
            #
            # # Remove last comma
            # schema = schema[:-1]
            try:
                os.system(set_gcp_project)
            except Exception as e:
                print(f"error: {e}")

            # if file.endswith(".csv"):
            file_fullpath = (f"{mc_reports_directory}{file}")

            cur_data[file] = pd.read_csv(file_fullpath, low_memory=False)

            # Ensure no spaces exist in any column names
            cur_data[file].rename(columns=lambda x: x.replace(" ", "_"), inplace=True)
            cur_data[file].rename(columns=lambda x: x.replace("/", "_"), inplace=True)

            job_config = bigquery.LoadJobConfig(

                autodetect=True,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                column_name_character_map="V2",
                allow_quoted_newlines=True,
                #schema=schema,
                source_format=bigquery.SourceFormat.CSV
            )

            job = client.load_table_from_dataframe(
                cur_data[file], table_id, job_config=job_config
            )  # Make an API request.
            job.result()  # Wait for the job to complete.

            cur_data[file] = cur_data[file].iloc[0:0]

            table = client.get_table(table_id)  # Make an API request.
            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )
        else:
            print(f"Skipping {file} since there is no data in the file.")

    print("Completed loading of AWS CUR Data into Big Query.\n")

    if display_looker is True:
        looker_report_url = create_looker_url("CUR", customer_name, datetime, gcp_project_id, bq_dataset_name, bq_table)

        print(f"Looker URL: {looker_report_url}")


def create_looker_url(looker_template, customer_name, datetime, gcp_project_id, bq_dataset_name, bq_table):
    # Looker Settings
    looker_url_prefix = "https://lookerstudio.google.com/reporting/create?c.reportId="
    looker_report_name = f"AWS -> GCP Pricing Analysis: {customer_name}, {datetime}"
    looker_report_name = urllib.parse.quote_plus(looker_report_name)

    if looker_template == 'MC':
        looker_template_id = default_mc_looker_template_id
        ds0_bq_datasource_name = "mapped"
        ds0_bq_table = f"{bq_table}mapped"

        ds1_bq_datasource_name = "unmapped"
        ds1_bq_table = f"{bq_table}unmapped"

        looker_report_url = f"{looker_url_prefix}{looker_template_id}&r.reportName={looker_report_name}&ds.ds0.connector=bigQuery&ds.ds0.datasourceName={ds0_bq_datasource_name}&ds.ds0.projectId={gcp_project_id}&ds.ds0.type=TABLE&ds.ds0.datasetId={bq_dataset_name}&ds.ds0.tableId={ds0_bq_table}&ds.ds1.connector=bigQuery&ds.ds1.datasourceName={ds1_bq_datasource_name}&ds.ds1.projectId={gcp_project_id}&ds.ds1.type=TABLE&ds.ds1.datasetId={bq_dataset_name}&ds.ds1.tableId={ds1_bq_table}"

    if looker_template == 'CUR':
        looker_template_id = default_cur_looker_template_id
        ds0_bq_datasource_name = "cur"
        ds0_bq_table = f"{bq_table}"

        looker_report_url = f"{looker_url_prefix}{looker_template_id}&r.reportName={looker_report_name}&ds.ds0.connector=bigQuery&ds.ds0.datasourceName={ds0_bq_datasource_name}&ds.ds0.projectId={gcp_project_id}&ds.ds0.type=TABLE&ds.ds0.datasetId={bq_dataset_name}&ds.ds0.tableId={ds0_bq_table}"

    return looker_report_url


# Parse CLI Arguments
def parse_cli_args():
    parser = argparse.ArgumentParser(prog='google-mc-c2c-data-import.py',
                                     usage='%(prog)s -d <mc report directory>\nThis creates an instance mapping between cloud providers and GCP')
    parser.add_argument('-d', metavar='Data Directory',
                        help='Directory containing MC report output or AWS CUR data.',
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
                        help='Import Migration Center data files into Biq Query Dataset.\nGCP BQ API must be enabled! ')
    parser.add_argument('-a', action='store_true', required=False,
                        help='Import AWS CUR file into Biq Query Dataset.\nGCP BQ API must be enabled! ')
    parser.add_argument('-l', action='store_true', required=False,
                        help='Display Looker Report URL. Migration Center or AWS CUR BQ Import must be enabled! ')
    parser.add_argument('-r', metavar='Looker Templ ID', required=False,
                        help='Replaces Default Looker Report Template ID')
    parser.add_argument('-n', action='store_true', required=False,
                        help='Create a Google Connected Sheets to newly created Big Query')
    parser.add_argument('-o', action='store_true', required=False,
                        help='Do not import to BQ, use an existing BQ instance (-i) and only create connected Sheets & Looker artifacts.')
    parser.add_argument('-i', metavar='BQ Connect Info', required=False,
                        help='BQ Connection Info: Format is <GCP Project ID>.<BQ Dataset Name>.<BQ Table Prefix>, i.e. googleproject.bqdataset.bqtable_prefix')
    return parser.parse_args()


def main():
    args = parse_cli_args()

    enable_cur_import = args.a
    enable_bq_import = args.b
    mc_reports_directory = args.d
    display_looker = args.l
    connect_sheets_bq = args.n
    sheets_emails = args.e
    do_not_import_data = args.o
    bq_connection_info = args.i

    if args.r is not None:
        looker_template_id = args.r
    else:
        if args.b is True:
            looker_template_id = default_mc_looker_template_id
        if args.a is True:
            looker_template_id = default_cur_looker_template_id

    print(f"Migration Center C2C Data Import, {version}")

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

    if connect_sheets_bq is True and (
            enable_bq_import is False and enable_cur_import is False and do_not_import_data is False):
        print("Must enable Big Query with -b or -a before creating a Connected BQ Google Sheets!")
        exit()

    if enable_bq_import is not True and enable_cur_import is not True and do_not_import_data is not True:

        check_csv_size(mc_reports_directory)

        if sheets_emails is not None:
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

        spreadsheet_url = 'https://docs.google.com/spreadsheets/d/%s' % spreadsheet.id

        print("Migration Center Pricing Report for " + customer_name + ": " + spreadsheet_url)
    else:
        if bq_connection_info is None:
            print("No Big Query connection information provided. Exiting!")
            exit()

        bq_connection_info = bq_connection_info

        (gcp_project_id, bq_dataset_name, bq_table_prefix) = bq_connection_info.split(".")

        bq_tables = []

        if enable_cur_import is True:
            print(f"BQ Table: {bq_table_prefix}")
            bq_tables.append(bq_table_prefix)
            overview_worksheets_name = "AWS Overview"
        else:
            print(f"BQ Table Prefix: {bq_table_prefix}")
            for table in list(mc_names.keys()):
                bq_tables.append(f'{bq_table_prefix}{table}')

        if do_not_import_data is False:
            print("Importing data into Big Query...")
            print(f"GCP Project ID: {gcp_project_id}")
            print(f"BQ Dataset Name: {bq_dataset_name}")

            if args.k is not None:
                service_account_key = args.k
                print("Using Google Service Account key: " + service_account_key)
            else:
                service_account_key = ""

            if args.c is not None:
                customer_name = args.c
            else:
                customer_name = "No Name Customer, Inc."

            if enable_bq_import is True and enable_cur_import is False:
                print("Migration Center Data import...")
                import_mc_into_bq(mc_reports_directory, gcp_project_id, bq_dataset_name, bq_table_prefix,
                                  service_account_key, customer_name, display_looker, looker_template_id)

            if enable_bq_import is True and enable_cur_import is True:
                print("Unable to import Migration Center & AWS CUR data at the same time. Please do each separately.")
                exit()

            if enable_cur_import is True and enable_bq_import is False:
                print("AWS CUR import...")
                import_cur_into_bq(mc_reports_directory, gcp_project_id, bq_dataset_name, bq_table_prefix,
                                   service_account_key,
                                   customer_name, display_looker, looker_template_id)

        if do_not_import_data is True:
            if enable_bq_import is not True and enable_cur_import is not True:
                print("Please specific whether to generate a Migration Center report (-b) or AWS CUR report (-a).")
                exit()

            if enable_bq_import is True:
                looker_report_url = create_looker_url("MC", customer_name, datetime, gcp_project_id, bq_dataset_name,
                                                      bq_table_prefix)
            elif enable_cur_import is True:
                looker_report_url = create_looker_url("CUR", customer_name, datetime, gcp_project_id, bq_dataset_name,
                                                      bq_table_prefix)

            print(f"Looker URL: {looker_report_url}")

        if connect_sheets_bq is True:
            if sheets_emails is not None:
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

            # Create New Google Sheet
            spreadsheet, credentials = create_google_sheets(customer_name, sheets_email_addresses, service_account_key,
                                                            sheets_id)

            data_source_ids = []
            worksheet_names = []

            # Connect each BG Table to a Worksheet
            for bq_table in bq_tables:
                response = spreadsheet.batch_update(connect_bq_to_sheets(gcp_project_id, bq_dataset_name, bq_table))
                bq_table_worksheet_id = spreadsheet.worksheet(bq_table)

                if do_not_import_data is True:
                    worksheet_names.append(bq_table_worksheet_id)

                # Autosize first cols in BQ table worksheet
                # res = spreadsheet.batch_update(autosize_worksheet(bq_table_worksheet_id, 0, 10))

                # Get dataource ID from batch update response
                data_source_ids.append(response['replies'][0]['addDataSource']['dataSource']['dataSourceId'])

            pivot_table_location = [0, 0]
            if enable_bq_import is True:
                generate_mc_sheets(spreadsheet, worksheet_names, "BQ", data_source_ids)
                exec_overview_worksheet = spreadsheet.worksheet("Executive Overview")
                gcp_overview_worksheet = spreadsheet.worksheet("GCP Detailed Overview")
                unmapped_worksheet = spreadsheet.worksheet("AWS Unmapped Overview")

                spreadsheet.reorder_worksheets([exec_overview_worksheet, gcp_overview_worksheet, unmapped_worksheet])

            if enable_cur_import is True:
                generate_bq_cur_sheets(spreadsheet, worksheet_names, data_source_ids)
                overview_worksheet = spreadsheet.worksheet("AWS Overview")
                details_worksheet = spreadsheet.worksheet("AWS Details")

                spreadsheet.reorder_worksheets([overview_worksheet, details_worksheet])

            # Delete default worksheet
            worksheet = spreadsheet.worksheet("Sheet1")
            spreadsheet.del_worksheet(worksheet)

            spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % spreadsheet.id

            print("Migration Center Sheets: " + spreadsheet_url)


if __name__ == "__main__":
    main()

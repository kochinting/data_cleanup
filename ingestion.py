"""
Python Scripts to read dataset file then export to both cleaned and anomalies files.
Author: Chin-Ting Ko
Date: 08/14/2020
"""

import pandas as pd
import json
import calendar
import numpy as np
from datetime import date
import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')


def load_file_to_df(file_name):
    """
    This function is to load dataset and convert to pandas dataframe
    :param file_name: input data set file
    :return: df (pandas dataframe)
    """
    json_list = []
    with open(file_name, "r") as f:
        for line in f.readlines():
            data = json.loads(line)
            json_list.append(data)
    f.close()
    df = pd.DataFrame.from_dict(json_list, orient="columns")
    df["anomalies"] = [""] * len(df)
    return df


def leap_year_anomalies(b_date):
    """
    This function is to check if 2/29 in any non leap year
    :param b_date: date string, either mm/dd/yyyy or yyyy-mm-dd
    :return: True/False
    """
    if ("/") in b_date:
        mon = b_date.split("/")[0]
        day = b_date.split("/")[1]
        year = b_date.split("/")[2]
    else:
        mon = b_date.split("-")[1]
        day = b_date.split("-")[2]
        year = b_date.split("-")[0]

    if (mon == "2" or mon == "02") and day == "29" and calendar.isleap(int(year)) is False:
        return True
    else:
        return False


def anomalies_checker(df):
    """
    This function is to check anomalies from input dataframe, rules is based on Chime take home challange listed.
    :param df: dataframe to check
    :return: export_df (cleaned) and drop_df (anomalies need to be reviewed)
    """

    # check if birthdata is invalid, format should follow yyyy/mm/dd
    birthdate_invalid = df[~df["birth_date"].str.contains(r'(\d+/\d+/\d+)')]
    birthdate_invalid_list = birthdate_invalid.index.values.tolist()
    df.loc[birthdate_invalid_list, "anomalies"] = df["anomalies"].astype(str) + " birthdate_invalid"

    # check if any 2/29 in non leap year
    df["leap_year_anomalies"] = df["birth_date"].apply(leap_year_anomalies)
    birthdate_invalid_leap_year = df[df["leap_year_anomalies"] == True]
    birthdate_invalid_leap_year_list = birthdate_invalid_leap_year.index.values.tolist()
    df.loc[birthdate_invalid_leap_year_list, "anomalies"] = df["anomalies"].astype(str) + " leap_year_anomalies"

    # check if any created_at is invalid, date format should follow yyyy-mm-ddTHH:MM:SS, year can only len = 4
    created_at_invalid = df[df['created_at'].str.contains(r'(\d{5}-\d+-\d+T\d+:\d+:\d+)')]
    created_at_invalid_list = created_at_invalid.index.values.tolist()
    df.loc[created_at_invalid_list, "anomalies"] = df["anomalies"].astype(str) + " created_at_invalid"

    # check if customer age < 18
    tmp_df = df[~df.isin(birthdate_invalid) & ~df.isin(birthdate_invalid_leap_year) & ~df.isin(created_at_invalid)]
    tmp_df['created_at'] = pd.to_datetime(tmp_df['created_at'], format='%Y-%m-%dT%H:%M:%S')
    tmp_df['birth_date'] = pd.to_datetime(tmp_df['birth_date'], format='%m/%d/%Y')
    tmp_df['created_age'] = (tmp_df['created_at'] - tmp_df['birth_date']) / np.timedelta64(1, 'Y')
    age_invalid = df[tmp_df['created_age'] < 18]
    age_invalid_list = age_invalid.index.values.tolist()
    df.loc[age_invalid_list, "anomalies"] = df["anomalies"].astype(str) + " age_invalid"

    # check if email follow email format.
    email_invalid = df[~df['email'].str.contains(r'[^@]+@[^@]+\.[^@]+')]
    email_invalid_list = email_invalid.index.values.tolist()
    df.loc[email_invalid_list, "anomalies"] = df["anomalies"].astype(str) + " email_invalid"

    # check if phone follow only int (10)
    phone_invalid = df[df['phone'].str.len() != 10]
    phone_invalid_list = phone_invalid.index.values.tolist()
    df.loc[phone_invalid_list, "anomalies"] = df["anomalies"].astype(str) + " phone_invalid"

    # check if zip code follow int(5)
    zip_invalid = df[df['zip5'].str.len() != 5]
    zip_invalid_list = zip_invalid.index.values.tolist()
    df.loc[zip_invalid_list, "anomalies"] = df["anomalies"].astype(str) + " zip_invalid"

    # check if update_dat follow date format
    updated_at_invalid = df[~df['updated_at'].str.contains(r'(\d{4}-\d+-\d+T\d+:\d+:\d+)')]
    updated_at_invalid_list = updated_at_invalid.index.values.tolist()
    df.loc[updated_at_invalid_list, "anomalies"] = df["anomalies"].astype(str) + " updated_at_invalid"

    # check if status not in 'active' or 'cancelled'
    status_invalid = df[df['status'] == 'reinstated']
    status_invalid_list = status_invalid.index.values.tolist()
    df.loc[status_invalid_list, "anomalies"] = df["anomalies"].astype(str) + " status_invalid"

    # data clean up
    del df['leap_year_anomalies']
    drop_df = df[df["anomalies"] != ""]

    export_df = df[df["anomalies"] == ""].reset_index(drop=True)
    del export_df['anomalies']
    export_df['id'] = df['id'].astype(int)
    export_df['phone'] = export_df['phone'].astype(int)
    export_df['zip5'] = export_df['zip5'].astype(int)
    export_df['created_at'] = pd.to_datetime(export_df['created_at'], format='%Y-%m-%dT%H:%M:%S')
    export_df['updated_at'] = pd.to_datetime(export_df['updated_at'], format='%Y-%m-%dT%H:%M:%S')
    export_df['birth_date'] = pd.to_datetime(export_df['birth_date'], format='%m/%d/%Y')

    return export_df, drop_df


if __name__ == '__main__':

    print("ingestions.py is running...")

    input_file = "Data_Sets.json"

    loaded_df = load_file_to_df(input_file)
    cleaned_df, review_df = anomalies_checker(loaded_df)

    cleaned_df.to_csv("cleaned_{}.csv".format(date.today()))
    review_df.to_csv("anomalies_{}.csv".format(date.today()))

    print("ingestion.py is finished")

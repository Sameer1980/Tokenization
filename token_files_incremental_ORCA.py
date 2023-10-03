# Tokenization of Data
# Developed by Rahul Goel
# Jan 2022

import pandas as pd
from pandas import json_normalize
import os
import sys
import numpy as np
import requests
import pyodbc
import json
import yaml
import csv
import warnings;

warnings.simplefilter('ignore')
import cx_Oracle
import logging

# Parameter info
credentials = yaml.safe_load(open('secrets_increment_ORCA.yaml'))
max_c = 0
file_dict = credentials['file-dev']

log_path = os.getcwd() + '/'

if os.path.exists(log_path) is False:
    os.mkdir(log_path)

FORMAT = '%(asctime)s:%(name)s:%(levelname)s - %(message)s'  # format to save errors, info etc., in log file
logging.basicConfig(filename=log_path + "/MDM_Tokenization_ORCA.log", format=FORMAT, level=logging.INFO,filemode="w")
logging.info("Started")



def get_data():
    import chardet
    with open(r'{}{}'.format(credentials['file-dev'][cred]['input_path'], credentials['file-dev'][cred]['filename']),
              'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large
    try:
        logging.info("File Name :- {}".format(credentials['file-dev'][cred]['filename']))
        logging.info('File encoding: {}, condifence :{}'.format(result['encoding'],result['confidence']))
        df = pd.read_csv(r'{}{}'.format(credentials['file-dev'][cred]['input_path'],credentials['file-dev'][cred]['filename']),delimiter=credentials['file-dev'][cred]['delimiter'],
                         header=None,dtype=str,encoding=result['encoding'],quoting=csv.QUOTE_NONE, keep_default_na=False, na_values=[''])
        logging.info("Input file read Successfully :- Rows : {} | Columns : {}".format(df.shape[0],df.shape[1]))
        return df
    except Exception as e:
        logging.error(e)
    try:
        df = pd.read_csv(
            r'{}{}'.format(credentials['file-dev'][cred]['input_path'], credentials['file-dev'][cred]['filename']),
            delimiter=credentials['file-dev'][cred]['delimiter'],
            header=None, dtype=str, encoding='Windows-1252', quoting=csv.QUOTE_NONE, keep_default_na=False,
            na_values=[''])
        return df
    except Exception as e:
        logging.error(e)
    # raise Exception



def token_data_col(df, col_names, token_group_template_detail, token_dtype_detail):
    auto_json_data = df
    # auto_json_data['tokengroup'] = (['CB01' for p in range(len(auto_json_data))])
    # auto_json_data['tokentemplate'] = (['CBASCII01' for p in range(len(auto_json_data))])

    # token_group_template_detail index 0 :- tokengroup
    # token_group_template_detail index 1 :- tokentemplate

    auto_json_data['tokengroup'] = token_group_template_detail[0]
    auto_json_data['tokentemplate'] = token_group_template_detail[1]

    auto_json_data = auto_json_data[[col_names, 'tokengroup', 'tokentemplate']].dropna()
    if token_dtype_detail == 'int':
        auto_json_data[col_names] = auto_json_data[col_names].astype(str).str.replace("-", "").str.replace("\.0", "")
    if token_dtype_detail == 'str':
        auto_json_data[col_names] = auto_json_data[col_names].where(
            ~auto_json_data[col_names].astype(str).str.fullmatch(r"\s*"))
        auto_json_data[col_names] = auto_json_data[col_names].str.strip()
        auto_json_data[col_names] = auto_json_data[col_names].str.pad(20, side='right')
        auto_json_data[col_names] = auto_json_data[col_names].str[:20]

    auto_json_data.rename(columns={col_names: 'data'}, inplace=True)
    auto_json_data_dict = auto_json_data.to_json(orient='records')
    logging.info("API input dictionary generated")

    return auto_json_data, auto_json_data_dict


def token_generic(auto_json_dict_data):
    bearer_header = {
        "APP_ID": credentials['vts-expl']['APP_ID'],
        "APP_KEY": credentials['vts-expl']['APP_KEY'],
        "apiVersion": str(credentials['vts-expl']['apiVersion'])
    }

    def header_generation():
        try:
            headers = {
                'Content-Type': 'Application/json',
                'Authorization': 'Bearer {}'.format(
                    requests.post(credentials['vts-expl']['bearer-url'], headers=bearer_header).json()[
                        'access_token'])
            }
            return headers
        except Exception as e:
            logging.error("Issue Generating Bearer Token")
            logging.error(e)
            raise

    headers = header_generation()

    try:
        payload = auto_json_dict_data
        url = credentials['vts-expl']['token-url']
        response = requests.post(url, headers=headers, data=payload, verify=False)

        if response.status_code == 401:
            if "Unauthorized. Azure AD  Access token is missing or invalid" in response.json(strict=False)['message']:
                headers = header_generation()
                response = requests.post(url, headers=headers, data=payload, verify=False)

        # data = response.json()
        data = response.json(strict=False)
        temp = pd.DataFrame(data)
        logging.info("Token data generated")

        return temp
    except Exception as e:
        logging.error("Issue performing Tokenization with url")
        logging.error(e)
        raise


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

try:
    for cred in range(len(credentials['file-dev'])):
        log_path = os.getcwd() + '/'

        if os.path.exists(log_path) is False:
            os.mkdir(log_path)

        FORMAT = '%(asctime)s:%(name)s:%(levelname)s - %(message)s'  # format to save errors, info etc., in log file
        logging.basicConfig(filename=log_path + "/" + credentials['file-dev'][cred]['filename'] + "_Tokenization.log",
                            format=FORMAT, level=logging.INFO, filemode="w")
        logging.info("Started")
        try:
            df3 = get_data()
            start_size = df3.shape[0]
        except:
            continue

        column_order = df3.columns
        arr = np.array(credentials['file-dev'][cred]['col_position'])
        arr = arr[(arr < len(column_order))]
        col_names_file = [int(i) - 1 for i in arr]
        token_group_template = {
            val: [credentials['file-dev'][cred]['tokengroup'][ind], credentials['file-dev'][cred]['tokentemplate'][ind]] for
            ind, val in enumerate(col_names_file)}
        token_dtype = {val: credentials['file-dev'][cred]['dtype'][ind] for ind, val in enumerate(col_names_file)}
        temp = pd.DataFrame()
        for col_name in col_names_file:
            logging.info("-" * 50)
            logging.info("Processing column {}".format(col_name))
            try:
                temp[col_name] = df3[col_name].str.strip()
                if len(temp[col_name].unique()) == 1 and temp[col_name].unique()[0] == '':
                    df3[str(col_name) + '_token'] = np.nan

                else:
                    df3[col_name] = df3[col_name].str.strip()
                    auto_json, auto_json_dict = token_data_col(df3, col_name, token_group_template[col_name],
                                                               token_dtype[col_name])
                    result = []

                    auto_json_unique = auto_json.drop_duplicates(subset=['data'])

                    if auto_json.shape[0] > 0:
                        chunk_count = 1
                        for i in chunker(auto_json_unique, 1000):
                            logging.info("Tokenizing Chunk {}".format(chunk_count))
                            i = i.to_json(orient='records')
                            array = i
                            auto_json_dict = array
                            result.append(token_generic(auto_json_dict))
                            chunk_count += 1

                        result = pd.concat(result).reset_index(drop=True).rename(
                            columns={'token': str(col_name) + '_token'})
                        result.drop(columns={'status'}, inplace=True)

                        result_join = auto_json_unique.reset_index().join(result).set_index('index')

                        auto_json[str(col_name) + '_token'] = auto_json['data'].map(
                            result_join.set_index('data')[str(col_name) + '_token'])

                        df3 = df3.join(auto_json[str(col_name) + '_token'].to_frame())
                    else:
                        df3[str(col_name) + '_token'] = np.nan
            except KeyError as e:
                print(e)

        df3.drop(columns={'tokengroup', 'tokentemplate'}, inplace=True)
        df3.drop(columns=col_names_file, inplace=True)
        for col_name in col_names_file:
            df3 = df3.rename(columns={str(col_name) + '_token': col_name})

        df3 = df3[column_order]
        if start_size == df3.shape[0]:
            logging.info('start & end size match')
            df3.to_csv(r'{}{}'.format(credentials['file-dev'][cred]['output_path'],
                                      credentials['file-dev'][cred]['filename']), sep='\x01', quoting=csv.QUOTE_NONE,
                       header=None, index=False
                       )
        else:
            logging.info('start & end size mismatch')
        # credentials['file-dev'][cred]['delimiter']

        logging.info("Output Data generated successfully :- Rows : {} | Columns : {}".format(df3.shape[0], df3.shape[1]))
        logging.info("Completed")
        logging.info("-" * 50)
        logging.info("-" * 50)
        print('File tokenisation done')
except Exception as e:
    logging.error(e)
print ('Run completed')
logging.info('Run completed')



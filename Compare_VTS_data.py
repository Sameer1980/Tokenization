import csv
import threading
import pandas as pd
import pyodbc
import yaml
import logging
import os
import time
import warnings
import requests
import shutil
from datetime import datetime
import re
warnings.filterwarnings('ignore')

now = datetime.now()
dt_str= now.strftime("%Y-%m-%d %H:%M:%S")
dt_string=re.sub('[^A-Za-z0-9]+', '', dt_str)

start = time.perf_counter()
# Parameter info
credentials = yaml.safe_load(open('data_compare_VTS.yaml'))
env = 'dev'
offset_by=100000

FORMAT = '%(asctime)s:%(name)s:%(levelname)s - %(message)s'  # format to save errors, info etc., in log file


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file,mode='w')
    handler.setFormatter(logging.Formatter(' %(message)s'))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# log and output directory creation
if os.path.exists(os.getcwd() + '/logs') is False:
    os.mkdir(os.getcwd() + '/logs')

if os.path.exists(os.getcwd() + '/output_files') is False:
    os.mkdir(os.getcwd() + '/output_files')

log_path = os.getcwd() + '/logs'
output_path = os.getcwd() + '/output_files'

archive_path = os.path.join(output_path, 'archive')
files = os.listdir( output_path )
for filename in files:
    fullpath = os.path.join(output_path, filename)
    shutil.move(fullpath, archive_path)


logging.basicConfig(filename=log_path + "/data_compare_VTS.log", format=FORMAT, level=logging.INFO,filemode="w")
logging.info("Started")
result = dict()

logger = setup_logger('first_logger', log_path +'/email.log')
logger.info('Hi Team,\n Please find the summary for VTS tokens table comparison against tokens in Client MDM.')
VTS_tokens_count=0
MDM_tokens_count=0

def retrive_pwd(api_url):
    url = api_url
    payload={}
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.request("GET", url, headers=headers, data=payload,verify=False)
    except Exception as e:
        print(e)
        raise('Unable to retrive password from API')
    res=response.json()
    return res['Content']

def get_sql_data(i):
    server = credentials['sqldw-dev']['server']
    database = credentials['sqldw-dev']['database']
    username = credentials['sqldw-dev']['login']
    password = retrive_pwd(credentials['sqldw-%s'%env]['pwd_url'])
    driver = '{ODBC Driver 17 for SQL Server}'
    try:
        cnxn = pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;UID=' + username + ';PWD=' + password + ';DATABASE=' + database + ';Encrypt=yes;TrustServerCertificate=yes')
    except Exception as e:
        logging.error("Error :-")
        logging.error(e)
        print(e)
        raise
    logging.info("Reading SQL Data")
    logging.info("Table Name :- {}".format(credentials['sqldw-dev']['table_name']))

    sql_query = """{}""".format(credentials['sqldw-dev']['query'])

    # if 'order by' in sql_query:
    #     sql_query = sql_query + ' OFFSET {} ROWS FETCH NEXT {} ROWS ONLY'.format(offset_count,offset_by)
    # else:
    #     logging.error("order by clause missing")
    #     print("order by clause missing")
    #     print("Failed")
    #     raise


    try:
        #print(sql_query)
        sql_df = pd.read_sql(sql_query, con=cnxn)
    except Exception as e:
        print("Error in Oracle query..Please review")
        logging.error("Error :-")
        logging.error(e)
        raise
    finally:
        cnxn.close()

    result['sql_data'] = sql_df
    return sql_df

def get_sql_data_VTS(i):
    server = credentials['sqldw-dev']['server_VTS']
    database = credentials['sqldw-dev']['database_VTS']
    username = credentials['sqldw-dev']['login_VTS']
    password = credentials['sqldw-dev']['pwd_VTS']
    driver = '{ODBC Driver 17 for SQL Server}'
    try:
        cnxn = pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;UID=' + username + ';PWD=' + password + ';DATABASE=' + database + ';Encrypt=yes;TrustServerCertificate=yes')
    except Exception as e:
        logging.error("Error :-")
        logging.error(e)
        print(e)
        raise
    logging.info("Reading SQL_VTS Data")
    logging.info("Table Name :- {}".format(credentials['sqldw-dev']['table_name_VTS']))

    sql_query_VTS = """{}""".format(credentials['sqldw-dev']['query_VTS'])

    # if 'order by' in sql_query_VTS:
    #     sql_query_VTS = sql_query_VTS + ' OFFSET {} ROWS FETCH NEXT {} ROWS ONLY'.format(offset_count,offset_by)
    # else:
    #     logging.error("order by clause missing")
    #     print("order by clause missing")
    #     print("Failed")
    #     raise


    try:
        #print(sql_query_VTS)
        sql_df = pd.read_sql(sql_query_VTS, con=cnxn)
    except Exception as e:
        #print("Error in Oracle query..Please review")
        logging.error("Error :-")
        logging.error(e)
        raise
    finally:
        cnxn.close()

    result['sql_data_VTS'] = sql_df
    return sql_df


x=0
for num in range(1):
    logging.info("*"*40)
    #logging.info("comparing {} and {}".format(compare_table_df['oracle_table_name'][num],compare_table_df['sql_table_name'][num]))
    logging.info("*" * 40)

    stats_table = pd.DataFrame(columns=['Column_Name', 'Matched', 'Unmatched'])
    unmatched_records_oracle_final = pd.DataFrame()
    unmatched_records_sql_final = pd.DataFrame()
    unified_unmatched_records_final = pd.DataFrame()
    up_sql_data = pd.DataFrame()
    up_oracle_data = pd.DataFrame()
    unmatched_records_oracle = pd.DataFrame()
    unmatched_records_sql = pd.DataFrame()
    unified_unmatched_records = pd.DataFrame()

    offset_count = 0
    offset_batch = 1
    offset_process = True
    while offset_process:

        print("-"*30)
        print("Processing Batch {}".format(offset_batch))
        print("Reading Database")

        logging.info("-" * 30)
        logging.info("Processing Batch {}".format(offset_batch))


        if __name__=='__main__':
            t1 = threading.Thread(target=get_sql_data, args=(num,))
            t2 = threading.Thread(target=get_sql_data_VTS, args=(num,))
            # t3 = threading.Thread(target=get_oracle_data2, args=(num,))

            t1.start()
            t2.start()
            # t3.start()

            t1.join()
            t2.join()
            # t3.join()

        oracle_data0 = pd.DataFrame(result['sql_data_VTS'])
        o=oracle_data0.loc[oracle_data0['token'].str.contains('!  # 0&^!cP0y|] vhPg*%?', case=True)]
        if o.shape[0]<0:
            print('true')
        #oracle_data0 = oracle_data0.loc[oracle_data0['source'] == 'PRSClientMDM']
        oracle_data0.drop(columns=[ 'LoadDate', 'source', 'original_value'], inplace=True)
        oracle_data0.sort_values(by=['token'],inplace=True)
        oracle_data0.drop_duplicates(inplace=True)
        VTS_tokens_count=VTS_tokens_count+oracle_data0.shape[0]
        sql_data0 = pd.DataFrame(result['sql_data'])
        #sql_data0['DR_LIC_N'] = sql_data0['DR_LIC_N'].replace(r" +$", r"", regex=True)
        size = oracle_data0.shape[0]
        size2 = sql_data0.shape[0]
        sql_data0.drop_duplicates(inplace=True)
        # sql_data0 = pd.melt(sql_data0[['SSN', 'DR_LIC_N']])
        # sql_data0.rename(columns={'value': 'token'},inplace=True)
        # sql_data0.drop(columns=['variable'], inplace=True)
        # sql_data0.drop_duplicates(inplace=True)
        sql_data0.sort_values(by=['token'],inplace=True)
        sql_data0.dropna(inplace=True)
        MDM_tokens_count=MDM_tokens_count+sql_data0.shape[0]

        print("Reading Database Completed")
        print('tokens in MDM:{}\ntokens in VTS:{}'.format(sql_data0.shape[0], oracle_data0.shape[0]))

        # if oracle_data0.shape[0]!=sql_data0.shape[0]:
        #     print('row count mismatch...pls check...last iteration')
        #     logging.info('row count mismatch...pls check...last iteration')

        f = sql_data0.columns[0]

        try:
            #taking inersection of data
            nt_df = pd.merge(sql_data0,oracle_data0, how='inner', on=f)
            #nt_df.drop_duplicates(inplace=True)
            nt_df = nt_df[f]
            oracle_data = pd.merge(oracle_data0, nt_df, how='inner', on=f)
            sql_data = pd.merge( sql_data0,nt_df, how='inner', on=f)
            print('sql data size Common:{}\noracle data size Common:{}'.format(sql_data.shape[0], oracle_data.shape[0]))
            #get discluded data
            p_sql_data = sql_data0[~sql_data0.loc[:,f].isin(sql_data.loc[:,f])]
            p_oracle_data = oracle_data0[~oracle_data0.loc[:,f].isin(oracle_data.loc[:,f])]
            #Store discluded data
            up_sql_data = up_sql_data.append(p_sql_data).reset_index(drop=True)
            #print('stored sql discluded data:{}'.format(up_sql_data.shape[0]))
            up_oracle_data=up_oracle_data.append(p_oracle_data).reset_index(drop=True)
            #print('stored oracle discluded data:{}'.format(up_oracle_data.shape[0]))

            #finding pairable discluded data
            #if max(size,size2)==0:
            nt_df1 = pd.merge(up_sql_data,up_oracle_data,how='inner',on=f)
            nt_df1 = nt_df1[f]
            up_sql_data1=pd.merge(nt_df1,up_sql_data,how='inner',on=f)
            up_oracle_data1 = pd.merge(nt_df1, up_oracle_data, how='inner', on=f)
            #adding pairable discluded data to main comparision frame
            oracle_data=oracle_data.append(up_oracle_data1).reset_index(drop=True)
            sql_data = sql_data.append(up_sql_data1).reset_index(drop=True)
            #get discluded discluded data if any
            dup_sql_data = up_sql_data[~up_sql_data.loc[:, f].isin(up_sql_data1.loc[:, f])]
            dup_oracle_data = up_oracle_data[~up_oracle_data.loc[:, f].isin(up_oracle_data1.loc[:, f])]
            oracle_data.drop_duplicates(inplace=True)
            oracle_data.reset_index(drop=True,inplace=True)
            sql_data.drop_duplicates(inplace=True)
            sql_data.reset_index(drop=True, inplace=True)
            print("Processing Database Completed")
        except MemoryError as e:
            print(e)
            logging.error(e)
            break
        except Exception as e:
            print(e)
            logging.error(e)

        print('sql data size:{} oracle data size:{}'.format(sql_data.shape[0], oracle_data.shape[0]))

        if oracle_data.shape[0] == sql_data.shape[0]:
            # Compare if columns are same [A-B] and [B-A] should be 0
            if len(oracle_data.columns.difference(sql_data.columns)) == 0 and len(sql_data.columns.difference(oracle_data.columns)) == 0:
                print("Comparing Data")
                completed_percent = round(len(oracle_data.columns)*0.2),round(len(oracle_data.columns)*0.4),round(len(oracle_data.columns)*0.6),round(len(oracle_data.columns)*0.8),round(len(oracle_data.columns))
                tot_completed = 0

                for k, col_name in enumerate(oracle_data.columns):
                    if k+1 in completed_percent:
                        tot_completed += 20
                        print("{} % Completed".format(tot_completed))

                    compare_vals = (oracle_data[col_name].fillna("Not_Avail").sort_index() == sql_data[col_name].fillna("Not_Avail").sort_index())
                    match_count = compare_vals.value_counts()
                    stats_table = stats_table.append({'Column_Name':col_name, 'Matched':match_count.loc[True] if True in match_count.index else 0,'Unmatched':match_count.loc[False] if False in match_count.index else 0},ignore_index=True)

                    # changes done
                    if credentials['basic-info']['token_cols']['flag']:
                        if col_name not in credentials['basic-info']['token_cols']['names']:
                            if False in match_count.index:
                                temp_df_oracle = oracle_data[
                                    (~oracle_data.index.isin(unmatched_records_oracle.index)) & (~compare_vals)]
                                temp_df_sql = sql_data[
                                    (~sql_data.index.isin(unmatched_records_sql.index)) & (~compare_vals)]
                                if temp_df_oracle.shape[0] > 0:
                                    unmatched_records_oracle = unmatched_records_oracle.append(temp_df_oracle)
                                    unmatched_records_sql = unmatched_records_sql.append(temp_df_sql)
                                    unified_unmatched_records = pd.concat([unmatched_records_oracle,unmatched_records_sql],axis=1)

                if unmatched_records_oracle.shape[0] > 0:
                    unmatched_records_oracle_final = unmatched_records_oracle_final.append(unmatched_records_oracle).reset_index(drop=True)
                    unmatched_records_sql_final = unmatched_records_sql_final.append(unmatched_records_sql).reset_index(drop=True)
                    unified_unmatched_records_final = unified_unmatched_records.append(unified_unmatched_records).reset_index(drop=True)

                offset_batch += 1
                offset_count += offset_by
                offset_process = False
                if max(size,size2)==0:
                    offset_process = False
            else:
                logging.error("Column Names Mismatch..Please check")
                print("Column Names Mismatch..Please check")
                offset_process = False
        else:
            logging.error("Row Count Mismatch..Please check")
            print("Row Count Mismatch..Please check")
            offset_process = False

    stats_table = stats_table.groupby('Column_Name')[['Matched', 'Unmatched']].sum().reset_index()
    logging.info(stats_table)
    #print(unified_unmatched_records_final.shape[0])
    #unified_unmatched_records_final.to_csv(output_path + '/VTS_unified_unmatched.csv', index=False)
    #print(dup_oracle_data.shape[0])
    dup_oracle_data.drop_duplicates(inplace=True)
    print(dup_oracle_data.shape[0])
    #if dup_oracle_data.shape[0]:
    #dup_oracle_data.to_csv(output_path + '/VTS_pairless_oracle_unmatched_{}.csv'.format(dt_string),index=False,quoting=csv.QUOTE_NONE,escapechar='/')
    #print(dup_sql_data.shape[0])
    dup_sql_data.drop_duplicates(inplace=True)
    dup_sql_data.dropna(inplace=True)
    print(dup_sql_data.shape[0])
    #if dup_sql_data.shape[0]:
    dup_sql_data.to_csv(output_path + '/VTS_pairless_sql_unmatched_{}.csv'.format(dt_string), index=False,quoting=csv.QUOTE_NONE,escapechar='/')

    stats_table = stats_table.groupby('Column_Name')[['Matched','Unmatched']].sum().reset_index()
    del stats_table
    print('MDM tokens stored in VTS table: {}'.format(VTS_tokens_count))
    logger.info('\n MDM tokens stored in VTS table: {}'.format(VTS_tokens_count))
    print('tokens in MDM table: {}'.format(MDM_tokens_count))
    logger.info('\n Tokens in MDM table: {}'.format(MDM_tokens_count))

    logger.info('\n Mismatches if any: {}'.format(unified_unmatched_records_final.shape[0]))
    logger.info('\n Tokens in VTS table, not present in MDM table: {}'.format(dup_oracle_data.shape[0]))
    logger.info('\n Tokens in MDM table, not present in VTS table: {}'.format(dup_sql_data.shape[0]))
    logger.info('\n PFA list of MDM tokens not in VTS table')

finish = time.perf_counter()
print(f'fnished in {round(finish-start,2)} seconds')







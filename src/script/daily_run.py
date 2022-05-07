import json
import os
import time
import requests
import upload_gcs_script
from google.cloud import bigquery
from src.script.slack_push_mes import slack_push_mes, slack_push_exception
import traceback
import sys

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/solana_data/solana-etl/blockchain-data-process@footprint-blockchain-etl.iam.gserviceaccount.com.json'

def get_current_num():
    url = 'https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec'
    data = {"jsonrpc": "2.0", "id": 1, "method": "getSlot"}
    req = requests.post(url, json=data)
    res = json.loads(req.text).get('result')
    new_block_num = res // 10000
    return new_block_num


def sql_to_bigquery(sql):
    client = bigquery.Client(project='footprint-blockchain-etl')
    query_job = client.query(sql)
    rows = query_job.result()
    for row in rows:
        print(row)
    return rows


def produce_table(filter):
    for task in ['blocks', 'transactions', 'token_transfers']:
        try:
            upload_gcs_script.load_to_bq(task, filter_=f'{filter}*')
        except Exception as e:
            return e


def produce_verify_sql(block_number):
    sql_string = ''
    with open("/solana_data/solana-etl/src/script/block_missing_verify", 'r') as file:
        line = file.read()
        sql_string = sql_string + line
    sql = sql_string.format(block_number=block_number)
    return sql

def produce_total_verify_sql():
    sql = ''
    with open("/solana_data/solana-etl/src/script/total_verify", 'r') as file:
        sql = file.read()
    return sql

def produce_merge_sql(block_number):
    sql_string = ''
    with open("/solana_data/solana-etl/src/script/only_merge", 'r') as file:
        line = file.read()
        sql_string = sql_string + line
    start_num = f'{block_number}0000'
    end_num = f'{block_number}9999'
    sql = sql_string.format(blocks_number=block_number, start_num=start_num, end_num=end_num)
    return sql


def clear_space(i):
    os.system(f'rm -rf /solana_data/bq_data/{i}0000')
    os.system(f'rm -rf /solana_data/blocks_raw/{i}0000')
    os.system(f'rm -rf /solana_data/data/{i}0000')


def upload_daily():
    max_num = 0
    while (True):
        new_block_num = get_current_num()
        with open('/solana_data/solana-etl/src/script/max_num', 'r') as file:
            num = file.read()
            max_num = int(num)
        if (new_block_num - max_num > 0):
            end_blocks = new_block_num - 1
            extract_str = f'export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start {max_num}0000 --end {end_blocks}9999 --endpoint https://bold-wispy-wave.solana-mainnet.quiknode.pro/ceb132a60cccc7ec89a7e2de92efec50706dfead/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl  >> /solana_data/solana_output.log &2>1'
            os.system(extract_str)
            for i in range(max_num, new_block_num):
                produce_table(i)
                verify_sql = produce_verify_sql(i)
                verify_result = sql_to_bigquery(verify_sql)
                if (verify_result.total_rows != 0):
                    slack_push_mes(max_num, end_blocks, "An exception occurred before merging")
                    return False
                merge_sql = produce_merge_sql(i)
                merge_result = sql_to_bigquery(merge_sql)
                total_verify_sql = produce_total_verify_sql()
                total_verify_result = sql_to_bigquery(total_verify_sql)
                if(total_verify_result.total_rows != 0):
                    slack_push_mes(max_num, end_blocks, "An exception occurred after merging")
                    return False
                clear_space(i)
                max_num = new_block_num

            num = max_num + 1
            with open('/solana_data/solana-etl/src/script/max_num', 'w') as f, open('/solana_data/solana-etl/src/script/hisitory_max_num', 'a') as file:
                num_str = f'{num}'
                f.write(num_str)
                file.writelines(num_str)
        time.sleep(60)

if __name__ == '__main__':
    try:
        upload_daily()
    except Exception as e:
        mes = f'An exception occurs when executing the solana daily upload!!! <@U02HSQQDX71> <@U010F3Z5Z98>\n Exception Message : {e}'
        exc_type, exc_value, exc_traceback_obj = sys.exc_info()
        traceback.print_tb(exc_traceback_obj)
        slack_push_exception(mes)

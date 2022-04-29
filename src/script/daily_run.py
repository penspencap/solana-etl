import json
import os
import time
import requests
import upload_gcs_script
from google.cloud import bigquery




def slack_push_mes(start_num, end_num, mes):
    url = 'https://hooks.slack.com/services/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'
    req = requests.post(url, json={
        'text': f'Solana chain extract failed: {mes} {start_num}0000 to {end_num}9999. Please check it out \n <@U02HSQQDX71> <@U010F3Z5Z98>'})
    print(req)


def get_current_num():
    url = 'https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec'
    data = {"jsonrpc": "2.0", "id": 1, "method": "getSlot"}
    req = requests.post(url, json=data)
    res = json.loads(req.text).get('result')
    new_block_num = res // 10000
    return new_block_num


def sql_to_bigquery(sql):
    client = bigquery.Client()
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
    with open("block_missing_verify", 'r') as file:
        line = file.read()
        sql_string = sql_string + line
    sql = sql_string.format(block_number=block_number)
    return sql

def produce_total_verify_sql():
    sql = ''
    with open("total_verify", 'r') as file:
        sql = sql + file.read()
    return sql

def produce_merge_sql(block_number):
    sql_string = ''
    with open("only_merge", 'r') as file:
        line = file.read()
        sql_string = sql_string + line
    start_num = f'{block_number}0000'
    end_num = f'{block_number}9999'
    sql = sql_string.format(blocks_number=block_number, start_num=start_num, end_num=end_num)
    return sql


def clear_space():
    os.system('rm -rf /solana_data/bq_data/*')
    os.system('rm -rf /solana_data/blocks_raw/*')
    os.system('rm -rf /solana_data/data/*')


def upload_daily():
    max_num = 0
    while (True):
        new_block_num = get_current_num()
        with open('max_num', 'r') as file:
            num = file.read()
            max_num = int(num)
        if (new_block_num - max_num > 0):
            end_blocks = new_block_num - 1
            extract_str = f'export PYTHONPATH=/solana_data/solana-etl;cd /solana_data/solana-etl/;/opt/service/python3.8.10/bin/python3 src/ExportLoadPeriod.py /solana_data/data --start {max_num}0000 --end {end_blocks}9999 --endpoint https://nameless-rough-shadow.solana-mainnet.quiknode.pro/90ad239f98ba9188c30660c2eeec4ef994627cec/ --n_jobs 15 --tasks all --temp_dir /solana_data/temp --destination_dir /solana_data/bq_data/ --destination_format jsonl  >> /solana_data/solana_output.log &2>1'
            os.system(extract_str)
            print(extract_str)
            for i in range(max_num, new_block_num):
                produce_table(i)
                verify_sql = produce_verify_sql(i)
                print(verify_sql)
                verify_result = sql_to_bigquery(verify_sql)
                print(verify_result.total_rows)
                if (verify_result.total_rows != 0):
                    slack_push_mes(max_num, end_blocks, "An exception occurred before merging")
                    return False
                merge_sql = produce_merge_sql(i)
                print(merge_sql)
                merge_result = sql_to_bigquery(merge_sql)
                total_verify_sql = produce_total_verify_sql()
                print(total_verify_sql)
                total_verify_result = sql_to_bigquery(total_verify_sql)
                print(total_verify_result.total_rows)
                # if(total_verify_result.total_rows != 0):
                #     # slack_push_mes(max_num, end_blocks, "An exception occurred after merging")
                #     print("合并后校验失败")
                #     return False
                clear_space()
                max_num = new_block_num
            with open('max_num', 'w') as f:
                num = max_num + 1
                str = f'{num}'
                f.write(str)
            with open('hisitory_max_num', 'a') as file:
                num = max_num + 1
                str = f'{num}\n'
                file.writelines(str)
            time.sleep(60)


if __name__ == '__main__':
    upload_daily()
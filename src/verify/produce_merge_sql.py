def read_file(block_number, file_address):
    sql_string = ''
    with open (file_address, 'r') as file:
        for line in file.readlines():
            sql_string = sql_string + line
    start_num = block_number.ljust(9, '0')
    end_num = block_number.ljust(9, '9')
    sql = sql_string.format(blocks_number=block_number, start_num=start_num, end_num=end_num)
    return sql

def produce_merge_sql(block_mums):
    print(read_file(block_mums ,"merge_trasaction"))


if __name__ == '__main__':
    ls = produce_merge_sql("121")

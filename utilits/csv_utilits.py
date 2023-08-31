import time
import csv
import os
import boto3
import subprocess

def download(csv_file, csvreader, symbol):
    with open(f'./{csv_file}', 'w', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        for idx, row in enumerate(csvreader):
            if idx == 0:
                row.append('symbol')
            else:
                row.append(symbol)
            writer.writerow(row)

def save_to_hdfs(csv_file, symbol):
    hdfs_url = 'hdfs://localhost:9000'
    from_path = os.path.abspath(f'./{csv_file}')
    file_name = f'{round(time.time())}_{symbol}.csv'
    to_path = f'{hdfs_url}/bronze/{file_name}'
    print(f"from path {from_path}")
    print(f"to path {to_path}")
    subprocess.run(["hadoop", "fs", "-put", from_path, to_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

def save_to_s3(csv_file, symbol):
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', 'pnPnSD6URaW1IyoB')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'Vqz6yaOgvdfOw4RmJntFH1ksgqNK3C8v')

    s3 = boto3.resource('s3', endpoint_url="http://127.0.0.1:9010")

    from_path = os.path.abspath(f'./{csv_file}')
    file_name = f'{round(time.time())}_{symbol}.csv'
    s3.Object('my-s3bucket', f'/bronze/{file_name}').put(Body=open(from_path, 'rb'))

from etl import pull_from_bucket

if __name__ == '__main__':
    bucket_name = 'sales-etl-bucket'
    file_name = 'Advertising.csv'
    pull_from_bucket(bucket_name, file_name)

class BronzeLayer:
    def __init__(self):
        pass
    
    def ingest_data_api(self,url):
        import pandas as pd
        import requests
        from io import StringIO
        
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.text
            df = pd.read_csv(StringIO(data))
            # converting the dataframe to csv in memory using StringIO
            csv_buffer = StringIO()
            df.to_csv(csv_buffer,index=False)

            print(csv_buffer.getvalue())
            
        else:
            print(f"failed to fetch data with status code: {response.status_code}")
            
    def put_data_s3(self,bucket_name,object_key,data):
        import os
        from dotenv import load_dotenv
        import boto3
        load_dotenv()
        
        aws_access_key_id = os.environ.get("aws_access_key_id")
        aws_secret_access_key = os.environ.get("aws_secret_access_key")
        
        s3_client = boto3.client(
            's3',
            region_name= 'ca-central-1',
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key
        )
        
        # upload the data in S3
        s3_client.put_object(
            Bucket = bucket_name,
            Key = object_key, #the path
            Body = data
        )
        return f"Data uploaded to S3 '{bucket_name}'"
        
obj = BronzeLayer()
data = obj.ingest_data_api("https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv")
obj.put_data_s3("tejasS3","bronze/bookings.csv",data)
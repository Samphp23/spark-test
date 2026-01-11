import json
import boto3
from pyspark.sql import SparkSession

client = boto3.client("s3")
Source_bucket = "source-s3-bucket-14-11"
Source_prefix = "daily/"
destination_prefix = "monthly/"

def lambda_handler():
    response = client.list_objects(Bucket=Source_bucket, Prefix=Source_prefix)
    for obj in response["Contents"]:
        Source_key = obj["Key"]
        if Source_key.endswith("/"):
            continue
        filename = Source_key.split("/")[-1] 
        destination_key = f"{destination_prefix}{filename}"
        if filename.endswith(".ipynb"):
            client.copy_object(
                Bucket=Source_bucket,
                CopySource={"Bucket": Source_bucket, "Key": Source_key},               
                Key=destination_key,
            )   
            print(f"File {filename} copied successfully to {destination_key}")
            client.delete_object(Bucket=Source_bucket, Key=Source_key)
            print(f"File {filename} copied successfully to {destination_key}")  
    return "Code ok"
if __name__=="__main__":
	print("datamigration image processing.....")
	lambda_handler()
	spark = SparkSession.builder \
	    .appName("Docker PySpark Job") \
	    .getOrCreate()
	data = [("Samir", 100), ("Rahul", 200)]
	df = spark.createDataFrame(data, ["name", "score"])
	df.show()
	spark.stop()

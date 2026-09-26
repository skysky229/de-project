from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

import os


# MINIO_ROOT_USER=minioadmin
# MINIO_ROOT_PASSWORD=minioadmin
# MINIO_ENDPOINT=http://minio:9000
# MINIO_BUCKET_RAW=raw
# MINIO_BUCKET_PROCESSED=processed
load_dotenv()

ROOT_USER = os.getenv("MINIO_ROOT_USER")
ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
ROOT_ENDPOINT = os.getenv("MINIO_ENDPOINT").replace('minio', 'localhost')
BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW")
BUCKET_PROCESSED = os.getenv("MINIO_BUCKET_PROCESSED")

DATA_DIR = '/Users/No/MasterProjects/DataEngineer/data_etl/data'

def file_resolver(dir: str = ""):
    if not dir:
        print("Failed to resolve directory")
        return []   
    return [os.path.join(dir,file_path) for file_path in os.listdir(dir) if file_path.endswith(".csv")]
    
def get_file_name(path: str = ""):
    file_parts = path.split("/")
    file_name = file_parts[-1].replace(".csv","")
    return file_name

def exclude_url_prefix(url: str = ""):
    if not url:
        print("No url exist !!")
        return
    cleaned_path = url.replace("http://", "").replace("https://", "")
    return cleaned_path

class S3Uploader(object):
    def __init__(self, access_key: str = "",  secret_key: str = "", endpoint: str = "",  bucket_raw: str = "", bucket_processed: str = ""):
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = endpoint
        self.bucket_raw = bucket_raw
        self.bucket_processed = bucket_processed

    def get_client(self): 
        return Minio(
            self.endpoint,
            self.access_key,
            self.secret_key,
            secure=False,
        )
    
    def init_buckets(self, client: Minio, buckets: list):
        try:
            for bucket in buckets:
                if bucket and not client.bucket_exists(bucket):
                    client.make_bucket(bucket)
        except S3Error as s3_error:
            print(f"S3 Error occured: {s3_error}")
            return
        except Exception as exc:
            print(f"Another Exception has occured : {exc}")
            return
        return
    
    def file_to_bucket(self, file_path: str, bucket_type: str):
        object_name = f"/logistics/" + get_file_name(file_path)
        try:
            object_stat = self.get_client().stat_object(bucket_type, object_name)
            if object_stat:
                print("Object exist")
            return
        except Exception as exc:
            print("Object has not existed:", exc)
       
        try:
            result = self.get_client().fput_object(
                    bucket_name=bucket_type,
                    object_name=object_name,
                    file_path=file_path,
                )
        except S3Error as s3_error:
            print(f"Error occured when putting file : {file_path} to {bucket_type}")
            return 
        except Exception as exc:
            print(f"Another error occured: {exc}")
            return 
        return result

    def put_bucket_raw(self, file_list: list):
        try:
            for file_path in file_list:
                result = self.file_to_bucket(file_path=file_path, bucket_type=self.bucket_raw)
                if not result:
                    print(f"Error occured")
                    return
        except Exception as exc:
            print(f"Error occured when putting : {exc}")
            return
    def put_bucket_processed(self, file_list: list):
        try:
            for file_path in file_list:
                result = self.file_to_bucket(file_path=file_path, bucket_type=self.bucket_processed)
                if not result:
                    print(f"Error occured")
                    return
        except Exception as exc:
            print(f"Error occured when putting : {exc}")
            return
        
if __name__ == "__main__":
    
    uploader = S3Uploader(ROOT_USER, ROOT_PASSWORD, exclude_url_prefix(ROOT_ENDPOINT)   , BUCKET_RAW, BUCKET_PROCESSED)
    client = uploader.get_client()
    uploader.init_buckets(client, [BUCKET_RAW, BUCKET_PROCESSED])
    file_list = file_resolver(DATA_DIR)
    uploader.put_bucket_raw(file_list)
    
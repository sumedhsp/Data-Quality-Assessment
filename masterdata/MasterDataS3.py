import boto3
import json
import csv

class MasterDataS3:
    
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        # public aws access key and secret key
        self.aws_access_key_id = ''
        self.aws_secret_access_key = ''

        # public S3 bucket object 
        self.s3 = None
        
        # S3 bucket name holding all the masterdata
        self.bucket_name = 'dqa-masterdata'

        try:
            boto3.setup_default_session(aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
#             print("Can't Authenticate into AWS", self.aws_access_key_id)
        except:
            print("Can't Authenticate into AWS")
            
    def reconnect_to_aws(self, aws_access_key_id=None, aws_secret_access_key=None):
        boto3.setup_default_session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


    def upload_to_s3(self, local_file_path=None, s3_file_name=None):
        """ Method to upload a given file with its local path and its file name to the respective s3 bucket """
        
        # Create an S3 client
        bucket_name=self.bucket_name
        s3 = boto3.client('s3')

        try:
        # Uploads the file to the configured s3 bucket 
            s3.upload_file(local_file_path, bucket_name, s3_file_name)
#             print(f"File uploaded successfully to {bucket_name}/{s3_file_name}")
            return 1
        except:
            print("The file upload was unsuccessful", e)
            return 0

    def index_master_data(self, arrayTags, filename):
        """ A function to index an uploaded master data file"""
        # load contents of index file
        try:
            with open('index.json') as file:
                index = json.load(file)
        except:
            print("Can't open or parse the json file")

        # append the new tags to the index
        for tag in arrayTags:
            if tag not in index.keys():
                index[tag] = [filename]
            else:
                index[tag].append(filename)

        # write the index with the new tags and file
        try:
            with open('index.json', 'w') as file:
                index = json.dump(index, file)
        except:
            print("Can't write to the json file")
        
        return 1
    
    def search_master_data(self, matchTags, intersect=False):
        """ A function to search the master data index via options of intersecting tags or union tags"""
        
        # resultant set and temporary set
        matchSet = set()
        tempMatchSet = set()

        try:
            with open('index.json') as file:
                index = json.load(file)
        except:
            print("Can't open or parse the json file")

        # iterate through the matchTags provided and generate the resultant set
        for tag in matchTags:
            tempMatchSet = set()
            if tag in index.keys():
                tempMatchSet.update(index[tag])
                if intersect:
                    if len(matchSet)!=0:
                        matchSet = matchSet.intersection(tempMatchSet)
                    else:
                        matchSet = tempMatchSet
                else:
                    matchSet = matchSet.union(tempMatchSet)
        return matchSet

    def retrieve_master_data_file(self, filename):
        """ A function to retrieve contents from a specific master data file"""

        # connect to the s3 client via boto3
        s3 = boto3.client('s3')

        try:
            response = s3.get_object(Bucket=self.bucket_name, Key=filename)
            
            # Read the content of the CSV file
            csv_content = response['Body'].read().decode('utf-8')

            # Parse the CSV content
            csv_reader = csv.reader(csv_content.splitlines())
            csv_data = list(csv_reader)

            return csv_data

        except Exception as e:
            print(f"An error occurred: {e}")
            return None
            




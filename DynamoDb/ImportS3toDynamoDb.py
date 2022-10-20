import json, os, gzip, sys, shutil, time
from ntpath import join


data_raw_folder = "./data_raw"
data_json_folder = "./data_json"

def preProcess(s3_uri):

    '''
        Get data (that exported from Dynamodb table to S3 bucket) to local
        ...
        Parameter
        ----------
        s3_uri: URI of exported data folder
    '''
    
    s3_sync_command = "aws s3 sync {} {}"
    os.system(s3_sync_command.format(s3_uri,data_raw_folder))

    # Convert from compressed file to normal Json
    for filename in os.listdir(data_raw_folder):
        file_path = os.path.join(data_raw_folder, filename)
        file_json = os.path.join(data_json_folder, filename[:3])
        with gzip.open(file_path, 'rb') as f_in:
            with open(file_json, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


def transformAndLoad(tableName):
    '''
        Transform to the standard Json format of DynamoDB Item
        ...
        Parameter
        ----------
        tableName: Name of DynamoDB table need to import
    '''

    file_item = 'item.json'
    file_item_path = "file://" + file_item
    put_item_command = "aws dynamodb put-item --table-name {} --item {}"
    for file_name in os.listdir(data_json_folder):
        file_path = os.join(data_json_folder, file_name)
        with open(file_path, 'r') as f_in:
            lines = f_in.readlines()
            for line in lines:
                line = line.strip('\{"Item\"}')[1:-2]
                with open(file_item, 'w') as f_out:
                    f_out.write(line)
                time.sleep(10)
                os.system(put_item_command.format(tableName, file_item_path))
                
if __name__ == "__main__":
    '''
        ...
        Guideline: Run the command: " python ImportS3toDynamoDb.py {s3_uri} {dynamoDbTableName} "
        ...

    '''
    # Make folder if not existed
    if not os.path.exists(data_raw_folder): os.makedirs(data_raw_folder)
    if not os.path.exists(data_json_folder): os.makedirs(data_json_folder)

    ## Parsing the arguments
    s3_uri = sys.argv[1]
    tableName = sys.argv[2]
    preProcess(s3_uri)
    transformAndLoad(tableName)
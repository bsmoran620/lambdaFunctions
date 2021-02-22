import boto3
import os
import sys
import gzip
import paramiko

host = os.environ['HOSTNAME']
port = 22
username = os.environ['USERNAME']
password = os.environ['PASSWORD']
remote_directory = os.environ['DIR']

s3_client = boto3.client('s3')


def SFTPTransfer(localPath, pathForUpload):
    print("ATTEMPTING SFTP TRANSFER")
    transport = paramiko.Transport((host, port))
    transport.connect(None, username, password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("Connected to SFTP successfully")

    # Upload
    sftp.put(localPath, remote_directory + '/' + pathForUpload)

    print('File transmitted!!!')
    # Close
    if sftp: sftp.close()
    if transport: transport.close()
    

def lambda_handler(event, context):
    print("STARTING")
    for record in event['Records']:
        sourcebucket = record['s3']['bucket']['name']
        sourcekey = record['s3']['object']['key'] 

        # Download the file to /tmp/ folder        
        download_path = f"/tmp/{sourcekey.split('/')[1]}"
        print("ATTEMPTING TO DOWNLOAD: " + download_path)
        s3_client.download_file(sourcebucket, sourcekey, download_path)
        
        print("FILE DOWNLOADED " + download_path)
        
        outputPath = os.path.splitext(download_path)[0]
        print("ATTEMPTING TO OUTPUT TO " + outputPath)
        try:
            with open(outputPath, 'wb') as f_out, gzip.open(download_path, 'rb') as f_in:
                f_out.writelines(f_in)
                print("FILE CREATED")

                pathForUpload = remove_prefix(outputPath, "/tmp/")
                SFTPTransfer(outputPath, pathForUpload)
                print("SFTP COMPLETED")
        except Exception as e:
            print("FAILED TO TRANSFER FILE")
            print(e)
            
        return


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

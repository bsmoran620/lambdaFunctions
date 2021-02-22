import boto3
import os
import sys
import ftplib
import gzip

host = os.environ['HOSTNAME']
username = os.environ['USERNAME']
password = os.environ['PASSWORD']
remote_directory = os.environ['DIR']

s3_client = boto3.client('s3')


def FTPTransfer(pathForUpload):
    print("ATTEMPTING FTP TRANSFER")
    os.chdir("/tmp/")
    ftp = ftplib.FTP(host)
    ftp.login(username, password)
    print(ftp.getwelcome())

    ftp.cwd(remote_directory)
    print('Working directory in target changed to {}'.format(remote_directory))
    file = open(pathForUpload, "rb")
    ftp.storbinary('STOR ' + pathForUpload, file)
    print('File transmitted!!!')


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
        
       	try:
	    # The following was build to extract a zip file and send each file over FTP but was later built for a single gzipped file
            # with zipfile.ZipFile(download_path, 'r') as zip_ref:
            #    zip_ref.extractall('/tmp/extract')
 
	    with gzip.GzipFile(fileobj=download_path, mode='rb') as fcontent:
            	FTPTransfer(fcontent)
        
	except Exception as e:
            print("FAILED TO TRANSFER FILE")
            print(e)
            
        return


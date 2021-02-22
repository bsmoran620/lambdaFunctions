# Lambda Function to transfer file from AWS S3 to Another host via SFTP
## Currently unzips GZIP file during processing

> Deployment:
> Python 3.8 AWS Lambda
> - Note: Needed to pip install paramiko on an AWS EC2 instance because the sub-dependencies
>        like bcrypt didn't work when packaged on osx locally
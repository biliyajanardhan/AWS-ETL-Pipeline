import json
import boto3

def lambda_handler(event, context):
    glue=boto3.client('glue')
    job_name='s3-glue-s3'
    try:
        response=glue.start_job_run(JobName=job_name)
        return {
            'statusCode': 200,
            'body': f"started glue job {job_name} with job run ID:{response['JobRunId']}"
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error starting glue job {job_name}: {str(e)}"
        }



import requests
import json
import time
import logging
from simple_salesforce import Salesforce
import pandas as pd
from typing import Optional

# Basic logging configuration to write to bulk_delete.log
logging.basicConfig(
    filename='bulk_delete.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SalesforceBulkDelete:
    def __init__(
        self,
        username: str,
        password: str,
        security_token: str,
        domain: Optional[str] = 'login'
    ):
        print("=== Initializing SalesforceBulkDelete ===")
        logging.info("Initializing SalesforceBulkDelete")

        self.sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain
        )
        print(f"Connected to Salesforce: {self.sf.base_url}")
        logging.info(f"Connected to Salesforce: {self.sf.base_url}")

        self.instance_url = self.sf.base_url
        self.headers = {
            'Authorization': f'Bearer {self.sf.session_id}',
            'Content-Type': 'application/json'
        }
        
    def create_delete_job(self, sobject_name: str) -> str:
        print("\n=== Creating new deletion job ===")
        logging.info("Creating new deletion job")
        endpoint = f"{self.instance_url}jobs/ingest"
        print(f"Endpoint used: {endpoint}")
        logging.info(f"Endpoint used for job creation: {endpoint}")
        
        body = {
            "operation": "delete",
            "object": sobject_name,
            "contentType": "CSV",
            "lineEnding": "LF"
        }
        
        response = requests.post(endpoint, headers=self.headers, json=body)
        print(f"Deletion job creation response: {response.status_code}")
        logging.info(f"Deletion job creation response: {response.status_code}")
        response.raise_for_status()
        job_id = response.json()['id']
        logging.info(f"Job ID created: {job_id}")
        return job_id
    
    def upload_data_for_deletion(self, job_id: str, ids_to_delete: list) -> None:
        print(f"\n=== Uploading data for job {job_id} ===")
        logging.info(f"Uploading data for job {job_id}")
        print(f"Number of records to delete: {len(ids_to_delete)}")
        logging.info(f"Number of records to delete for job {job_id}: {len(ids_to_delete)}")

        endpoint = f"{self.instance_url}jobs/ingest/{job_id}/batches"
        
        csv_data = "Id\n" + "\n".join(ids_to_delete)
        print("CSV data ready for upload")
        logging.info("CSV data ready for upload")
        
        headers = self.headers.copy()
        headers['Content-Type'] = 'text/csv'
        
        response = requests.put(endpoint, headers=headers, data=csv_data)
        print(f"Upload status: {response.status_code}")
        logging.info(f"Upload status for job {job_id}: {response.status_code}")
        response.raise_for_status()
    
    def check_job_status(self, job_id: str) -> dict:
        print(f"\n=== Checking job status {job_id} ===")
        logging.info(f"Checking job status {job_id}")
        endpoint = f"{self.instance_url}jobs/ingest/{job_id}"
        
        response = requests.get(endpoint, headers=self.headers)
        status = response.json()
        print(f"Current state: {status['state']}")
        logging.info(f"Current state for job {job_id}: {status['state']}")
        print(f"Records processed: {status.get('numberRecordsProcessed', 0)}")
        logging.info(f"Records processed for job {job_id}: {status.get('numberRecordsProcessed', 0)}")
        print(f"Records failed: {status.get('numberRecordsFailed', 0)}")
        logging.info(f"Records failed for job {job_id}: {status.get('numberRecordsFailed', 0)}")
        response.raise_for_status()
        return status
    
    def close_job(self, job_id: str) -> None:
        print(f"\n=== Closing job {job_id} ===")
        logging.info(f"Closing job {job_id}")
        endpoint = f"{self.instance_url}jobs/ingest/{job_id}"
        body = {"state": "UploadComplete"}
        
        response = requests.patch(endpoint, headers=self.headers, json=body)
        print(f"Closing job status: {response.status_code}")
        logging.info(f"Closing job status for {job_id}: {response.status_code}")
        response.raise_for_status()
    
    def execute_bulk_delete(self, soql_query: str, object_name: str, batch_size: int = 10000) -> dict:
        print("\n=== Starting bulk deletion operation ===")
        logging.info("Starting bulk deletion operation")
        print(f"SOQL Query: {soql_query}")
        logging.info(f"SOQL Query: {soql_query}")
        
        results = []
        query_result = self.sf.query_all(soql_query)
        
        for record in query_result['records']:
            results.append(record['Id'])
        
        total_records = len(results)
        print(f"\nFound {total_records} records to delete")
        logging.info(f"Found {total_records} records to delete")
        
        batches = [results[i:i + batch_size] for i in range(0, len(results), batch_size)]
        print(f"Number of batches created: {len(batches)}")
        logging.info(f"Number of batches created: {len(batches)}")
        
        final_results = {
            'total_records': total_records,
            'successful_deletes': 0,
            'failed_deletes': 0
        }
        
        for i, batch in enumerate(batches, 1):
            print(f"\n=== Processing batch {i}/{len(batches)} ===")
            logging.info(f"Processing batch {i}/{len(batches)}")
            job_id = self.create_delete_job(object_name)
            print(f"Assigned job ID: {job_id}")
            logging.info(f"Assigned job ID for batch {i}: {job_id}")
            
            self.upload_data_for_deletion(job_id, batch)
            self.close_job(job_id)
            
            while True:
                status = self.check_job_status(job_id)
                if status['state'] in ['JobComplete', 'Failed', 'Aborted']:
                    final_results['successful_deletes'] += status.get('numberRecordsProcessed', 0)
                    final_results['failed_deletes'] += status.get('numberRecordsFailed', 0)
                    print(f"Batch {i} completed!")
                    logging.info(f"Batch {i} completed: {status}")
                    break
                print("Waiting for job to complete...")
                logging.info(f"Waiting for job {job_id} to complete...")
                time.sleep(5)
        
        print("\n=== Final summary ===")
        logging.info("Final summary of bulk deletion process")
        print(f"Total records: {final_results['total_records']}")
        logging.info(f"Total records: {final_results['total_records']}")
        print(f"Successful deletions: {final_results['successful_deletes']}")
        logging.info(f"Successful deletions: {final_results['successful_deletes']}")
        print(f"Failed deletions: {final_results['failed_deletes']}")
        logging.info(f"Failed deletions: {final_results['failed_deletes']}")
        
        return final_results

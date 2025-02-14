import requests
import json
import time
import logging
from simple_salesforce import Salesforce
import pandas as pd
from typing import Optional, Dict, List
import concurrent.futures

# Configurazione base del logging per scrivere su bulk_delete.log
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
        print("=== Inizializzazione SalesforceBulkDelete ===")
        logging.info("=== Inizializzazione SalesforceBulkDelete ===")

        self.sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain
        )
        print(f"Connesso a Salesforce: {self.sf.base_url}")
        logging.info(f"=== Connesso a Salesforce: {self.sf.base_url} ===")

        self.instance_url = self.sf.base_url
        self.headers = {
            'Authorization': f'Bearer {self.sf.session_id}',
            'Content-Type': 'application/json'
        }
        
    def create_delete_job(self, sobject_name: str) -> str:
        endpoint = f"{self.instance_url}jobs/ingest"
        
        body = {
            "operation": "delete",
            "object": sobject_name,
            "contentType": "CSV",
            "lineEnding": "LF"
        }
        
        response = requests.post(endpoint, headers=self.headers, json=body)
        response.raise_for_status()
        job_id = response.json()['id']
        logging.info(f"=== Creazione nuovo job di cancellazione: Risposta creazione job: {response.status_code} - Job ID creato: {job_id}")
        print(f"=== Creazione nuovo job di cancellazione: Risposta creazione job: {response.status_code} - Job ID creato: {job_id}")
        return job_id
    
    def upload_data_for_deletion(self, job_id: str, ids_to_delete: list) -> None:
        print(f"\n=== Caricamento dati per job {job_id}: Numero di record da cancellare: {len(ids_to_delete)}")
        logging.info(f"=== Caricamento dati per job {job_id}: Numero di record da cancellare: {len(ids_to_delete)}")

        endpoint = f"{self.instance_url}jobs/ingest/{job_id}/batches"
        
        csv_data = "Id\n" + "\n".join(ids_to_delete)
        
        headers = self.headers.copy()
        headers['Content-Type'] = 'text/csv'
        
        response = requests.put(endpoint, headers=headers, data=csv_data)
        print(f"Stato upload: {response.status_code}")
        logging.info(f"Stato upload per job {job_id}: {response.status_code}")
        response.raise_for_status()
    
    def check_job_status(self, job_id: str) -> dict:
        endpoint = f"{self.instance_url}jobs/ingest/{job_id}"
        
        response = requests.get(endpoint, headers=self.headers)
        status = response.json()
        print(f"\n=== Controllo stato job {job_id}: stato {status['state']} - Record processati: {status.get('numberRecordsProcessed', 0)} - Record falliti: {status.get('numberRecordsFailed', 0)}")
        logging.info(f"=== Controllo stato job {job_id}: stato {status['state']} - Record processati: {status.get('numberRecordsProcessed', 0)} - Record falliti: {status.get('numberRecordsFailed', 0)}")
        response.raise_for_status()
        return status
    
    def close_job(self, job_id: str) -> None:
        endpoint = f"{self.instance_url}jobs/ingest/{job_id}"
        body = {"state": "UploadComplete"}
        
        response = requests.patch(endpoint, headers=self.headers, json=body)
        print(f"=== Chiusura job {job_id}: Stato chiusura job: {response.status_code}")
        logging.info(f"=== Chiusura job {job_id}: Stato chiusura job: {response.status_code}")
        response.raise_for_status()

    def execute_bulk_delete(self, soql_query: str, object_name: str, batch_size: int = 10000, max_workers: int = 5) -> dict:
        print("\n=== Avvio operazione di cancellazione bulk in parallelo ===")
        print(f"=== Query SOQL: {soql_query}")
        logging.info(f"=== Avvio operazione di cancellazione bulk in parallelo ===")
        logging.info(f"=== Query SOQL: {soql_query}")
        
        results = []
        query_result = self.sf.query_all(soql_query)
        
        for record in query_result['records']:
            results.append(record['Id'])
        
        total_records = len(results)
        print(f"\nTrovati {total_records} record da cancellare")
        logging.info(f"Trovati {total_records} record da cancellare")
        
        batches = [results[i:i + batch_size] for i in range(0, len(results), batch_size)]
        print(f"Numero di batch creati: {len(batches)}")
        logging.info(f"Numero di batch creati: {len(batches)}")
        
        final_results = {
            'total_records': total_records,
            'successful_deletes': 0,
            'failed_deletes': 0
        }

        def process_batch(batch: List[str]) -> Dict:
            job_id = self.create_delete_job(object_name)
            self.upload_data_for_deletion(job_id, batch)
            self.close_job(job_id)
            
            while True:
                status = self.check_job_status(job_id)
                if status['state'] in ['JobComplete', 'Failed', 'Aborted']:
                    return {
                        'processed': status.get('numberRecordsProcessed', 0),
                        'failed': status.get('numberRecordsFailed', 0)
                    }
                time.sleep(5)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}
            
            for future in concurrent.futures.as_completed(future_to_batch):
                result = future.result()
                final_results['successful_deletes'] += result['processed']
                final_results['failed_deletes'] += result['failed']

        print("\n=== Riepilogo finale ===")
        print(f"Record totali: {final_results['total_records']}")
        print(f"Cancellazioni riuscite: {final_results['successful_deletes']}")
        print(f"Cancellazioni fallite: {final_results['failed_deletes']}")
        logging.info(f"\n\n=== Riepilogo finale del processo di cancellazione bulk ===\nRecord totali: {final_results['total_records']}\nCancellazioni riuscite: {final_results['successful_deletes']}\nCancellazioni fallite: {final_results['failed_deletes']}")
        
        return final_results

import os
from dotenv import load_dotenv
from salesforce_bulk_delete import SalesforceBulkDelete

# Carica variabili d'ambiente da .env
load_dotenv()

# Leggi credenziali e configurazioni
username = os.getenv("SF_USERNAME")
password = os.getenv("SF_PASSWORD")
security_token = os.getenv("TOKEN")
domain = os.getenv("DOMAIN")
soql_query = os.getenv("SOQL_QUERY")
object_name = os.getenv("OBJECT_NAME")
batch_size = int(os.getenv("BATCH_SIZE", "10000"))
max_workers = int(os.getenv("MAX_WORKERS", "5"))

print(f"Username: {username}")
print(f"Password: {password}")
print(f"Security Token: {security_token}")
print(f"Domain: {domain}")
print(f"SOQL Query: {soql_query}")
print(f"Object Name: {object_name}")
print(f"Batch Size: {batch_size}")
print(f"Max Workers: {max_workers}")

# Inizializza il client SalesforceBulkDelete
bulk_delete = SalesforceBulkDelete(
    username=username,
    password=password,
    security_token=security_token,
    domain=domain
)

# Esegui la cancellazione bulk in parallelo
results = bulk_delete.execute_bulk_delete(
    soql_query, 
    object_name=object_name,
    batch_size=batch_size,
    max_workers=max_workers
)

# Stampa i risultati
print("Risultati cancellazione:")
print(f"Record totali: {results['total_records']}")
print(f"Cancellazioni riuscite: {results['successful_deletes']}")
print(f"Cancellazioni fallite: {results['failed_deletes']}")

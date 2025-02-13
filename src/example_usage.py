import os
from dotenv import load_dotenv
from salesforce_bulk_delete import SalesforceBulkDelete

# Load environment variables from .env
load_dotenv()

# Read credentials and query from environment variables
username = os.getenv("SF_USERNAME")
password = os.getenv("SF_PASSWORD")
security_token = os.getenv("TOKEN")
domain = os.getenv("DOMAIN")
soql_query = os.getenv("SOQL_QUERY")
object_name = os.getenv("OBJECT_NAME")

print(f"Username: {username}")
print(f"Password: {password}")
print(f"Security Token: {security_token}")
print(f"Domain: {domain}")
print(f"SOQL Query: {soql_query}")
print(f"Object Name: {object_name}")

# Initialize the SalesforceBulkDelete client
bulk_delete = SalesforceBulkDelete(
    username=username,
    password=password,
    security_token=security_token,
    domain=domain
)

# Execute the bulk deletion
results = bulk_delete.execute_bulk_delete(soql_query, object_name=object_name)

# Print out deletion results
print("Deletion results:")
print(f"Total records: {results['total_records']}")
print(f"Successful deletions: {results['successful_deletes']}")
print(f"Failed deletions: {results['failed_deletes']}")

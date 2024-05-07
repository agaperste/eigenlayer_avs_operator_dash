import requests
import pandas as pd
import os
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Function to initialize Dune client from environment variables
def get_dune_client():
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path)
    return DuneClient.from_env()

# Function to run a query and retrieve metadata URIs along with operator addresses
def fetch_metadata_uris(dune, query_id):
    query = QueryBase(name="Fetch Metadata URIs", query_id=query_id)
    attempts = 0
    while attempts < 20:
        try:
            results_df = dune.run_query_dataframe(query)
            return results_df[['metadataURI', 'operator']]  # Adjust column names as needed
        except Exception as e:
            attempts += 1
            print(f"Failed to fetch metadata URIs. Attempt {attempts}. Error: {e}")
    print("Failed to fetch metadata URIs after 20 attempts. Exiting script.")
    exit(1)

# Helper function to return default metadata dictionary
def default_metadata(operator_address):
    return {
        'operator_name': '',
        'operator_contract_address': operator_address,
        'website': '',
        'twitter': '',
        'logo': '',
        'description': ''
    }

# Function to fetch and parse metadata from a URI, including the operator address
def fetch_metadata(uri, operator_address):
    if not uri:
        print(f"Missing URI: {uri}")
        return default_metadata(operator_address)

    try:
        response = requests.get(uri)
        if response.status_code == 200:
            data = response.json()
            return {
                'operator_name': data.get('name', ''),
                'operator_contract_address': operator_address,
                'website': data.get('website', ''),
                'twitter': data.get('twitter', ''),
                'logo': data.get('logo', ''),
                'description': data.get('description', '')
            }
        else:
            print(f"Failed to fetch data for URI: {uri} with status code {response.status_code}")
            return default_metadata(operator_address)
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for URI: {uri}. Error: {e}")
        return default_metadata(operator_address)
# Function to delete a table from Dune
def delete_existing_table(dune, namespace, table_name):
    try:
        dune.delete_table(namespace=namespace, table_name=table_name)
        print(f"Successfully deleted the table: {table_name}")
    except Exception as e:
        print(f"Failed to delete the table: {table_name}. Error: {e}")


# Main function to execute the workflow
def main():
    dune = get_dune_client()

    # Fetch metadata
    metadata_info = fetch_metadata_uris(dune, 3685692)  # Update this query_id as per your actual ID

    # List to hold all metadata for CSV creation
    metadata_records = []

    for _, row in metadata_info.iterrows():
        metadata = fetch_metadata(row['metadataURI'], row['operator'])
        metadata_records.append(metadata)
    
    # Convert list of dictionaries to DataFrame
    metadata_df = pd.DataFrame(metadata_records)
    metadata_csv = metadata_df.to_csv(index=False)

    # Step to delete the existing table before uploading new data
    delete_existing_table(dune, "dune", "dataset_eigenlayer_operator_metadata")

    # Upload to Dune
    upload_success = dune.upload_csv(
        table_name="dataset_eigenlayer_operator_metadata",
        data=metadata_csv,
        is_private=False
    )
    print("Upload Successful:", upload_success)

if __name__ == "__main__":
    main()



'''
Script logic: 

(than you chatgpt who makes wrote my script for me based on this logic blurb)

- Get metadataURI per operator from query https://dune.com/queries/3685692
    - use Python SDK to trigger run and get results 
    - use this function  
    def run_query(
            self,
            query: QueryBase,
            ping_frequency: int = POLL_FREQUENCY_SECONDS,
            performance: Optional[str] = None,
            batch_size: Optional[int] = None,
            columns: Optional[List[str]] = None,
            sample_count: Optional[int] = None,
            filters: Optional[str] = None,
            sort_by: Optional[List[str]] = None,
        ) -> ResultsResponse:
            """
            Executes a Dune `query`, waits until execution completes,
            fetches and returns the results.
            Sleeps `ping_frequency` seconds between each status request.
            """
    
- loop through each metadataURI and get metadata 
    - it's a json so I want to parse like this 
        json_extract_scalar(json_parse(http_get(metadataURI)), '$.name') AS operator_name
        # , operator as operator_contract_address
        , json_extract_scalar(json_parse(http_get(metadataURI)), '$.website') AS website
        , json_extract_scalar(json_parse(http_get(metadataURI)), '$.twitter') AS twitter
        , json_extract_scalar(json_parse(http_get(metadataURI)), '$.logo') AS logo
        , json_extract_scalar(json_parse(http_get(metadataURI)), '$.description') AS description
- join the metadata back to the operator to get a CSV table with schema like this and save it in a csv file
    operator_name, operator_contract_address, website, twitter, logo, description 
- upload this to Dune as a new table
    - use Python SDK to upload the table to Dune
    - relevant SDK function 
        def upload_csv(
            self,
            table_name: str,
            data: str,
            description: str = "",
            is_private: bool = False,
        ) -> bool:
'''
import os
from dotenv import load_dotenv
from pyairtable import Api
from .customresponse import CustomResponse
from RikPy.commonfunctions import rfplogger

### AIRTABLE CREDENTIALS
load_dotenv()  
airtable_pat = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
airtable_base_id = os.getenv("AIRTABLE_BASE_ID")

def airtable_update_filtered_rows(table_name, filter_formula, data):
    """
    Update rows in an Airtable table based on a filter formula.

    Args:
    - table_name (str): The name of the table where the rows will be updated.
    - filter_formula (str): A formula to filter the rows. Example: "{team}='vinissimus'"
    - data (dict): A dictionary containing the field names and values to update.

    Returns:
    - list: A list of updated records.
    """
    try:
        # Initialize the API with your API key
        api = Api(airtable_pat)
        table = api.table(airtable_base_id, table_name)

        # Fetch records from the table based on the filter formula
        records_to_update = table.all(formula=filter_formula)
        updated_records = []

        # Update each fetched record with the new data
        for record in records_to_update:
            record_id = record['id']
            updated_record = table.update(record_id, data)
            updated_records.append(updated_record)

        return CustomResponse(data=updated_records, status_code=200)
        
    except Exception as e:
        message = f"Failed to update data: {e}"
        print(message)
        return CustomResponse(data=message, status_code=400)

def airtable_fetch_filtered_rows(table_name, filter_formula="", fields="", exclude_fields=[]):
    """
    Fetch rows from a specified Airtable table filtered by a custom formula.

    Args:
    - airtable_api_key (str): Your Airtable API key.
    - base_id (str): The Base ID of the Airtable base.
    - table_name (str): The name of the table from which to fetch the rows.
    - filter_formula (str): A formula to filter the rows. Example: "team = 13"
    - fields example     fields = ['content_group', 'type']

    Returns:
    - list: A list of rows (records) that match the filter formula from the table.
    """
    
    try:
        # Initialize the API with your API key
        api = Api(airtable_pat)
        
        # Access the specified table in the base
        table = api.table(airtable_base_id, table_name)
        
        # Fetch records from the table based on the filter formula
        if fields is None or fields == "":
            records = table.all(formula=filter_formula)
        else:
            records = table.all(formula=filter_formula, fields=fields)

        # Exclude specified fields if exclude_fields is provided
        if exclude_fields is not None and exclude_fields != "":
            for record in records:
                # Remove specified fields while keeping the rest of the record
                record['fields'] = {k: v for k, v in record['fields'].items() if k not in exclude_fields}

        return CustomResponse(data=records, status_code=200)

    except Exception as e:
        message = f"Failed to fetch data: {e}"
        print(message)
        return CustomResponse (data=message, status_code=400)

def airtable_delete_filtered_rows(table_name, filter_formula=""):
    """
    Delete rows from a specified Airtable table based on a filter formula.

    Args:
    - table_name (str): The name of the table from which to delete the rows.
    - filter_formula (str): A formula to filter the rows. Example: "team = 13"

    Returns:
    - str: A message indicating the result of the deletion operation.
    """
    try:
        # Initialize the API with your API key
        api = Api(airtable_pat)
        
        # Access the specified table in the base
        table = api.table(airtable_base_id, table_name)
        
        # Fetch records from the table based on the filter formula
        records_to_delete = table.all(formula=filter_formula)

        # Delete each fetched record
        for record in records_to_delete:
            table.delete(record['id'])
        
        print(f"--- Deleted {len(records_to_delete)} records matching the filter formula.")

        return CustomResponse(data=records_to_delete, status_code=200)

    except Exception as e:
        message = f"Failed to delete data: {e}"
        print(message)
        return CustomResponse (data=message, status_code=400)

def airtable_insert_row(table_name, data):
    """
    Insert a new row into an Airtable table.

    Args:
    - table_name (str): The name of the table where the row will be inserted.
    - data (dict): A dictionary containing the field names and values for the new row.

    Returns:
    - dict: The newly created record.

    # Example usage
    table_name = 'your_table_name_here'
    data = {
        'Name': 'John Doe',
        'Email': 'johndoe@example.com',
        'Age': 30
    }
    """
    try:
        # Initialize the API with your API key
        api = Api(airtable_pat)
        table = api.table(airtable_base_id, table_name)

        # Create a new record
        record = table.create(data)
        return CustomResponse(data=record, status_code=200)
        
    except Exception as e:
        message = (f"An error occurred: {e}")
        print(message)
        return CustomResponse(data=message, status_code=400)

def airtable_update_row(table_name, record_id, data):
    """
    Update an existing row in an Airtable table.

    Args:
    - table_name (str): The name of the table where the row will be updated.
    - record_id (str): The ID of the record to update.
    - data (dict): A dictionary containing the field names and values to update.

    Returns:
    - dict: The updated record.

    # Example usage
    table_name = 'your_table_name_here'
    record_id = 'rec123456789'
    data = {
        'Name': 'Jane Doe',
        'Email': 'janedoe@example.com',
        'Age': 32
    }
    """
    try:
        # Initialize the API with your API key
        api = Api(airtable_pat)
        table = api.table(airtable_base_id, table_name)

        # Update the record
        record = table.update(record_id, data)
        return CustomResponse(data=record, status_code=200)
        
    except Exception as e:
        message = (f"An error occurred: {e}")
        print(message)
        return CustomResponse(data=message, status_code=400)

def airtable_fetch_specific_row(table_name, record_id, exclude_fields=[]):
    """
    Fetch a specific row from a specified Airtable table by its record ID.

    Args:
    - table_name (str): The name of the table from which to fetch the row.
    - record_id (str): The ID of the record to retrieve.
    - exclude_fields (list): A list of fields to exclude from the response.

    Returns:
    - dict: The record data if found, otherwise None.
    """

    try:
        # Initialize the API with your personal access token
        api = Api(airtable_pat)
        
        # Access the specified table in the base
        table = api.table(airtable_base_id, table_name)
        
        # Fetch the specific record by its ID
        record = table.get(record_id)

        # Exclude specified fields if exclude_fields is provided
        if exclude_fields is not None and exclude_fields != "":          
            record_fields = {k: v for k, v in record['fields'].items() if k not in exclude_fields}
            record['fields'] = record_fields  # Update the record to exclude specified fields

        return CustomResponse(data=record, status_code=200)

    except Exception as e:
        message = f"Failed to fetch data: {e}"
        print(message)
        return CustomResponse(data=message, status_code=400)

def airtable_fetch_record_id_by_key(table_name, key_column, key_value):
    """
    Fetch the record ID from a specified Airtable table by a key column value.

    Args:
    - table_name (str): The name of the table from which to fetch the record ID.
    - key_column (str): The name of the key column to search by.
    - key_value (str): The value to search for in the key column.

    Returns:
    - dict: The record ID if found, otherwise None.
    """
    try:
        # Initialize the API with your personal access token
        api = Api(airtable_pat)
        
        # Access the specified table in the base
        table = api.table(airtable_base_id, table_name)
        
        # Fetch records where the key_column matches the key_value
        records = table.all(formula=f"{{{key_column}}}='{key_value}'")
        
        if records:
            # Assuming you want the first match
            record_id = records[0]['id']
            return CustomResponse(data={'record_id': record_id}, status_code=200)
        else:
            return CustomResponse(data="No matching record found", status_code=404)

    except Exception as e:
        message = f"Failed to fetch record ID: {e}"
        print(message)
        return CustomResponse(data=message, status_code=400)

###### ONLY FOR TEST PURPOSES
def main():
    # Configuration
    TOKEN = airtable_pat
    BASE_ID = airtable_base_id
    TABLE_NAME = 'tbltkwTvbQIZpyIHd' # teams

    # Retrieve all records from teams table
    filter_formula = "{team}='vinissimus'" 
    fields = ['team', 'campaign_feed']
    fields=['Team']
    fields=None
    fields = ['team', 'campaign_feed']
    fields = ""
    record_id = "recfUYWiJVKbF6y8B" # vinissimus
   
    exclude_fields = ""
    exclude_fields = ['Generation_content', 'Banner_groups', 'usage_records', 'guidance_records', 'Campaigns', 'Banner_templates'] 
    custom_response = airtable_fetch_filtered_rows(table_name=TABLE_NAME, filter_formula=filter_formula, fields=fields, exclude_fields=exclude_fields)

    print(f"airtable_fetch_filtered_rows: {custom_response.data}")
    
    custom_response = airtable_fetch_specific_row(table_name=TABLE_NAME, record_id=record_id, exclude_fields=exclude_fields)        

    print(f"airtable_fetch_specific_row: {custom_response.data}")


if __name__ == '__main__':
    main()

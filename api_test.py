import requests
import json

# Define the API endpoint
url = "http://127.0.0.1:5000/run_pipeline"

# Define the file to upload
file_path = r"json_data.json"

# Define the tasks order
tasks_order = ["duplicate_rows", "missing_values","email_validity"]

# Define the tolerance dictionary (optional)
tolerance_dict = {
    "ID": 0,
    "Name": 3,
    "Age": 2
}

# Define columns for duplicate check (optional)
columns_for_duplicates = ["ID","Name"]

# Prepare the payload
payload = {
    'tasks': json.dumps(tasks_order),
    'tolerance_dict': json.dumps(tolerance_dict),
    'columns_for_duplicates': json.dumps(columns_for_duplicates)
}

# Prepare the files to upload
files = {
    'file': open(file_path, 'rb')
}

# Send the request
response = requests.post(url, data=payload, files=files)

# Print the response
print(response.status_code)
print(json.dumps(response.json(), indent=4))

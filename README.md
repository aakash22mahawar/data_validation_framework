# **Project Overview**

This project implements a configurable data pipeline framework using Flask and Python, designed for scalable and reusable data processing tasks. It includes functionalities for:

**Data Handling:** Processes JSON data into pandas DataFrames for manipulation and analysis.

**Pipeline Execution:** Executes sequential data processing tasks based on user-defined configurations.

**Task Modules:** Includes modules such as DuplicateRowsCheck, MissingValuesCheck, EmailValidityCheck, and GenderValidityCheck for specific data validations and analyses.

**Reporting:** Generates structured reports for each task executed within the pipeline, maintaining order as specified by the user.

**Flexibility:** Designed with extensibility in mind to easily integrate additional data processing tasks in the future.

## **Dependencies and Installation**

* flask
* pandas
* reqeusts  
```python  
pip install -r requirements.txt
```
## **Usage**

### To run the pipeline using an HTTP request, follow these steps:

1. Endpoint: /run_pipeline
2. Method: POST
3. Request Body:  
    - file: Upload a JSON file containing data for analysis.
    - tasks: Specify tasks in the order they should be executed.[DuplicateRowsCheck,MissingValuesCheck]
    - additional parameters as required (e.g., tolerance_dict, columns_for_duplicates)
   

## **Example HTTP Request**   
```python
import requests
import json

# Define the API endpoint
url = "http://127.0.0.1:5000/run_pipeline"

# Define the file to upload
file_path = r"C:\Users\AakashMahawar\Downloads\json_data.json"

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
```
## **Example HTTP Response**  
```python
{
    "message": "Pipeline executed successfully",
    "reports": [
        {
            "duplicate_rows": [
                {
                    "Total Duplicate Rows": 4
                }
            ]
        },
        {
            "missing_values": [
                {
                    "Column": "ID",
                    "Exceeds Tolerance": true,
                    "Null Tolerance (%)": 2,
                    "Tolerable (%)": 0
                },
                {
                    "Column": "Name",
                    "Exceeds Tolerance": false,
                    "Null Tolerance (%)": 3,
                    "Tolerable (%)": 3
                },
                {
                    "Column": "Age",
                    "Exceeds Tolerance": false,
                    "Null Tolerance (%)": 2,
                    "Tolerable (%)": 2
                }
            ]
        },
        {
            "email_validity": [
                {
                    "Category": "Valid",
                    "Count": 96
                },
                {
                    "Category": "Invalid",
                    "Count": 2
                },
                {
                    "Category": "Missing",
                    "Count": 4
                }
            ]
        }
    ]
}
```
## **Future Improvements**
* Enhance error handling and logging capabilities.
* Integrate more data validation tasks.
* Implement authentication and authorization mechanisms for API access.

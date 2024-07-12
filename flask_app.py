from flask import Flask, request, jsonify
import pandas as pd
import json
import logging
from pipeline import *

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


@app.route('/run_pipeline', methods=['POST'])
def run_pipeline():
    try:
        # Reset the pipeline steps for each request
        task_steps = []

        if 'file' in request.files:
            # Read the uploaded file
            file = request.files['file']
            data = json.load(file)
        else:
            return jsonify({'error': 'Invalid input format. Expected a JSON file for data.'}), 400

        # Use pandas to convert JSON to DataFrame
        if 'data' in data:
            df = pd.DataFrame(data['data'])
        else:
            return jsonify(
                {'error': 'Invalid input format. JSON file should contain "data" key with a list of records.'}), 400

        # Get tasks order from form-data
        if 'tasks' in request.form:
            tasks_order = json.loads(request.form['tasks'])
        else:
            return jsonify({'error': 'Invalid input format. Expected tasks in form-data.'}), 400

        # Get tolerance dict from form-data (optional for missing values check)
        tolerance_dict = {}
        if 'tolerance_dict' in request.form:
            tolerance_dict = json.loads(request.form['tolerance_dict'])

        # Get columns for duplicate check from form-data (optional)
        columns_for_duplicates = []
        if 'columns_for_duplicates' in request.form:
            columns_for_duplicates = json.loads(request.form['columns_for_duplicates'])

        # Define reports list to maintain order
        reports = []

        for task_name in tasks_order:
            if task_name == "missing_values":
                step = MissingValuesCheck(tolerance_dict)
                df, report = step.run(df)
                reports.append({
                    'task_name': 'missing_values',
                    'report': report
                })
                task_steps.append(step)
            elif task_name == "duplicate_rows":
                step = DuplicateRowsCheck(columns_for_duplicates)
                df, report = step.run(df)
                reports.append({
                    'task_name': 'duplicate_rows',
                    'report': report
                })
                task_steps.append(step)
            elif task_name == "email_validity":
                step = EmailValidityCheck()
                df, report = step.run(df)
                reports.append({
                    'task_name': 'email_validity',
                    'report': report
                })
                task_steps.append(step)
            elif task_name == "gender_string_validity":
                step = GenderValidityCheck()
                df, report = step.run(df)
                reports.append({
                    'task_name': 'gender_string_validity',
                    'report': report
                })
                task_steps.append(step)

        # Prepare response with execution report in specified order
        response = {
            'message': 'Pipeline executed successfully',
            'reports': [{task['task_name']: task['report']} for task in reports]
        }

        # Logging the reports for verification and reporting
        for idx, report in enumerate(reports, start=1):
            logging.info(f"Task {idx}: {report['task_name']} executed with report: {report['report']}")

        return jsonify(response), 200

    except Exception as e:
        logging.error(f"Error in executing pipeline: {str(e)}")
        return jsonify({'error': 'Internal Server Error'}), 500


if __name__ == '__main__':
    app.run(debug=True)

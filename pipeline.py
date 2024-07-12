import pandas as pd
import re
import logging
from abc import ABC, abstractmethod


class PipelineStep(ABC):
    @abstractmethod
    def run(self, df):
        pass


class DataPipeline:
    def __init__(self, steps):
        self.steps = steps

    def run(self, df):
        report = []
        for step in self.steps:
            df, step_report = step.run(df)
            report.append(step_report)
        return df, report

    @staticmethod
    def get_task(name):
        tasks = {
            'missing_values': MissingValuesCheck,
            'duplicate_rows': DuplicateRowsCheck,
            'email_validity': EmailValidityCheck,
            'gender_string_validity': GenderValidityCheck
        }
        return tasks.get(name, None)


class MissingValuesCheck(PipelineStep):
    def __init__(self, tolerance_dict):
        self.tolerance_dict = tolerance_dict

    def null_tolerance_col(self, df, col_name):
        length_df = len(df)
        null_count = df[col_name].isna().sum()
        percent_tolerance = round(null_count * 100 / length_df)
        return percent_tolerance

    def run(self, df):
        result = []
        for col, tolerance in self.tolerance_dict.items():
            if col in df.columns:
                null_tolerance = self.null_tolerance_col(df, col)
                item = {
                    'Column': col,
                    'Null Tolerance (%)': null_tolerance,
                    'Tolerable (%)': tolerance,
                    'Exceeds Tolerance': null_tolerance > tolerance
                }
                result.append(item)
            else:
                logging.warning(f"Column '{col}' not found in the DataFrame.")

        result_df = pd.DataFrame(result)
        return df, result_df.to_dict(orient='records')


class DuplicateRowsCheck(PipelineStep):
    def __init__(self, columns):
        self.columns = columns

    def run(self, df):
        if self.columns:
            duplicates = df[df.duplicated(subset=self.columns, keep=False)]
        else:
            duplicates = df[df.duplicated(keep=False)]

        duplicate_count = len(duplicates)
        result_df = pd.DataFrame([{'Total Duplicate Rows': duplicate_count}])
        return df, result_df.to_dict(orient='records')


class EmailValidityCheck(PipelineStep):
    def run(self, df):
        valid_count = 0
        invalid_count = 0
        missing_count = 0

        for email in df['Email']:
            if pd.isna(email) or email == '':
                missing_count += 1
            else:
                pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
                if re.match(pattern, email):
                    valid_count += 1
                else:
                    invalid_count += 1

        item = {
            'Category': ['Valid', 'Invalid', 'Missing'],
            'Count': [valid_count, invalid_count, missing_count]
        }

        result_df = pd.DataFrame(item)
        return df, result_df.to_dict(orient='records')


class GenderValidityCheck(PipelineStep):
    def run(self, df):
        valid_count = 0
        invalid_count = 0
        missing_count = 0

        for gender in df['Gender']:
            if pd.isna(gender) or gender == '':
                missing_count += 1
            elif gender in ['M', 'F']:
                valid_count += 1
            else:
                invalid_count += 1

        item = {
            'Category': ['Valid', 'Invalid', 'Missing'],
            'Count': [valid_count, invalid_count, missing_count]
        }

        result_df = pd.DataFrame(item)
        return df, result_df.to_dict(orient='records')

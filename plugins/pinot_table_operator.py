import json
from pathlib import Path
from typing import Optional

import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context


class PinotTableSubmitOperator(BaseOperator):
    """
    Operator to submit Pinot table JSON files from a specified local folder to a given Pinot endpoint.

    This operator:
    - Validates that the given folder_path exists and is a directory.
    - Finds all JSON files in that directory.
    - For each JSON file, ensures it contains valid JSON.
    - POSTs each valid table configuration to the provided Pinot endpoint.
    - Raises AirflowException on invalid directory, invalid JSON, or non-200 responses.
    """

    template_fields = ('folder_path', 'pinot_url')

    def __init__(
            self,
            folder_path: str,
            pinot_url: str,
            *args,
            **kwargs
    ) -> None:
        """
        Initialize the PinotTableSubmitOperator.

        :param folder_path: The local directory path containing Pinot table JSON files.
        :param pinot_url: The Pinot endpoint URL to which table configurations will be submitted.
        """
        super().__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context: Context) -> Optional[str]:
        """
        Executes the operator steps:
        - Validate that folder_path is a directory.
        - Find *.json files in the directory.
        - For each file, check for valid JSON and submit it to Pinot via POST.
        - If no files are found, returns "No tables submitted."
        - Logs and raises exceptions for any errors encountered.

        :param context: The Airflow execution context.
        :return: A summary string indicating how many tables were submitted,
                 or "No tables submitted." if none were found.
        """
        folder = Path(self.folder_path)
        if not folder.is_dir():
            raise AirflowException(
                f"The provided folder_path '{self.folder_path}' is not a directory or doesn't exist."
            )

        table_files = list(folder.glob('*.json'))
        if not table_files:
            self.log.info("No table files found in %s. Nothing to submit.", self.folder_path)
            return "No tables submitted."

        self.log.info("Found %d table file(s) in %s.", len(table_files), self.folder_path)
        headers = {'Content-Type': 'application/json'}

        for table_file in table_files:
            self.log.info("Processing table file: %s", table_file)
            try:
                # Read file and validate JSON
                with table_file.open('r', encoding='utf-8') as f:
                    table_data = f.read()
                    json.loads(table_data)  # Validate JSON structure

                # POST request to Pinot endpoint
                response = requests.post(self.pinot_url, data=table_data, headers=headers)
                if response.status_code == 200:
                    self.log.info("Table successfully submitted: %s", table_file)
                else:
                    self.log.error("Failed to submit table file %s: %d - %s",
                                   table_file, response.status_code, response.text)
                    raise AirflowException(
                        f"Table submission failed for {table_file} with status code {response.status_code}"
                    )

            except json.JSONDecodeError as e:
                self.log.error("Invalid JSON in %s: %s", table_file, str(e))
                raise AirflowException(f"Invalid JSON table file in {table_file}: {e}")
            except Exception as e:
                self.log.exception("An unexpected error occurred while submitting %s", table_file)
                raise AirflowException(f"Error submitting table {table_file}: {e}")

        self.log.info("All table files have been successfully submitted.")
        return f"Submitted {len(table_files)} table(s) successfully."

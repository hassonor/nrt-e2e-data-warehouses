import json
from pathlib import Path
from typing import Optional

import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context


class PinotSchemaSubmitOperator(BaseOperator):
    """
    Operator to submit Pinot schema JSON files from a specified local folder to a given Pinot endpoint.

    This operator:
    - Validates that the given folder_path exists and is a directory.
    - Finds all JSON files in that directory.
    - For each JSON file, ensures it contains valid JSON.
    - POSTs each valid schema to the provided Pinot endpoint.
    - Raises AirflowException on invalid directory, invalid JSON, or non-200 responses.
    """

    template_fields = ("folder_path", "pinot_url")

    def __init__(self, folder_path: str, pinot_url: str, *args, **kwargs) -> None:
        """
        Initialize the PinotSchemaSubmitOperator.

        :param folder_path: The local directory path containing Pinot schema JSON files.
        :param pinot_url: The Pinot endpoint URL to which schemas will be submitted.
        """
        super().__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context: Context) -> Optional[str]:
        """
        Executes the operator steps:
        - Validate that folder_path is a directory.
        - Find *.json files in the directory.
        - For each file, check for valid JSON and submit to Pinot via POST.
        - If no files are found, returns "No schemas submitted."
        - Logs and raises exceptions for any errors encountered.

        :param context: The Airflow execution context.
        :return: A summary string indicating how many schemas were submitted,
                 or "No schemas submitted." if none were found.
        """
        folder = Path(self.folder_path)
        if not folder.is_dir():
            raise AirflowException(
                f"The provided folder_path '{self.folder_path}' is not a directory or doesn't exist."
            )

        schema_files = list(folder.glob("*.json"))
        if not schema_files:
            self.log.info(
                "No schema files found in %s. Nothing to submit.", self.folder_path
            )
            return "No schemas submitted."

        self.log.info(
            "Found %d schema file(s) in %s.", len(schema_files), self.folder_path
        )
        headers = {"Content-Type": "application/json"}

        for schema_file in schema_files:
            self.log.info("Processing schema file: %s", schema_file)
            try:
                # Read file and validate JSON
                with schema_file.open("r", encoding="utf-8") as f:
                    schema_data = f.read()
                    json.loads(schema_data)  # Validate JSON

                # POST request to Pinot endpoint
                response = requests.post(
                    self.pinot_url, data=schema_data, headers=headers
                )
                if response.status_code == 200:
                    self.log.info("Schema successfully submitted: %s", schema_file)
                else:
                    self.log.error(
                        "Failed to submit schema file %s: %d - %s",
                        schema_file,
                        response.status_code,
                        response.text,
                    )
                    raise AirflowException(
                        f"Schema submission failed for {schema_file} with status code {response.status_code}"
                    )

            except json.JSONDecodeError as e:
                self.log.error("Invalid JSON in %s: %s", schema_file, str(e))
                raise AirflowException(f"Invalid JSON schema in {schema_file}: {e}")
            except Exception as e:
                self.log.exception(
                    "An unexpected error occurred while submitting %s", schema_file
                )
                raise AirflowException(f"Error submitting schema {schema_file}: {e}")

        self.log.info("All schema files have been successfully submitted.")
        return f"Submitted {len(schema_files)} schema(s) successfully."

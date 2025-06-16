# The tutputal is here
# https://www.youtube.com/watch?v=F3yvXqzkDhU&list=PLYDwWPRvXB8_XOcrGlYLtmFEZywOMnGSS&ab_channel=JoanMedia




# prerequisites
# 1. Python 3.8+
# 2. pip install great_expectations
# 3. pip install pandas


import great_expectations as gx
import pandas as pd

print("Great Expectations version:", gx.__version__)
print("pandas version:", pd.__version__)

# Load your CSV file
df = pd.read_csv("C:\\\\Users\\\\TechTeam-08\\\\PycharmProjects\\\\Essential Data Quality Principles, Implement Testing with Python and Great Expectations Framework\\\\temperature.csv")

# Get the default context (ephemeral in-memory)
context = gx.get_context()

# Add a pandas datasource manually using config
datasource_config = {
    "name": "my_pandas_ds",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "PandasExecutionEngine"
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"]
        }
    }
}
context.add_or_update_datasource(**datasource_config)

# Create a batch request
from great_expectations.core.batch import RuntimeBatchRequest

batch_request = RuntimeBatchRequest(
    datasource_name="my_pandas_ds",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="temp_asset",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"default_identifier_name": "batch_001"}
)

validator = context.get_validator(batch_request=batch_request)


# Validate temperature range
validator.expect_column_values_to_be_between(
    column="Temperature",
    min_value=10,
    max_value=45
)

# Run validation
results = validator.validate()
print(results)

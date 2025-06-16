# The tutputal is here
# https://www.youtube.com/watch?v=F3yvXqzkDhU&list=PLYDwWPRvXB8_XOcrGlYLtmFEZywOMnGSS&ab_channel=JoanMedia




# prerequisites
# 1. Python 3.8+
# 2. pip install great_expectations
# 3. pip install pandas




import great_expectations as gx
import pandas as pd
from datetime import datetime, timedelta
from great_expectations.core.batch import RuntimeBatchRequest

# Load and clean the dataset
df = pd.read_csv("C:\\Users\\TechTeam-08\\PycharmProjects\\Essential Data Quality Principles, Implement Testing with Python and Great Expectations Framework\\temperature.csv")
df["Date"] = pd.to_datetime(df["Date"], errors='coerce')

# 👉 Print total number of rows
print(f"\n📊 Total records in dataset: {len(df)}")

# Set up context
context = gx.get_context()

# Add datasource
datasource_config = {
    "name": "my_pandas_ds",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"]
        }
    }
}
context.add_or_update_datasource(**datasource_config)

# Create batch request
batch_request = RuntimeBatchRequest(
    datasource_name="my_pandas_ds",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="temp_asset",
    runtime_parameters={"batch_data": df},
    batch_identifiers={"default_identifier_name": "batch_001"}
)

# Validator
validator = context.get_validator(batch_request=batch_request)

# Convert date to proper format for string matching
df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

# --- Group Expectations by Metric Type ---
expectation_results = {
    "completeness": [],
    "accuracy": [],
    "validity": [],
    "uniqueness": [],
    "consistency": [],
    "timeliness": []
}

# Completeness Expectations
expectation_results["completeness"].append(
    validator.expect_column_values_to_not_be_null("Date")
)
expectation_results["completeness"].append(
    validator.expect_column_values_to_not_be_null("City")
)
expectation_results["completeness"].append(
    validator.expect_column_values_to_not_be_null("Temperature")
)

# Accuracy Expectations
expectation_results["accuracy"].append(
    validator.expect_column_values_to_be_between("height", min_value=10, max_value=45)
)

# Validity Expectations
expectation_results["validity"].append(
    validator.expect_column_values_to_be_of_type("Temperature", type_="int64")
)
expectation_results["validity"].append(
    validator.expect_column_values_to_match_regex("City", regex="^[A-Za-z\\s]+$")
)

# Consistency Expectations
expectation_results["consistency"].append(
    validator.expect_column_values_to_match_strftime_format("Date", "%Y-%m-%d")
)

# Timeliness Expectations
twelve_months_ago = datetime.now() - timedelta(days=365)
expectation_results["timeliness"].append(
    validator.expect_column_max_to_be_between("Date", min_value=twelve_months_ago)
)

# # Uniqueness Expectations
# expectation_results["uniqueness"].append(
#     validator.expect_column_values_to_be_unique("id")
# )

# --- Run all expectations ---
results = validator.validate()

# --- Print pass percentage per metric type ---
print("\n📋 Expectation Group Summary:")

for category, exps in expectation_results.items():
    total = len(exps)
    passed = sum(exp.success for exp in exps)
    percent = (passed / total) * 100 if total else 0
    print(f"  ✅ {category.capitalize():<12}: {passed}/{total} passed ({percent:.2f}%)")

# --- Print overall summary ---
all_expectations = [exp for group in expectation_results.values() for exp in group]
total_all = len(all_expectations)
passed_all = sum(exp.success for exp in all_expectations)
overall_percent = (passed_all / total_all) * 100 if total_all else 0

print(f"\n📊 Overall: {passed_all}/{total_all} expectations passed ({overall_percent:.2f}%)")

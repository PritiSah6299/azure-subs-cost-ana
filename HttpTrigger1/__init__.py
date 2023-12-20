import logging

import azure.functions as func

from azure.storage.blob import BlobServiceClient

import pandas as pd

from azure.identity import DefaultAzureCredential

from azure.keyvault.secrets import SecretClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    key_vault_url = "https://sead-doc-pub-sbx-kv001.vault.azure.net/"
    secret_name = "st-azpriti"

    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)


    connection_string =secret_client.get_secret(secret_name).value
    container_name = "az-cost"
    blob_name = "anonymized_costs.csv"

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    blob_data = blob_client.download_blob()
    content = blob_data.readall().decode('utf-8')

    df = pd.read_csv(pd.compat.StringIO(content))

    df['Date'] = pd.to_datetime(df['Date'])
    df['Month'] = df['Date'].dt.month
    df['Week'] = df['Date'].dt.isocalendar().week
    df['Day'] = df['Date'].dt.day

    monthly_expenses = df.groupby('Month')['CostInBillingCurrency'].sum()
    weekly_expenses = df.groupby('Week')['CostInBillingCurrency'].sum()
    daily_expenses = df.groupby('Day')['CostInBillingCurrency'].sum()


    most_expensive_month = monthly_expenses.idxmax()
    most_expensive_week = weekly_expenses.idxmax()
    most_expensive_day = daily_expenses.idxmax()

    df_sorted = df.sort_values(by='Date', ascending=False)
    most_recent_records = df_sorted.head(10)  

    return func.HttpResponse(
             f"This HTTP triggered function executed successfully.\n\n    monthly_expenses :- \n{monthly_expenses}\n\n  weekly_expenses :- \n{weekly_expenses}\n\n    daily_expenses :-\n{daily_expenses}\n\n\n\nmost_expensive_month{most_expensive_month}\n   most_expensive_week:-\n{most_expensive_week}\n  most_expensive_day:-\n{most_expensive_day}\n\n\n\nmost_recent_records:-{most_recent_records}",
             status_code=200
        )

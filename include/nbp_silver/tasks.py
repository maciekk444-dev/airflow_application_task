from pathlib import Path

from airflow.operators.python import get_current_context

def _dag_init():
    context = get_current_context()
    dag_run = context["dag_run"]
    execution_date = dag_run.execution_date

    print(f"Task is started : {execution_date}")
    return f"Task is started : {{ ds }}"


def _silver_transform(
    bronze_path: Path,
    parquet_file: Path,
    current_date: str,
    date: str,
    ingestion_timestamp: str,
    ingestion_dag_run: str,
):
    
    """
    Parse NBP XML from bronze layer into a cleaned pandas DataFrame, enrich with metadata,
    and write/update a partitioned Parquet dataset (deduplicated by currency and date).
    """

    import os
    import shutil
    import xml.etree.ElementTree as ET

    import pandas as pd
    
    full_path = bronze_path / f"nbp_table_{current_date}.xml"
    xml_data = open(full_path, "rb").read()  # Read file
    root = ET.XML(xml_data)  # Parse XML

    if len(root) == 0:
        raise ValueError("Empty XML")

    data = []
    for i, child in enumerate(root):
        data.append([subchild.text for subchild in child])

    cols = [subchild.tag for subchild in child]
    df = pd.DataFrame(data)  # Write in DF and transpose it
    df.columns = cols  # Update column names

    df = df.dropna(how="all")
    df["kurs_sredni"] = df["kurs_sredni"].str.replace(",", ".")

    # add metadata
    df["date"] = date
    df["ingestion_timestamp"] = ingestion_timestamp
    df["ingestion_dag_run"] = ingestion_dag_run
    df["ingestion_file_path"] = str(full_path)

    df = df.astype(
        {
            "nazwa_waluty": "object",
            "przelicznik": "int32",
            "kod_waluty": "object",
            "kurs_sredni": "float64",
            "date": "object",
            "ingestion_timestamp": "object",
            "ingestion_dag_run": "object",
            "ingestion_file_path": "object",
        }
    )

    # Append to existind parquet file

    # If file not exists - create it:
    if not os.path.exists(parquet_file):
        print("File does not exist. Creating parquet file...")
        df.to_parquet(parquet_file, partition_cols="date", index=False)
    # Else - read / check if required override/ append
    else:
        existing_df = pd.read_parquet(parquet_file)
        updated_df = pd.concat([existing_df, df], ignore_index=True)

        updated_df = updated_df.sort_values("ingestion_timestamp", ascending=False)
        updated_df = updated_df.drop_duplicates(
            subset=["date", "nazwa_waluty", "przelicznik", "kod_waluty"], keep="first"
        )
        shutil.rmtree(parquet_file)
        updated_df.to_parquet(parquet_file, partition_cols="date", index=False)

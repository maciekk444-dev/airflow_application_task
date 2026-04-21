from pathlib import Path
from datetime import datetime

from airflow.operators.python import get_current_context


def _dag_init() -> "str":
    context = get_current_context()
    dag_run = context["dag_run"]
    execution_date = dag_run.execution_date

    print(f"Task is started : {execution_date}")
    return f"Task is started : {{ ds }}"


def get_latest_nbp_table(url_archive: "str", current_date: "str") -> "str":
    import re
    import requests

    response = requests.get(url_archive)
    html = response.text

    pattern = r'(\d{2}\.\d{2}\.\d{4}).*?<a href="([^"]+)"'  #
    matches = re.findall(pattern, html)
    result = {datetime.strptime(date, "%d.%m.%Y"): name for date, name in matches}
    result = dict(
        filter(
            lambda item: item[0] <= datetime.strptime(current_date, "%Y%m%d"),
            result.items(),
        )
    )

    return result[max(result.keys())]


def _get_file(
    url_archive: "str", url_xml: "str", current_date: "str", raw_path: Path
) -> None:
    import requests

    # get most recent file based on execution date
    xml_name = get_latest_nbp_table(url_archive, current_date) 
    # download and save file
    resp = requests.get(url_xml + xml_name)
    filename = f"nbp_table_{current_date}.xml"
    file_path = raw_path / filename
    with open(file_path, "wb") as f:
        f.write(resp.content)
        f.close()

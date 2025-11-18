import requests
from datetime import datetime, timedelta
import pandas as pd

def main_fetch(execution_date, mailto, base_url) :

      if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

      domain_params = {
            'group_by': "primary_topic.domain.id:include_unknown",
            'mailto': mailto
            }

      response = requests.get(base_url, params=domain_params)
      response.raise_for_status()
      domain_data = response.json()

      row = {}
      row["execution_date"] = execution_date.strftime("%Y-%m-%d")
      row["total"] = domain_data["meta"]["count"]

      DOMAIN_KEYS = [
            "Physical_Sciences",
            "Life_Sciences",
            "Health_Sciences",
            "Social_Sciences",
            "unknown"
      ]

      for key in DOMAIN_KEYS:
            row[key] = 0

      for item in domain_data["group_by"]:
            name = item["key_display_name"]
            count = item["count"]

            safe_name = name.replace(" ", "_").replace("-", "_")

            if safe_name in DOMAIN_KEYS:
                  row[safe_name] = count

      oa_params = {
            'group_by': "is_oa:include_unknown",
            'mailto': mailto
            }
      response = requests.get(base_url, params=oa_params)
      response.raise_for_status()
      oa_data = response.json()

      for item in oa_data["group_by"]:
            name = item["key_display_name"]
            count = item["count"]

            if name == "true":
                  row["oa_true"] = count

      return row

def daily_fetch(execution_date, mailto, base_url):

      if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

      rows = []
      for i in range(7):
            publication_date = execution_date - timedelta(days=i)
            pub_str = publication_date.strftime("%Y-%m-%d")

            params = {
                  'group_by': "primary_topic.domain.id:include_unknown",
                  'filter' : f"publication_date:{pub_str}",
                  'mailto': mailto
                  }

            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

            row = {}
            row["execution_date"] = execution_date.strftime("%Y-%m-%d")
            row["publication_date"] = pub_str
            row["total"] = data["meta"]["count"]

            DOMAIN_KEYS = [
                  "Physical_Sciences",
                  "Life_Sciences",
                  "Health_Sciences",
                  "Social_Sciences",
                  "unknown"
            ]

            for key in DOMAIN_KEYS:
                  row[key] = 0

            for item in data["group_by"]:
                  name = item["key_display_name"]
                  count = item["count"]

                  safe_name = name.replace(" ", "_").replace("-", "_")

                  if safe_name in DOMAIN_KEYS:
                        row[safe_name] = count
            rows.append(row)

      return rows

def type_fetch(execution_date, mailto, base_url):

      if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

      params = {
            'group_by': "type:include_unknown",
            'mailto': mailto
            }

      response = requests.get(base_url, params=params)
      response.raise_for_status()
      data = response.json()

      row = {}

      for item in data["group_by"]:
            name = item["key_display_name"]
            count = item["count"]

            safe_name = name.replace(" ", "_").replace("-", "_")

            row["execution_date"] = execution_date.strftime("%Y-%m-%d")
            row[safe_name] = count

      return row

def field_fetch(execution_date, mailto, base_url, field_df):
      if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

      params = {
      'group_by': "primary_topic.field.id",
      'mailto': mailto
      }

      response = requests.get(base_url, params=params)
      response.raise_for_status()
      data = response.json()

      mapping = dict(zip(field_df["field"], field_df["domain"]))

      rows = []

      for item in data["group_by"]:
            name = item["key_display_name"]
            count = item["count"]

            safe_name = name.replace(" ", "_").replace("-", "_")

            row = {}
            row["execution_date"] = execution_date.strftime("%Y-%m-%d")
            row["domain"]= mapping.get(item["key_display_name"])
            row["field"] = safe_name
            row["count"] = count

            rows.append(row)

      return rows

def field_3_days_fetch(execution_date, mailto, base_url, field_df):
      if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

      start_datetime = execution_date - timedelta(days=3)
      start_date = start_datetime.strftime("%Y-%m-%d")
      end_date = execution_date.strftime("%Y-%m-%d")

      params = {
      'group_by': "primary_topic.field.id",
      'filter' : f"from_publication_date:{start_date},to_publication_date:{end_date}",
      'mailto': mailto
      }

      response = requests.get(base_url, params=params)
      response.raise_for_status()
      data = response.json()

      mapping = dict(zip(field_df["field"], field_df["domain"]))

      rows = []
      for item in data["group_by"]:
            name = item["key_display_name"]
            count = item["count"]
            safe_name = name.replace(" ", "_").replace("-", "_")

            row = {
                  "execution_date": execution_date.strftime("%Y-%m-%d"),
                  "start_date": start_date,
                  "end_date": end_date,
                  "domain": mapping.get(name),
                  "field": safe_name,
                  "count": count
            }
            rows.append(row)


      return rows
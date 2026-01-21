from dagster import op, job
import os

@op
def scrape():
    os.system("python src/scraper.py")

@op
def load():
    os.system("python src/load_to_postgres.py")

@op
def dbt_run():
    os.system("cd medical_warehouse && dbt run")

@job
def medical_pipeline():
    load(dbt_run(scrape()))

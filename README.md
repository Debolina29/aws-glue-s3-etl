# AWS Glue + S3 ETL Pipeline

This project demonstrates a beginner-friendly ETL pipeline using AWS Glue and S3.

## Pipeline Steps
- Upload CSV to S3
- Use AWS Glue to transform the data
- Save the output back to S3

## Tools Used
- AWS Glue
- AWS S3
- PySpark
- IAM Role

**Transformation Logic:**
In AWS Glue Studio, the following transformation was applied:

**Renamed column:** salary → monthly_salary
The logic was implemented using Glue Studio's visual job editor and auto-converted to a PySpark script (glue_script.py).

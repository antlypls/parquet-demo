Writing Parquet Demo
====================

Tiny project that shows how to write ProtoBuf objects to Parquet files.

Program requires following env variables to be defined:
* for writing to local file, `LOCAL_PATH` — path to local parquet file.
* fo writing to S3, `S3_PATH` — S3 bucket and key of a parquet file,
`AWS_ACCESS_KEY`, `AWS_SECRET_KEY` — AWS credentials.

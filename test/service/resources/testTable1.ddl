CREATE EXTERNAL TABLE IF NOT EXISTS ${TableName} (
  name string,
  happiness double,
  timestamp double,
  version string)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
LOCATION '${S3Location}'

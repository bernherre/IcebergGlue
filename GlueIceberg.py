import sys
from awsglue.utils import getResolvedOptions
from awsglue.transforms import (...)
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import (...)
from pyspark.sql.functions import (...)

args=getResolvedOptions(sys.ardv, 
                        [ ...])

var1= arg["..."]

config_especials={f"spark.sql.catalog.{catalog_name}":"org.apache.iceberg.spark.SparkCatalog",
                  f"spark.sql.catalog.{catalog_name}.warehouse":s3_warehouse,
                  f"spark.sql.catalog.{catalog_name}.catalog-impl":"org.apache.iceberg.aws.glue.GlueCatalog",
                  f"spark.sql.catalog.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", 
                  f"spark.sql.catalog.{catalog_name}.catalog-impl":"org.apache.iceberg.aws.s3.S3FileIO",
                  f"spark.sql.catalog.{catalog_name}.s3.sse.type":"s3",
}
### catalog name is normally iceberg_catalog and s3_warehouse is the path in s3 where is placed the physical metadata of iceberg


conf=SparkConf()
if config_espacials and isinstance(config_especials,dict):
    for k,v in config_especials.items():
      conf.set(k,v)
      
sc= SparkContext(conf=conf)
glueContext=GlueContext(sc)
spark=glueContext.spark_session
job=Job(Context)
job.init(args['JOB_NAME',args])
spark.config.set('spark.sql.session.timeZone','UTC')

spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{database_name}.{table_name}")



df=(glueContext.read.forma('parquet')
    .option("partitionOverwriteMode", "dynamic")
    .load("MyS3Path")
)

df.writeTo(f"{catalog_name}.{database_name}.{table_name}")\
  .tableProperty("format-version","2")\
  .createOrReplace()

job.commit()
print("JobEnds")

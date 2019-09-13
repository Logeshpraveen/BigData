import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ReadJsonFile {
  def main(args: Array[String]){
        
val spark = SparkSession.builder().master("local[*]").appName("Read JSON File")
.config("spark.some.config.option", "some-value").getOrCreate()

import spark.implicits._
    
val df = spark.read.json("hdfs://localhost:9000/Project_1")
    
df.printSchema()

df.show()

df.createOrReplaceTempView("purchase")

val sqlDF = spark.sql ("select * from purchase")
sqlDF.show()

#Save to MySQL
      sqlDF.write.format("jdbc").option("url", "jdbc:mysql:dbserver").option("dbtable", "schema.tablename")
.option("user", "username").option("password", "password").save()

#Save to S3 Bucket
      sqlDF.write.format("parquet").mode(SaveMode.Append).save(S3_url)

#Save to HBASE
      sqlDF.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark ").save()   
  }
}
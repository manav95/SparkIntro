val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val rddCustomers = sc.textFile("data/customers.txt")

val schemaString = "customer_id name city state zip_code"

import org.apache.spark.sql._

import org.apache.spark.sql.types._;

val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

val rowRDD = rddCustomers.map(_.split(",")).map(p => Row(p(0).trim,p(1),p(2),p(3),p(4)))

val dfCustomers = sqlContext.createDataFrame(rowRDD, schema)

dfCustomers.registerTempTable("customers")

val custNames = sqlContext.sql("SELECT name FROM customers")

custNames.map(t => "Name: " + t(0)).collect().foreach(println)

val customersByCity = sqlContext.sql("SELECT name,zip_code FROM customers ORDER BY zip_code")

customersByCity.map(t => t(0) + "," + t(1)).collect().foreach(println)

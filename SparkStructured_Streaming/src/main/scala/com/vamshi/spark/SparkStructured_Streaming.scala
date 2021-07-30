package com.vamshi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._

object SparkStructured_Streaming 
{
	def main(args:Array[String]):Unit = {

			val conf = new SparkConf().setAppName("Spark Structured Streaming").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					//val ssc = new StreamingContext(conf,Seconds(2))
					val structSchema = StructType(Array(
							StructField("EmpId",StringType,false),
							StructField("EmpName",StringType,true),
							StructField("Sal",IntegerType,true),
							StructField("Sex",StringType,true),
							StructField("Dno",IntegerType,true)))

					val empDF = spark.readStream.format("csv").schema(structSchema).load("file:///D:/D Data/ResultDir/Kinesis_dir")
					//empDF.show(false)

					//					empDF.writeStream
					//					.format("console")
					//					.option("checkpointLocation","file:///D:/D Data/check1")
					//					.outputMode("append")
					//					.start().awaitTermination();
					
					//Writing Stream data to windows local file system
					empDF.writeStream
					.format("parquet")
					.option("checkpointLocation","file:///D:/D Data/check2")
					.start("file:///D:/D Data/ResultDir/Streaming_Dir").awaitTermination()
					print("Data Written....")

	}
}
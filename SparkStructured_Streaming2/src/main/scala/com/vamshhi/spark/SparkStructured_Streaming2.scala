package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkStructured_Streaming2 
{
	def main(args:Array[String]):Unit = {

			val Conf = new SparkConf().setAppName("Spark Structured Streaming").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					//reading data from telnet socket - port 9999
					val streamDF = spark.readStream.format("socket").option("host","192.168.56.101").option("port","9999").load()
					//writing the output to console
					streamDF.writeStream.format("console").option("checkpointLocation","file:///D:/D Data/ResultDir/checkpoint2").start().awaitTermination()
	}
}
package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkPracticeProjects 
{
case class Schema(txnno:Int,txndate:String,custno:Int,amount:Double,category:String,product:String,city:String,state:String,spendby:String)

case class f1schema(txnno:Int,txndate:String,custno:Int,amount:Double)
case class f2schema(txnno:Int,category:String,product:String,city:String,state:String,spendby:String)
case class person(ID:Int,Name:String,Amount:Int)
def main(args:Array[String]):Unit = 
{
		val Conf = new SparkConf().setAppName("Spark Practice 1").setMaster("local[*]")
				val sc = new SparkContext(Conf)
				sc.setLogLevel("Error")
				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._
				val txns_data = sc.textFile("file:///C:/Data/txns")
				//txns_data.take(10).foreach(println)

				// Imposing the schema to txns_data RDD using Case Class
				val txns_schema_RDD = txns_data.map(x=>x.split(",")).map(x=>Schema(x(0).toInt,x(1),x(2).toInt,x(3).toDouble,x(4),x(5),x(6),x(7),x(8)))
				//txns_schema_RDD.take(10).foreach(println)

				// Spark RDD API Operations:

				// 1. Single Grouping and Single aggregation

				val CategoryAmt = txns_schema_RDD.map(x=>(x.category,x.amount))
				val CategoryAmtTotal = CategoryAmt.reduceByKey(_+_)
				//CategoryAmtTotal.take(10).foreach(println)

				// 2.Single Grouping and Multiple Aggregation

				val CategoryAmtMulAgg = CategoryAmt.groupByKey()

				val CategoryAmtMulAgg_res = CategoryAmtMulAgg.map{ x=>

				val category = x._1
				val cb = x._2
				val sum = cb.sum
				val cnt = cb.size
				val avg = sum/cnt
				val max = cb.max
				val min = cb.min

				val t = (category,sum,avg,max,min,cnt)
				t
		}
		//CategoryAmtMulAgg_res.foreach(println)

		// 3. Multi Grouping and Single Aggregation

		val stateCatAmt = txns_schema_RDD.map(x=>((x.state,x.category),x.amount))
				val stateCatAmt_res = stateCatAmt.reduceByKey(_+_).map(x=>(x._1._1+"\t"+x._1._2+"\t"+x._2))
				//stateCatAmt_res.foreach(println)

				// 4.Multi Grouping and Multiple Aggregation

				val stateCatAmt_res1 = stateCatAmt.groupByKey()
				val stateCatAmt_res2 = stateCatAmt_res1.map {x=>

				val state = x._1._1
				val category = x._1._2
				val cb = x._2
				val sum = cb.sum
				val cnt = cb.size
				val avg = sum/cnt
				val max = cb.max
				val min = cb.min

				val t = (state,category,sum,avg,max,min,cnt)
				t
		}
		//stateCatAmt_res2.foreach(println)

		// filter operation on a Schema Rdd

		val txns_filter = txns_schema_RDD.filter(x=>x.category.equals("Gymnastics") || x.category.equals("Team Sports"))
				//txns_filter.take(10).foreach(println)

				// Joins - inner, left, right and full outer

				val f1 = sc.textFile("file:///C:/Data/f1.csv")
				//f1.take(10).foreach(println)

				val f2 = sc.textFile("file:///C:/Data/f2.csv")
				//f2.take(10).foreach(println)

				val f1schemardd = f1.map(x=>x.split(",")).map(x=>f1schema(x(0).toInt,x(1),x(2).toInt,x(3).toDouble))
				//f1schemardd.take(10).foreach(println)
				val f2schemardd = f2.map(x=>x.split(",")).map(x=>f2schema(x(0).toInt,x(1),x(2),x(3),x(4),x(5)))
				//f2schemardd.take(10).foreach(println)

				val f1kv = f1schemardd.map(x=>(x.txnno,(x.txndate,x.custno,x.amount)))
				val f2kv = f2schemardd.map(x=>(x.txnno,(x.category,x.product,x.city,x.state,x.spendby)))

				// inner join

				val inner_join = f1kv.join(f2kv)
				val inner_join1 = inner_join.map(x=>(x._1,x._2._1._1,x._2._1._2,x._2._1._3,x._2._2._1,x._2._2._2,x._2._2._3,x._2._2._4))
				//inner_join1.take(10).foreach(println)

				// left outer join
				val left_outer = f1kv.leftOuterJoin(f2kv)
				//left_outer.take(10).foreach(println)

				//right outer join
				val right_outer = f1kv.rightOuterJoin(f2kv)
				//right_outer.take(10).foreach(println)

				//full outer join
				val full_outer = f1kv.fullOuterJoin(f2kv)
				//full_outer.take(10).foreach(println)

				// distinct

				val distinct_category = txns_schema_RDD.map(x=>x.category).distinct()
				//distinct_category.foreach(println)

				//union

				val file1 = sc.textFile("file:///C:/Data/file1.txt")
				val file2 = sc.textFile("file:///C:/Data/file2.txt")

				val union_res = file1.union(file2).map(x=>x.split(",")).map(x=>person(x(0).toInt,x(1),x(2).toInt))
				//union_res.foreach(println)

				//countByKey and countByValue

				val categorytxnno = txns_schema_RDD.map(x=>(x.category,x.txnno))

				val cntbyKey = categorytxnno.countByKey()
				//cntbyKey.foreach(println)

				val state = txns_schema_RDD.map(x=>x.state)
				val cntbyValue = state.countByValue()
				//cntbyValue.foreach(println)

				//cartesian

				val city1 = txns_schema_RDD.map(x=>x.city).distinct()
				val category1 = txns_schema_RDD.map(x=>x.category).distinct()
				val cartesian_res = city1.cartesian(category1)
				//cartesian_res.foreach(println)

				//1. RDD to DataFrame conversion using Case class:
				val DF1 = txns_schema_RDD.toDF()
				//DF1.show(10)

				//2. RDD to DataFrame conversion using StructType:
				val SchemaStruct = StructType(Array(
						StructField("txnno",IntegerType,false),
						StructField("txndate",StringType,true),
						StructField("custno",IntegerType,true),
						StructField("amount",DoubleType,true),
						StructField("category",StringType,true),
						StructField("product",StringType,true),
						StructField("city",StringType,true),
						StructField("state",StringType,true),
						StructField("spendby",StringType,true)
						)
						)

				val txns_row_RDD = txns_data.map(x=>x.split(",")).map(x=>Row(x(0).toInt,x(1),x(2).toInt,x(3).toDouble,x(4),x(5),x(6),x(7),x(8)))
				val DF2 = spark.createDataFrame(txns_row_RDD, SchemaStruct)
				//DF2.show(10)
				
				// Spark SQL DataFrame API's:
				
				//1. DF.select()
				
				val DF1_res = DF1.select(col("txnno"),col("custno"),col("product"),col("amount"))
				//DF1_res.show()
				
				//2. DF.printSchema() - Prints the Schema for the specified DF
				
				//DF2.printSchema()
				
				//3. DF.describe(col("<col_name>")) - displays different aggregations for specified column
				
				//DF1.describe("amount").show()
				
				//4. DF.describe() - displays different aggregations for all the columns
				
				//DF2.describe().show()
				
				//5. DF.count()
				
				val res = DF1.count()
				//print(res)
				
				//6. DF.collect() - Returns the Array of rows
				
				//DF2.collect().foreach(println)
				
				//7. DF.columns - print the column names for the Dataframe
				
				val res2 = DF1.columns
				//res2.foreach(println)
				
				//8. DF.selectExpr()
				
				val res3 = DF1.selectExpr("split(txndate,'-')[1] as month").distinct().sort(col("month").desc)
				//res3.show()
				
				//9. DF.schema()
				
				val res4 = DF1.schema
				res4.foreach(println)
				
				//10. DF.withColumn() - used to modify existing column data or create new column
			
}
}
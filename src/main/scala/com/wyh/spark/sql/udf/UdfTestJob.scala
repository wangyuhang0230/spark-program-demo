package com.wyh.spark.sql.udf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.util.Scanner

object UdfTestJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.implicits._

    spark.udf.register("myAverage", MyAverage)


    Seq(("tom", 14), ("bob", 15), ("jack", 12)).toDF("name", "age").createOrReplaceTempView("user")

    spark.sql("select * from user").show()

    spark.sql("select myAverage(age) from user").show()

    spark.sql("drop table user").show()

    spark.sql("select * from user")
      .write
      .partitionBy()

    new Scanner(System.in).next()

  }

  object MyAverage extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(Seq(StructField("in", LongType)))

    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)){
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}

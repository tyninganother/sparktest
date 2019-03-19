package com.sparktest.spark.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {

  /**
    *
    * sockt 处理
    *
    * 使用 nc -lk 6789 启动服务器来进行测试
    * <p>
    * tip:其中local[2]要大于1，不然只能有一个线程来接受数据但是没有线程来处理数据
    * </p>
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

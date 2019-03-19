package com.sparktest.spark.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {
  /**
    *
    * 其中该文件加下的子目录不支持，
    * 文件必须要同格式
    * 文件必须要是原子性的移动到文件夹下
    * 在文件夹下编辑的文件是不会被处理的，也可以是如果文件已经被处理过了就不会再次处理
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    /**
      * 这是一个目录不是一个文件
      */
    val lines = ssc.textFileStream("file:///Users/haining/sparktest/")

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

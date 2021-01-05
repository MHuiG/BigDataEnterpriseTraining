package example07

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object StreamingAndSQLTest {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    //    步骤1：初始化程序入口
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    //    步骤2：通过数据源获取数据（数据输入）
    val myDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    //    步骤3：进行算子的操作，实现业务（数据处理）
    val wordDStream = myDStream.flatMap(_.split(","))

    wordDStream.foreachRDD(rdd => {

      //rdd -> DataFrame -> Table -> SQL
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val wordDataFrame = rdd.toDF("word")

      wordDataFrame.createOrReplaceTempView("words")
      spark.sql("select word,count(*) as total from words group word").show

    })


    ssc.start()
    ssc.awaitTermination()

  }

}

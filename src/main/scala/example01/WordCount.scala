package example01

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}


object WordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //    步骤1：初始化程序入口
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    //    步骤2：通过数据源获取数据（数据输入）nc -lp 9999
    val myDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //    步骤3：进行算子的操作，实现业务（数据处理）
    val result: DStream[(String, Int)] = myDStream
      .flatMap(_.split(","))
      .map((_, 1)).reduceByKey(_ + _)

    //    步骤4：数据的输出
    result.print()

    //    步骤5：启动任务
    ssc.start()
    //    步骤6：等待任务结束
    ssc.awaitTermination()

  }

}

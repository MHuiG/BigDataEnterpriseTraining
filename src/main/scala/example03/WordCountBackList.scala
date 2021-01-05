package example03

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCountBackList {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //    步骤1：初始化程序入口
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val backList: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(List("!", "?")).map(x => (x, true))
    val blBC: Broadcast[Array[(String, Boolean)]] = ssc.sparkContext.broadcast(backList.collect())

    //    步骤2：通过数据源获取数据（数据输入）
    val myDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordDStream: DStream[(String, Int)] = myDStream.flatMap(_.split(",")).map((_, 1))

    val result: DStream[(String, Int)] = wordDStream.transform(rdd => {
      val blRDD = ssc.sparkContext.parallelize(blBC.value)
      val filterBL: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(blRDD)
      val result = filterBL.filter(_._2._2.isEmpty).map(x => (x._1, x._2._1))
      result
    })

    result.print()



    //    步骤5：启动任务
    ssc.start()
    //    步骤6：等待任务结束
    ssc.awaitTermination()
  }

}

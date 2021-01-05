package example02

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCountUpdateStateByKeyCKP {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val checkpointDirectory ="ckp_10"

    def functionToCreateContext(): StreamingContext = {
      //    步骤1：初始化程序入口
      val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
      val ssc = new StreamingContext(conf, Seconds(2))

      ssc.checkpoint(checkpointDirectory)

      //    步骤2：通过数据源获取数据（数据输入）
      val myDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

      val wordDStream: DStream[(String, Int)] = myDStream.flatMap(_.split(",")).map((_,1))

      /**
       * valuses 当前输入的数据
       * state   上一次计算的结果
       */
      val result: DStream[(String, Int)] = wordDStream.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        val currentCount = values.sum
        val lastCount = state.getOrElse(0)
        Some(currentCount + lastCount)
      })

      result.print()
      ssc
    }

    val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)

    // Start the context
    context.start()
    context.awaitTermination()



  }

}

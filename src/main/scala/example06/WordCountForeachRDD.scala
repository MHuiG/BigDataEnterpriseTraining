package example06

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCountForeachRDD {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val myDataStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val lineDStream: DStream[String] = myDataStream.flatMap(_.split(","))
    val wordAndOneDStream: DStream[(String, Int)] = lineDStream.map((_, 1))
    val wordCounts: DStream[(String, Int)] = wordAndOneDStream.reduceByKey(_ + _)



    //将结果保存到Mysql(一) 这句代码是不能运行的。
    wordCounts.foreachRDD { (rdd,time) =>
      //创建了数据库连接
      //executed at the driver
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
      val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
      //statement 要从Driver通过网络发送过来
      //序列化的事，statement不支持序列化。
      //connection object not serializable)
      rdd.foreach { record =>
        //executed at the worker(Executor)
        //遍历每一条数据，然后把数据插入数据库。
        statement.setLong(1, time.milliseconds)
        statement.setString(2, record._1)
        statement.setInt(3, record._2)
        statement.execute()
      }
      statement.close()
      conn.close()
    }
    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)


    //将结果保存到Mysql(二)
    wordCounts.foreachRDD { (rdd, time) =>
      // 在worker，这个代码不报错
      rdd.foreach { record =>
        //为每一条数据都创建了一个连接。
        //连接使用完了以后就关闭。
        //频繁的创建和关闭连接。其实对数据性能影响很大。
        //executor,worker
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        statement.setLong(1, time.milliseconds)
        statement.setString(2, record._1)
        statement.setInt(3, record._2)
        statement.execute()
        statement.close()
        conn.close()
      }
    }

    //将结果保存到Mysql(三)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        //executor,Worker
        //为每个partition的数据创建一个连接。
        //比如这个partition里面有1000条数据，那么这1000条数据
        //就共用一个连接。这样子的话，连接数就减少了1000倍了。
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.foreach { case (word, count) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.execute()
        }
        statement.close()
        conn.close()
      }
    }

    //将结果保存到Mysql(四)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        //使用连接池，我们连接就可以复用
        //性能就更好了。
        val conn = ConnectionPool.getConnection
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.foreach { case (word, count) =>
          //缺点： 还是一条数据一条数据插入
          //每条数据插入都需要跟MySQL进行通信。
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.execute()
        }
        statement.close()
        //使用完了以后，把连接还回去
        ConnectionPool.returnConnection(conn)
      }
    }

    //将结果保存到Mysql(五)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.foreach { case (word, count) =>
          //使用了批处理。性能就更好了。
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.addBatch()
        }
        statement.executeBatch()
        statement.close()
        ConnectionPool.returnConnection(conn)
      }
    }


    //将结果保存到Mysql(六)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        //自动提交的事务关闭
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.foreach { case (word, count) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.addBatch()
        }
        statement.executeBatch()
        statement.close()
        //提交了一个批次以后，我们手动提交事务。
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)
      }
    }


    //将结果保存到Mysql(七)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.zipWithIndex.foreach { case ((word, count), index) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.addBatch()
          //批处理的时候，我们可以决定多少条数据为一个批次
          //我们这儿设置的是500条。
          if (index != 0 && index % 500 == 0) {
            statement.executeBatch()
            conn.commit()
          }
        }
        statement.executeBatch()
        statement.close()
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)
      }
    }

    //等待Streaming程序终止
    ssc.awaitTermination()
  }

}

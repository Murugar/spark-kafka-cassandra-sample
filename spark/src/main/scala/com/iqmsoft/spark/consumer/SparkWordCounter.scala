package com.iqmsoft.spark.consumer

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import com.google.gson.Gson
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}


object SparkWordCounter {

  private val gson = new Gson()

  case class Event(id: Integer, name: String) {
    def getName(): String = name
  }

  /**
    * @param args consists of:
    *             - line of brokers, such as: 'broker1:9092,broker2:9092'
    *             - line of topics, such as: 'topic1,topic2'
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 2)
      System.exit(1)

    val Array(brokers, topics) = args

    SparkLogging.setStreamingLogLevels()

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkConsumer")
    //      .setMaster("local[*]").set("spark.cassandra.connection.host", "172.17.0.2")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("./.temp/")

    CassandraConnector(sparkConf).withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS api")
      session.execute("CREATE KEYSPACE api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
      session.execute("CREATE TABLE api.words (word TEXT PRIMARY KEY, amount BIGINT)")
    }

    val initialRDD = ssc.cassandraTable[(String, Int)]("api", "words")

    //    val lines: DStream[String] = ssc.socketTextStream("localhost", 9999)
    //      .map(parser(_, gson, classOf[Event]))
    //    val words: DStream[String] = lines.flatMap(_.split(" "))
    //    val wordCount: DStream[(String, Int)] = words.map(x => (x, 1))

    val topicsSet: Set[String] = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines: DStream[String] = messages.map(_._2)
      .map(parser(_, gson, classOf[Event]))
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordCount: DStream[(String, Int)] = words.map(x => (x, 1))

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    wordCount
      .mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))
      .saveToCassandra("api", "words")

    ssc.start()
    ssc.awaitTermination()
  }

  private def parser[T <: Event](json: String, gson: Gson, clazz: Class[T]): String = {
    gson.fromJson(json, clazz).getName
  }

}



package com.examples.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object WindowJoin {

  case class WordTimeStamp(word: String, name:String,time: Long,count:Long )

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


  //Timestamp assigner.- check the notes for what this dones.
  class TimestampAssigner extends  AscendingTimestampExtractor[WordTimeStamp]{
    override def extractAscendingTimestamp(input: WordTimeStamp): Long = {
       input.time
    }
  }

  def main(args: Array[String]) {

    val hostname:String="localhost"
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text1= env.socketTextStream(hostname, 9000, '\n')

    val text2 = env.socketTextStream(hostname, 9001, '\n')


    //Using a WatermarkStrategy the below way takes a stream and produce a new stream with timestamped elements and watermarks. If the original stream had timestamps and/or watermarks already, the timestamp assigner overwrites them.

    val aStream =text1
      .flatMap{w=>w.split("\\s")}
      .map{w=>WordTimeStamp(w,"aStream",System.currentTimeMillis(),1)}
      .assignTimestampsAndWatermarks(new TimestampAssigner())

    val bStream =text2
      .flatMap{w=>w.split("\\s")}
      .map{w=>WordTimeStamp(w,"bStream",System.currentTimeMillis(),1)}
      .assignTimestampsAndWatermarks(new TimestampAssigner())


          val output1=aStream.keyBy("word").window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .sum("count")

   val output2=bStream.keyBy("word").window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .sum("count")

    // print the results with a single thread, rather than in parallel
    output1.print().setParallelism(1)
    output2.print().setParallelism(1)

    print("here")
   val joined=output1.join(output2)
       .where(_.word)
       .equalTo(_.word)
       .window(TumblingEventTimeWindows.of(Time.seconds(5)))
       .apply{(w1,w2)=> WordTimeStamp(w1.word,"jstream",System.currentTimeMillis(),w1.count+w2.count)}

    // print the results with a single thread, rather than in parallel
  joined.print().setParallelism(1)


    // execute program
    env.execute("Windowed Join Example")
  }


}
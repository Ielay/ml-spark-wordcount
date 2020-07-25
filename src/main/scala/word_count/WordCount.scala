package word_count

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author lelay
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val textFilePath = args.apply(0)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCountSpark")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(textFilePath)
    val wordToWordCount = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collectAsMap()

    println("Total number of words: " + wordToWordCount.toStream.map(pair => pair._2).sum)
    println("Different words count: " + wordToWordCount.size)
    println("Words:")
    wordToWordCount.foreach(pair => println(pair._1 + " : " + pair._2))
  }
}

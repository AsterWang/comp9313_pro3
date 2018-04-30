package comp9313.ass3

import org.apache.spark.{SparkConf, SparkContext}

object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile =  sc.textFile(inputFile)
    val s = textFile.map(line => line.split(" ")).filter(line => line.length == 4)
    val x = s.map(line => (line(1).toInt, line(3).toDouble)).sortByKey().groupByKey()
    val x2 = x.map(t => (t._1, t._2.sum / t._2.size.toDouble))
    val x3 = x2.sortBy(t => (-t._2,t._1)).map(x => x._1 +"\t"+x._2+"\r")
    x3.saveAsTextFile(outputFolder)
  }
}

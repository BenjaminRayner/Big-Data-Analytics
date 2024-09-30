import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    // modify this code
    val userRatings = lines.flatMap(line => 
    {
      val movieRatings = line.split(",", 2)

      var index = 0
      var columnNum = 1
      var tupleList = new ListBuffer[(Int, Int)]()
      while (index < movieRatings(1).length) {
        val rating = movieRatings(1).charAt(index)
        if (rating == ',') {
          tupleList += ((columnNum, 0))
          index += 1
        }
        else {
          tupleList += ((columnNum, 1))
          index += 2
        }
        columnNum += 1
      }
      tupleList
    }).reduceByKey(_+_).map(x => x._1 + "," + x._2).saveAsTextFile(args(1))
  }
}

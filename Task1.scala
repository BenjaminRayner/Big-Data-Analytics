import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    // modify this code
    val maxRating = lines.map(line => 
    {
        val movieRatings = line.split(",", 2)

        var max = '1'
        var index = 0
        var columnNum = 1
        var topColumns = new ListBuffer[String]()
        while (index < movieRatings(1).length) {
          val rating = movieRatings(1).charAt(index)
          if (rating == ',') {
            index += 1
          }
          else {
            if (rating > max) {
              max = rating
              topColumns.clear()
              topColumns += columnNum.toString
            }
            else if (rating == max) {
              topColumns += columnNum.toString
            }
            index += 2
          }
          columnNum += 1
        }
        movieRatings(0) + "," + topColumns.mkString(",")
    }).saveAsTextFile(args(1))
  }
}

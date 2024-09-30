import org.apache.spark.{SparkContext, SparkConf}

object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    // modify this code
    val totalRatings = lines.map(line => 
    {
        val movieRatings = line.split(",", 2)

        var numRatings = 0
        var index = 0
        while (index < movieRatings(1).length) {
          val rating = movieRatings(1).charAt(index)
          if (rating == ',') {
            index += 1
          }
          else {
            numRatings += 1
            index += 2
          }
        }
        //What if output list with flatmap
        (0, numRatings)
    }).reduceByKey(_+_).values.saveAsTextFile(args(1))
  }
}

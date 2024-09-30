import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    // modify this code
    val moviesRatingsAll = sc.broadcast(lines.collect())
    var startLine = 0
    val ratingSimilarities = lines.flatMap(line => 
    {
        val movieRatingsLocal = line.split(",", 2)

        if (startLine == 0) {
          startLine = moviesRatingsAll.value.indexOf(line)
        }
        startLine += 1

        val totalLines = moviesRatingsAll.value.size;
        var list = new ListBuffer[String]()
        for (lineNum <- startLine until totalLines) {
          val movieRatingsCached = moviesRatingsAll.value(lineNum).split(",", 2)
          
          var localIndex = 0
          var cacheIndex = 0
          var similarities = 0
          while ((localIndex < movieRatingsLocal(1).length) && (cacheIndex < movieRatingsCached(1).length)) {
            if (movieRatingsLocal(1).charAt(localIndex) == ',') localIndex += 1
            else {
              if (movieRatingsLocal(1).charAt(localIndex) == movieRatingsCached(1).charAt(cacheIndex)) similarities += 1
              localIndex += 2
            }
            if (movieRatingsCached(1).charAt(cacheIndex) == ',') cacheIndex += 1
            else cacheIndex += 2
          }
          
          if (movieRatingsLocal(0).compareTo(movieRatingsCached(0)) < 0)
            list += movieRatingsLocal(0) + "," + movieRatingsCached(0) + "," + similarities
          else
            list += movieRatingsCached(0) + "," + movieRatingsLocal(0) + "," + similarities
        }
        list
    }).saveAsTextFile(args(1))
  }
}

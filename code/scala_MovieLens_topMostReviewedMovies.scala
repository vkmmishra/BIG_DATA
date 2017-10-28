package MovieLens

/**
  * Created by varun on 27/8/17.
  */
import org.apache.spark.{SparkContext,SparkConf}
import com.typesafe.config._
import org.apache.hadoop.fs._

case class Movies(
                   MovieId:Int,
                   Title:String,
                   Genres:String)

case class Ratings(
                  UserId:Int,
                  MovieId:Int,
                  Rating:Int,
                  TimeStamp:String
                  )

case class Users(
                  UserID:Int,
                  Gender:String,
                  Age:Int,
                  ZipCode:Int
                )
object topMostReviewedMovies {
  def main(args:Array[String]): Unit = {
    val appProps = ConfigFactory.load().
      getConfig(args(0))
    val conf = new SparkConf().
      setMaster(appProps.getString("executionMode")).
      setAppName("Top Most Reviewd Movies").
      set("spark.ui.port", "12349")
    val sc = new SparkContext(conf)
    val inputPath = args(1)
    val outputPath = args(2)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath)


    if (!fs.exists(inPath))
      println("ooooooooooppppssss" +
        "sssssssssssssssss Input Path does not exist")
    else {
      if(fs.exists(outPath))
        fs.delete(outPath,true)
      val movies = sc.textFile(inputPath + "movies.txt").map(rec => {
        val r = rec.split("::")
        Movies(r(0).toInt, r(1).toString, r(2).toString)
      }).map(rec => (rec.MovieId, rec.Title))

      val ratings = sc.textFile(inputPath + "ratings.txt").map(rec => {
        val r = rec.split("::")
        Ratings(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toString)
      }).
        map(rec => (rec.MovieId, (rec.UserId, rec.Rating)))


      val moviesJoinRatings = movies.join(ratings).map(rec => ((rec._1, rec._2._1), rec._2._2._2))

      val moviesJoinRatingsMap = moviesJoinRatings.aggregateByKey((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

      val topNRatedMovies = sc.parallelize(moviesJoinRatingsMap.filter(rec => rec._2._2 > 40).
        sortBy(rec => -(rec._2._1.toFloat / rec._2._2).toFloat).
        map(rec => (rec._1, (rec._2._1.toFloat / rec._2._2).toFloat)).take(30)).
        map(rec=>(rec._1 +"\t"+ rec._2))

      /*val users = sc.textFile(inputPath + "users.txt").map(rec=> {
      val r = rec.split("::")
      Users(r(0).toInt,r(1).toString,r(2).toInt,r(3).toInt)})*/

      topNRatedMovies.repartition(1).saveAsTextFile(outputPath)
    }

  }
}

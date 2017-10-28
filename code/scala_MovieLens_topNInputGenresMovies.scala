package MovieLens

/**
  * Created by varun on 19/9/17.
  */
import org.apache.spark.{SparkContext,SparkConf}
import com.typesafe.config._
import org.apache.hadoop.fs._

object topNInputGenresMovies {
  def main(args:Array[String]): Unit ={
    val appProps =  ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().
      setAppName("Top N Input Genres Movies").
      setMaster(appProps.getString("executionMode")).
      set("spark.ui.port","12349")

    val sc = new SparkContext(conf)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val inputPath = args(1)
    val outputPath = args(2)

    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath + "topNInputGenresMovies")

    if(!fs.exists(inPath))
      println("Input Path does not exists!")
    else{
      if(fs.exists(outPath))
        fs.delete(outPath)

      val genres = readLine("Enter the type of movie: ").toUpperCase()
      val rate = readLine("Enter the min Ratings: ").toFloat
      val num = readLine("Enter the no. of movies: ").toInt
      val moviesHeader = sc.textFile(inputPath + "movies.txt").first
      val movies = sc.textFile(inputPath + "movies.txt").filter(rec=>rec!=moviesHeader)
      val moviesMap = movies.map(rec=>(rec.split(",")(0),
        (rec.split(",")(1),rec.split(",")(2).toString))).
        filter(rec=>rec._2._2.toUpperCase().contains(genres))

      val ratingsHeader = sc.textFile(inputPath + "ratings.txt").first
      val ratings = sc.textFile(inputPath + "ratings.txt").filter(rec=>rec!=ratingsHeader)
      val ratingsMap = ratings.filter(rec=>rec.split(",")(2).toFloat>=rate).
        map(rec=>(rec.split(",")(1),(rec.split(",")(0),rec.split(",")(2),rec.split(",")(3))))

      val moviesJoinRatings = moviesMap.join(ratingsMap).
        map(rec=>((rec._1,rec._2._1._1,rec._2._1._2),1))

      val topNInputGenresMovi = moviesJoinRatings.
        reduceByKey((rec,ele)=>rec+ele).sortBy(rec=> -rec._2)

      println("Total no. of " + genres +"related movies are: ",topNInputGenresMovi.count())
      topNInputGenresMovi.take(num).foreach(println)


    }






  }

}

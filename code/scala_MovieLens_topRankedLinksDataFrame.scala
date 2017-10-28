package MovieLens





import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import com.typesafe.config._
import org.apache.spark.sql.SQLContext

object topRankedLinksDataFrame {
  def main(args:Array[String]): Unit = {
    val appProps = ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().
      setAppName("Top Ranked Movies Link").
      setMaster(appProps.getString("executionMode")).set("spark.ui.port","12349")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val fs =  FileSystem.get(sc.hadoopConfiguration)
    val inputPath = args(1)
    val outputPath = args(2)

    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath + "topRankedLinksDataFrame")

    if(!fs.exists(inPath))
      println("Input Path Does not exists")
    else
    {
      if(fs.exists(outPath))
        fs.delete(outPath)

      val movies = sc.textFile(inputPath + "movies.txt").coalesce(3).map(rec=> {
        val r = rec.split(",")
        movies20(r(0).toInt,r(1),r(2))
      }).toDF

      movies.registerTempTable("movies")

      val ratings = sc.textFile(inputPath + "ratings.txt").coalesce(1).map(rec=> {
        val r = rec.split(",")
        ratings20(r(0).toInt,r(1).toInt,r(2).toDouble,r(1))
      }).toDF

      ratings.registerTempTable("ratings")

      val links = sc.textFile(inputPath + "links.txt").coalesce(1).
        filter(rec=>rec.substring(rec.lastIndexOf(",")+1, rec.length()).nonEmpty).map(rec=> {
        val r = rec.split(",")
        links20(r(0).toInt,("http://www.imdb.com/title/tt" + r(1)),
          ("https://www.themoviedb.org/movie/" + r(2)))
      }).toDF

      links.registerTempTable("links")

    sqlContext.setConf("spark.sql.shuffle.partitions","20")
    val output = sqlContext.sql("select a.movieId,a.title,a.numusers,a.rate,l.imdbId,l.tmdbId" +
      " from links l inner join (select m.movieId,m.title, " +
      " count(r.userId) numusers ,sum(r.rating)/count(r.userId) rate " +
      " from movies m inner join ratings r on m.movieId=r.movieId  " +
      " group by m.movieId,m.title) a on l.movieId=a.movieId order by a.numusers desc ,a.rate desc")


      output.write.saveAsTable("topRanked")
      //links.take(500).foreach(println)


    }

  }



}

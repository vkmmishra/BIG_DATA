package MovieLens

/**
  * Created by varun on 27/8/17.
  */
import org.apache.spark.{SparkContext,SparkConf}
import com.typesafe.config._
import org.apache.hadoop.fs._

object topMostViewedMovies {
  def main(args:Array[String]): Unit ={

    val appProps = ConfigFactory.load().
      getConfig(args(0))

    val conf = new SparkConf().
      setAppName("Top Viewd Movies").
      setMaster(appProps.getString("executionMode"))
      //set("spark.ui.port","12349")
      val sc = new SparkContext(conf)

    val inputPath = args(1)
    val outputPath = args(2)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath)
    if(!fs.exists(inPath))
      println("The Given input Path does not exists")
    else {
      if(fs.exists(outPath))
        fs.delete(outPath,true)
      val moviesDatMap = sc.textFile(inputPath + "movies.txt").
        map(rec => (rec.split("::")(0), (rec.split("::")(1), rec.split("::")(2))))
      val ratingsDatMap = sc.textFile(inputPath + "ratings.txt").
        map(rec => (rec.split("::")(1), rec.split("::")(0)))
      val moviesJoinRatingsMap = moviesDatMap.join(ratingsDatMap).map(rec => (rec._2._1._1, 1))
      val topTenMovies = moviesJoinRatingsMap.reduceByKey((rec, ele) => rec + ele).
        sortBy(x => -x._2).take(10)
      val k = sc.parallelize(topTenMovies).map(rec => rec._1 + "\t" + rec._2)
      k.saveAsTextFile(outputPath)

    }

  }

}

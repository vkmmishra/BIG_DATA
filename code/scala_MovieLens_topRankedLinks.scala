package MovieLens


case class movies20(
                     movieId:Int,
                     title:String,
                     genres:String
                   )

case class ratings20(
                      userId:Int,
                      movieId:Int,
                      rating:Double,
                      timestamp:String
                   )

case class links20(
                    movieId:Int,
                    imdbId:String,
                    tmdbId:String
                    )


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import com.typesafe.config._
import org.apache.spark.sql.SQLContext

object topRankedLinks {
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
    val outPath = new Path(outputPath + "topRankedLinks")

    if(!fs.exists(inPath))
      println("Input Path Does not exists")
    else
      {
        if(fs.exists(outPath))
          fs.delete(outPath)

        val movies = sc.textFile(inputPath + "movies.txt").map(rec=> {
        val r = rec.split(",")
        movies20(r(0).toInt,r(1),r(2))
        }).toDF

        val ratings = sc.textFile(inputPath + "ratings.txt").map(rec=> {
          val r = rec.split(",")
          ratings20(r(0).toInt,r(1).toInt,r(2).toDouble,r(3))
        }).toDF

        val links = sc.textFile(inputPath + "links.txt").
          filter(rec=>rec.substring(rec.lastIndexOf(",")+1, rec.length()).nonEmpty).map(rec=> {
          val r = rec.split(",")
          links20(r(0).toInt,("http://www.imdb.com/title/tt" + r(1)),
            ("https://www.themoviedb.org/movie/" + r(2)))
        }).map(rec=>(rec.movieId,(rec.imdbId,rec.tmdbId)))



        /*val moviesJoinratings = movies.join(ratings,movies("movieId")===ratings("movieId")).
          join(links,movies("movieId")===links("movieId"))*/

          val moviesJoinRatings = movies.join(ratings,"movieId").
            map(rec=>((rec(0),rec(1)),rec(4).toString.toDouble)).
            aggregateByKey((0.0,0))((acc,value)=>(acc._1 + value,acc._2+1),
              (acc1,acc2)=>(acc1._1+acc2._1,acc1._2+acc2._2)).
            map(rec=>(rec._1,rec._2._2,rec._2._1/rec._2._2)).
            map(rec=>(rec._1._1.toString.toInt,(rec._1._2,rec._2,rec._3))).join(links).
            sortBy(rec=> (-rec._2._1._2,-rec._2._1._3)).
            map(rec=>rec._1+"\t"+rec._2._1._1+"\t"+rec._2._1._2+
              "\t"+rec._2._1._3+"\t"+rec._2._2._1+"\t"+rec._2._2._2)


        moviesJoinRatings.repartition(2).saveAsTextFile(outputPath + "topRankedLinks")
        //links.take(500).foreach(println)


      }

  }



}

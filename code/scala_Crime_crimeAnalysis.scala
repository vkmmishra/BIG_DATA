package Crime


case class Crime(Id:Int, caseNumber:String, Date:String,
                 Block:String, IUCR:String, primaryType:String,
                  Description:String, locationDescription:String,
                 Arrest:String, Domestic:String, Beat:String, District:String,
                 Ward:String, communityArea:String, fbiCode:String,
                 xCoordinate:String, yCoordinate:String, Year:String,
                 updatedOn:String, Latitude:String,
                 Longitude:String, Location:String
                )

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import com.typesafe.config._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Substring


object crimeAnalysis {
def main(args:Array[String]): Unit ={
  val appProps =  ConfigFactory.load().getConfig(args(0))
  val conf = new SparkConf().
    setAppName("Crime Data Analysis").
    setMaster(appProps.getString("executionMode")).
    set("spark.ui.port","12349")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val fs = FileSystem.get(sc.hadoopConfiguration)
  val inputPath = args(1)
  val outputPath = args(2)

  val inPath = new Path(inputPath)
  val outPath = new Path(outputPath + "crimeAnalysis")

  if(!fs.exists(inPath))
    println("The input path does not exists")
  else {
    if (fs.exists(outPath))
      fs.delete(outPath)

    val firs =  sc.textFile(inputPath + "Crimes.csv").first()

  val crime = sc.textFile(inputPath + "Crimes.csv").coalesce(10).
    filter(rec=>rec!=firs ).filter(p => !p.contains(",,,")).
    map(rec=> {
    val r = rec.split(",")
    Crime(r(0).toInt,r(1),r(2),r(3),r(4),r(5), r(6),r(7),r(8), r(9),r(10),r(11),
      r(12),r(13), r(14), r(15), r(16), r(17), r(18), r(19),r(20), r(21))
  }).toDF

    crime.registerTempTable("crime")
    val data = sqlContext.sql("select * from crime where Arrest = 'false' " +
      " and  Description like '%MURDER'")

    data.take(10).foreach(println)
    println(data.count())



  }



  }
}

package SparkDemo
/**
  * Created by varun on 20/8/17.
  */
import org.apache.spark.{SparkConf,SparkContext}
import com.typesafe.config._
import org.apache.hadoop.fs._

object WordCount {
  def main(args:Array[String]): Unit ={

    val appProps = ConfigFactory.load().
      getConfig(args(0))

    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appProps.getString("executionMode")).
      set("spark.ui.port","12349")

    val sc = new SparkContext(conf)
    val inputPath = args(1)
    //println(inputPath)
    val outputPath = args(2)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inPath = new Path(inputPath)
    //println(inPath)
    val outPath= new Path(outputPath + "wordCount")

    if(!fs.exists(inPath)){
      println(inPath,"Input Path does not Exist")}
    else {
      if(fs.exists(outPath))
        fs.delete(outPath,true)
      val wordsFile = sc.textFile(inputPath + "wordcount").coalesce(10)
      val wordsFileFlatMap = wordsFile.flatMap(words => words.split(" "))
      val wordsFileMap = wordsFileFlatMap.map(word => (word, 1))
      val wordCount = wordsFileMap.reduceByKey((rec, cnt) => rec + cnt,2).
        map(rec => rec._1 + "\t" + rec._2)
      //Saving File in some Direction
      wordCount.saveAsTextFile(outputPath + "wordCount")
    }

  }

}

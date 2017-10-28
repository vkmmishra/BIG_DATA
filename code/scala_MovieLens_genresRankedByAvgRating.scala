package MovieLens

/**
  * Created by varun on 21/9/17.
  */

case class joinedData(userId:Int,
                      movieId:Int,
                      movieName:String,
                      movieGenres:String,
                      movieRating:Float,
                      userAge:Int,
                      userOccupation:Int)

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.hadoop.fs._
import com.typesafe.config._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object genresRankedByAvgRating {
  def main(args:Array[String]): Unit ={
    val appProps = ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().
      setAppName("Geners Ranked By Avg rating").
      setMaster(appProps.getString("executionMode")).
      set("spark.ui.port","12349")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPath = args(1)
    val outputPath = args(2)

    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath + "genresRankedByAvgRating")

    if(!fs.exists(inPath))
      println("The input path does not exists")
    else{
      if(fs.exists(outPath))
        fs.delete(outPath)

      val moviesMap = sc.textFile(inputPath + "movies.txt").
        map(rec => (rec.split("::")(0), (rec.split("::")(1), rec.split("::")(2))))
      val ratingsMap = sc.textFile(inputPath + "ratings.txt").
        map(rec => (rec.split("::")(1), (rec.split("::")(0),rec.split("::")(2))))

      val moviesJoinRatings = moviesMap.join(ratingsMap).
        map(rec=>(rec._2._2._1,(rec._1,rec._2._1,rec._2._2._2)))

      val usersMap = sc.textFile(inputPath + "users.txt").
        map(rec => (rec.split("::")(0), (rec.split("::")(2), rec.split("::")(3))))

      val userAgeGroup = udf((age: Int) => {
        if (age>=1 && age<=17) "Under-Age"
        else if (age>=18 && age<=24) "18-24"
        else if (age>=25 && age<=34) "25-34"
        else if (age>=35 && age<=44) "35-44"
        else if (age>=45 && age<=49) "45-49"
        else if (age>=50 && age<=55) "50-55"
        else "56+"
        })



      val ocup = udf((userOccupation: Int) => {
        if (userOccupation==0) "other"
        else if (userOccupation==1) "academic/educator"
        else if (userOccupation==2) "artist"
        else if (userOccupation==3) "clerical/admin"
        else if (userOccupation==4) "college/grad-student"
        else if (userOccupation==5) "customer-service"
        else if (userOccupation==6)  "doctor/health-care"
        else if (userOccupation==7) "executive/managerial"
        else if (userOccupation==8) "farmer"
        else if (userOccupation==9) "homemaker"
        else if (userOccupation==10) "K-12-student"
        else if (userOccupation==11) "lawyer"
        else if (userOccupation==12) "programmer"
        else if (userOccupation==13) "retired"
        else if (userOccupation==14) "sales/marketing"
        else if (userOccupation==15) "scientist"
        else if (userOccupation==16) "self-employed"
        else if (userOccupation==17) "technician/engineer"
        else if (userOccupation==18) "tradesman/craftsman"
        else if (userOccupation==19) "unemployed"
        else  "writer"
      })


      val moviesJoinRatingsJoinUsersMap = moviesJoinRatings.join(usersMap).
          map(rec=>rec._1 +"~~"+ rec._2._1._1 +"~~"+ rec._2._1._2._1.toString +"~~" + rec._2._1._2._2 +"~~"
            + rec._2._1._3 +"~~"+ rec._2._2._1 +"~~"+rec._2._2._2.toInt).map(rec=> {
        val r = rec.split("~~")
        joinedData(r(0).toInt,r(1).toInt,r(2),r(3),r(4).toFloat,r(5).toInt,r(6).toInt)
      })./*filter(rec=>rec.userOccupation==7 && (rec.userAge>=1 && rec.userAge<=17)).*/
        toDF.withColumn("ageGroup",userAgeGroup($"userAge")).
        withColumn("occupation",ocup($"userOccupation"))


      moviesJoinRatingsJoinUsersMap.registerTempTable("output")

      val moviesJoinRatingsJoinusersFlatmap = sqlContext.sql("select occupation,ageGroup," +
        " movieName,movieGenres,movieRating from output ").
        map(rec=>((rec(0),rec(1),rec(2),rec(4)),rec(3))).
       flatMapValues(rec=>rec.toString.split('|')).
        map(rec=>((rec._1._1,rec._1._2,rec._2),rec._1._4.toString.toFloat)).
        filter(rec=>rec._1._3=="Action" || rec._1._3=="Suspense" || rec._1._3=="Thriller" || rec._1._3=="Romance" || rec._1._3=="Horror")

      val moviesJoinRatingsAgg = moviesJoinRatingsJoinusersFlatmap.
        aggregateByKey((0.0,0))((acc,value)=>(acc._1+value ,acc._2 + 1),
          (acc1,acc2)=>(acc1._1+acc2._1,acc1._2+acc2._2)).
        map(rec=>((rec._1._1,rec._1._2),rec._1._3,rec._2._1/rec._2._2))

      val moviesJoinRatingsGrp = moviesJoinRatingsAgg.groupBy(rec=>rec._1).
        sortBy(rec=>rec._2.map(rec=> -rec._3)).
        map(rec=>((rec._1._1).toString,rec._1._2,rec._2.map(rec=>rec._2).
          mkString(","))).sortBy(rec=>rec._1)


      moviesJoinRatingsGrp.saveAsTextFile(outputPath + "genresRankedByAvgRating")




    }


  }

}

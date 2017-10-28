package SparkDemo

/**
  * Created by varun on 18/9/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
object employee {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().
      setAppName("Employees").
      setMaster("local").
      set("spark.ui.port","12349")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    //import sqlContext.implicits._
    val employee = sqlContext.sql("SELECT * FROM RETAIL_DB.EMPLOYEE")
    val department = sqlContext.sql("SELECT * FROM RETAIL_DB.DEPARTMENT")
    //val employeeJoinDepartment = employee.join(department, col("employee.deptno") === col("department.deptno"), "inner")

    //val empResult = employeeJoinDepartment.filter(not(employeeJoinDepartment("location").like("Chicago")))

    //empResult.write.format("csv").save("/tmp/df.csv")




  }

}

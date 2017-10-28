package jdbc

/**
  * Created by varun on 19/8/17.
  */
import java.sql.DriverManager
import com.typesafe.config._


case class EmployeesCommission(first_name: String,
                               last_name: String,
                               salary: Double,
                               commission_pct: Double) {
  override def toString(): String = {
    s"first_name: " + first_name + ";" + "last_name: " + last_name +
      ";" + "commission amount:" + getCommissionAmount()
  }

  def getCommissionAmount(): Any = {
    if(commission_pct == 0.0) {
      "Not eligible"
    } else salary * commission_pct
  }
}


object CommissionAmount {
  def main(args : Array[String]): Unit ={
    val conf: Config = ConfigFactory.load()
    val run_env = args(0)
    val props = conf.getConfig(run_env)

    val driver = "com.mysql.jdbc.Driver"
    val host = props.getString("host")
    val port = props.getString("port")
    val db = props.getString("db")
    val url = "jdbc:mysql://" + host + ":" + port + "/" + db
    //println(url)

    Class.forName(driver)
    val connection = DriverManager.getConnection(url)

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT first_name, last_name, " +
      "salary, commission_pct FROM employees")


        while (resultSet.next()) {
          val e = EmployeesCommission(resultSet.getString("first_name"),
            resultSet.getString("last_name"),
            resultSet.getDouble("salary"),
            if(resultSet.getDouble("commission_pct").isNaN) -1.0 else resultSet.getDouble("commission_pct"))
          println(e)
        }

  }

}

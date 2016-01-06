/**
 * Created by hikozuma on 2016/01/06.
 */
import java.sql.{Connection, DriverManager}

object ConnectMySQL {
  def main(args: Array[String]) {
    val url = "jdbc:mysql://localhost:3306/LoLResearch"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "root"
    var connection:Connection = null

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    val statement = connection.createStatement
    val rs = statement.executeQuery("select * from Item order by ItemId")

    while(rs.next){
      val itemId = rs.getString("ItemId")
      val itemName = rs.getString("ItemName")

      println("ItemId = %s, ItemName = %s".format(itemId, itemName))
    }

    connection.close
  }
}

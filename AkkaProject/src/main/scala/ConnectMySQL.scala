/**
 * Created by hikozuma on 2016/01/06.
 */

import java.sql.{Statement, Connection, DriverManager}

case class ItemBuild(itemId: Int,
                     itemName: String,
                     itemDescription: String,
                     itemGoldTotal: Int,
                     timeStamp: Long)
/*
class ItemBuild{
  val itemId: Int = _
  val itemname: String = _
  val itemDescription: String = _
  val itemGoldTotal: Int = _
}
*/

object ConnectMySQL {
  def main(args: Array[String]) {
    val url = "jdbc:mysql://localhost:3306"
    val database = "LoLResearch"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "root"
    var cn:Connection = null

    try{
      Class.forName(driver)
      cn = DriverManager.getConnection(url + "/" + database, username, password)
      val st = cn.createStatement

      getMatchInfo(cn, st) match {
        case Left(s) =>
              // checked output
              println("Error message: " + s);
              System.exit(0)

        case Right((matchId, participantId, summonerNameKey, summonerName)) =>
          // checked output
          println("MatchId = %d, ParticipantId = %d, NameKey = %s, Name = %s".format(matchId,
                                                                                      participantId,
                                                                                      summonerNameKey,
                                                                                      summonerName))
      }



      cn.close

    }catch{
      case e: Exception => e.printStackTrace
    }
  }

  def getMatchInfo(cn: Connection, st: Statement): Either[String, (Long, Int, String, String)] = {
    try {
      val rs = st.executeQuery("select MatchId, " +
                                        "ParticipantId, " +
                                        "SummonerNameKey, " +
                                        "SummonerName " +
                                "from MatchPlayerInfo " +
                                "ORDER BY RAND() limit 0, 1")

      if (rs.next) {
        Right(rs.getLong("MatchId"),
              rs.getInt("ParticipantId"),
              rs.getString("SummonerNameKey"),
              rs.getString("SummonerName"))

      } else {
        Left("MatchPlayer Info table have no records");
      }
    }catch{
      case e: Exception => Left("SQL statement is not correct or MatchPlayer Info table isn't existed");
    }
  }

/*
case class ItemBuild(itemId: Int,
                     itemName: String,
                     itemDescription: String,
                     itemGoldTotal: Int,
                     timeStamp: Long)
 */


  def getItemBuild(cn: Connection, st: Statement, matchId: Long, buyerId: Int): Either[String, List[ItemBuild]] = {
    try {
      val rs = st.executeQuery("select MatchId, " +
        "ParticipantId, " +
        "SummonerNameKey, " +
        "SummonerName " +
        "from MatchPlayerInfo " +
        "ORDER BY RAND() limit 0, 1")

/*
      if (rs.next) {
        Right(ItemBuild(rs.getInt("ItemId"),
                        rs.getString("ItemName"),
                        rs.getString("ItemDescription"),
                        rs.getInt("ItemGoldTotal"),
                        rs.getLong("TimeStamp")))

      } else {
        Left("MatchPlayer Info table have no records");
      }
*/
      rs.getRow match {
        case 0 => Left("MatchPlayer Info table have no records");
        case _ =>
          var itemBuildList = List.empty[ItemBuild]

          while(rs.next){
            itemBuildList :+ ItemBuild(rs.getInt("ItemId"),
                                        rs.getString("ItemName"),
                                        rs.getString("ItemDescription"),
                                        rs.getInt("ItemGoldTotal"),
                                        rs.getLong("TimeStamp"))
          }

          Right(itemBuildList)
      }

    }catch{
      case e: Exception => Left("SQL statement is not correct or MatchPlayer Info table isn't existed");
    }
  }

}

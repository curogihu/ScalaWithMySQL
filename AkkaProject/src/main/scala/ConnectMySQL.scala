/**
 * Created by hikozuma on 2016/01/06.
 */

import java.sql.{Statement, Connection, DriverManager}

case class Summoner(matchId: Long,
                    participantId: Int,
                    summonerNameKey: String,
                    summonerName: String)

case class ItemBuild(championName: String,
                     itemName: String,
                     itemDescription: String,
                     itemGoldTotal: Int,
                     purchaseSeconds: Int)

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
      var targetSummoner: Summoner = null

      getMatchInfo(cn, st) match {
        case Left(s) =>
              println("Error message: " + s)
              System.exit(0)

        case Right(summoner: Summoner) =>
              targetSummoner = summoner
      }

      getItemBuild(cn,st, targetSummoner.matchId, targetSummoner.participantId) match {
        case Left(s) =>
          println("Error message: " + s)
          System.exit(0)

        case Right(itemBuildList) =>
          displayItemBuild(itemBuildList)
      }

      cn.close()

    }catch{
      case e: Exception => e.printStackTrace()
    }
  }

  private def displayItemBuild(itemBuildList: List[ItemBuild]): Unit = {
    for(itemBuild <- itemBuildList){
      print("Purchase Seconds = %4d, ".format(itemBuild.purchaseSeconds))
      print("Champion Name = %s, ".format(itemBuild.championName))
      print("Item Name = %s, ".format(itemBuild.itemName))
      print("Item Description = %s, ".format(itemBuild.itemDescription))
      println("ItemGoldTotal = %d".format(itemBuild.itemGoldTotal))
    }
  }

  private def getMatchInfo(cn: Connection,
                           st: Statement): Either[String, Summoner] = {
    try {
      val rs = st.executeQuery("select MatchId, " +
                                        "ParticipantId, " +
                                        "SummonerNameKey, " +
                                        "SummonerName " +
                                "from MatchPlayerInfo " +
                                "ORDER BY RAND() limit 0, 1")

      if (rs.next) {
        Right(Summoner(rs.getLong("MatchId"),
                        rs.getInt("ParticipantId"),
                        rs.getString("SummonerNameKey"),
                        rs.getString("SummonerName")))

      } else {
        Left("MatchPlayer Info table have no records")
      }
    }catch{
      case e: Exception => Left("SQL statement is not correct or MatchPlayerInfo table isn't existed");
    }
  }

  private def getItemBuild(cn: Connection,
                           st: Statement,
                           matchId: Long,
                           buyerId: Int): Either[String, List[ItemBuild]] = {

    try {
      val rs = st.executeQuery("SELECT c.ChampionName, " +
                                        "i.ItemName, " +
                                        "i.ItemDescription, " +
                                        "i.ItemGoldTotal, " +
                                        "truncate(ibl.TimeStamp / 1000, 0) PurchaseSeconds " +
                              "FROM ItemBuildLog ibl " +
                              "inner join Item i " +
                                "on ibl.ItemId = i.ItemId " +
                              "inner join Champion c " +
                                "on ibl.ChampionId = c.ChampionId " +
                                  "WHERE  ibl.MatchId = " + matchId + " " +
                              "and ibl.BuyerId = " + buyerId + " " +
                            "order by ibl.TimeStamp desc")

      rs.last

      if(rs.getRow == 0) {
        Left("MatchPlayer Info table have no records")

      }else{
        var itemBuildList = List.empty[ItemBuild]
          rs.first

          while(rs.next){
            itemBuildList ::= ItemBuild(rs.getString("ChampionName"),
                                        rs.getString("ItemName"),
                                        rs.getString("ItemDescription"),
                                        rs.getInt("ItemGoldTotal"),
                                        rs.getInt("PurchaseSeconds"))
          }
          Right(itemBuildList)
      }

    }catch{
      case e: Exception => Left("SQL statement is not correct or " +
                                "at least one of three tables, tableMatchPlayerInfo, Item and Champion table isn't existed");
    }
  }
}
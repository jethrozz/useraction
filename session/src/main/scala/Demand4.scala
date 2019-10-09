import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object Demand4 {

  def top10ActiveSession(sparkSession: SparkSession, taskUUID: String, top10CategoryArray: Array[(Top10SortKey, String)], sessid2FilterActionRDD: RDD[(String, UserVisitAction)]): Unit = {
    //首先获取到top10热门品类的id
    val cidArray = top10CategoryArray.map {
      case (sortKey: Top10SortKey, fullInfo: String) => {
        val cid = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong;
        cid
      }
    }
    //再对session 进行过滤，获取到点击了这10个热门品类的session
    val clickTop10CidSessionRDD:RDD[(String, UserVisitAction)] = sessid2FilterActionRDD.filter {
      case (sid, userAction) => {
        cidArray.contains(userAction.click_category_id)
      }
    }
    //将一个session中的所有点击动作聚合，再进行计数
    val clickTop10CidSessionGroupRDD: RDD[(String, Iterable[UserVisitAction])]= clickTop10CidSessionRDD.groupByKey()


    val cid2SessionCountRDD = clickTop10CidSessionGroupRDD.flatMap{
      case (sid, iterUserAction) => {
        val cidMap = new mutable.HashMap[Long,Long]()

        for(action <- iterUserAction){
            if(cidMap.get(action.click_category_id).isDefined){
              cidMap.update(action.click_category_id,cidMap(action.click_category_id)+1)
            }else{
              cidMap += (action.click_category_id->0L)
            }
        }

        for( (cid, clickCount) <- cidMap)
            yield (cid, sid + "=" + clickCount.toString)

      }
    }

    //再对cid进行聚合,就获取到了每个品类的session的点击次数。再进行排序取前10即可
    val cid2SessionCountGroupRDD = cid2SessionCountRDD.groupByKey();

    //获取到每个品类的前10的活跃session
    val top10SessionRDD = cid2SessionCountGroupRDD.flatMap{
      case (cid, iterCountInfo)=>{
        //排序
        val top10Array = iterCountInfo.toList.sortWith( (item1,item2) => {
          item1.split("=")(1) > item2.split("=")(1)
        }).take(10)
        //封装数据
        top10Array.map(item=>{
          val sid = item.split("=")(0)
          val count = item.split("=")(1).toLong
          val top10Session = Top10Session(taskUUID,cid,sid,count)
          top10Session
        })
      }
    }

    //保存至数据库
    //    import  sparkSession.implicits._
    //    top10SessionRDD.toDF().write.format("jdbc")
    //              .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
    //              .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
    //              .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
    //              .option("dbtable","top10_session_0308")
    //              .mode(SaveMode.Append)
    //              .save()
    top10SessionRDD.collect().foreach(println)
  }


}

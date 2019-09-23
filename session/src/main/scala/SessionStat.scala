import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object SessionStat {


  def main(args: Array[String]): Unit = {
    //获取到筛选条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskJson = JSONObject.fromObject(jsonStr)
    //创建本次任务的主键
    val taskUUID = UUID.randomUUID().toString()

    //创建所需要的spark上下文环境，以及sparksql上下文环境
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

    val actionRDD:RDD[UserVisitAction] = getOriActionRDD(sparkSession,taskJson)

    //将所有session打散为 sessionId-session 这种k-v格式的数据
    val sessid2Action:RDD[(String, UserVisitAction)] = actionRDD.map(action => (action.session_id, action))
    //再按照sessionId进行聚合操作
    val sessid2ActionGroupRDD:RDD[(String,Iterable[UserVisitAction])] = sessid2Action.groupByKey()

    //对当前数据进行暂存
    sessid2ActionGroupRDD.cache()
    //获取到当前用户的访问信息
    //数据格式 
    val userId2AggrInfo = getFullSessionInfo(sparkSession, sessid2ActionGroupRDD)
    userId2AggrInfo.foreach(println)
  }



  def getOriActionRDD(sparkSession: SparkSession, taskJson: JSONObject) = {
    val startDate = ParamUtils.getParam(taskJson,"startDate");
    val endDate = ParamUtils.getParam(taskJson,"endDate");
    val sql = "select * from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"'";
    //隐式转换
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
  def getFullSessionInfo(sparkSession: SparkSession, sessid2ActionGroupRDD: RDD[(String, Iterable[UserVisitAction])]) ={

    //返回值是一个以userId 为key ，aggrInfo为值的数据
    sessid2ActionGroupRDD.map { case (sessionId, iterableAction) => {
      var userId = -1L
      var startTime: Date = null
      var endTime: Date = null
      var stepLength = 0
      var searchKey = new StringBuffer("")
      var clickCategaryId = new StringBuffer("")

      for (ua <- iterableAction) {
        //设置userId
        if (userId == -1L) {
          userId = ua.user_id
        }
        val actionTime = DateUtils.parseTime(ua.action_time)
        //设置session的开始时间
        if (startTime == null || startTime.after(actionTime)) {
          startTime = actionTime
        }
        //设置session的结束时间
        if (endTime == null || endTime.before(actionTime)) {
          endTime = actionTime
        }
        //设置session的搜索关键字
        if (StringUtils.isNotEmpty(ua.search_keyword) && !searchKey.toString.contains(ua.search_keyword)) {
          searchKey.append(ua.search_keyword + ",")
        }
        //设置session的点击目录关键字
        if (ua.click_category_id != -1 && !clickCategaryId.toString.contains(ua.click_category_id)) {
          clickCategaryId.append(ua.click_category_id + ",")
        }
        stepLength += 1
      }
      //去掉尾部的逗号
      val searchKeyStr = StringUtils.trimComma(searchKey.toString)
      //去掉尾部的逗号
      val clickCateGaryIdStr = StringUtils.trimComma(clickCategaryId.toString)
      //计算访问时长
      val visitLength = (endTime.getTime - startTime.getTime) / 1000
      val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyStr + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCateGaryIdStr + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime);
      (userId, aggrInfo)
    }
    }

//    userId2AggrInfoRDD.map(item => {
//      //获取user信息表的数据
//    })
  }

}

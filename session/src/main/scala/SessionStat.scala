import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object SessionStat {


  def main(args: Array[String]): Unit = {
    //获取到筛选条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskJson = JSONObject.fromObject(jsonStr)
    //创建本次任务的主键
    val taskUUID = UUID.randomUUID().toString()

    //创建所需要的spark上下文环境，以及sparksql上下文环境
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[1]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

    val actionRDD: RDD[UserVisitAction] = getOriActionRDD(sparkSession, taskJson)

    //将所有session打散为 sessionId-session 这种k-v格式的数据
    val sessid2Action: RDD[(String, UserVisitAction)] = actionRDD.map(action => (action.session_id, action))
    //再按照sessionId进行聚合操作
    val sessid2ActionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessid2Action.groupByKey()

    //对当前数据进行暂存
    sessid2ActionGroupRDD.cache()
    //获取到当前用户的访问信息
    //数据格式
    val sessionId2FullInfo:RDD[(String,String)] = getFullSessionInfo(sparkSession, sessid2ActionGroupRDD)

    //创建自定义累加器
    var sessionAccumulator = new SessionAccumulator
    //对自定义累加器进行注册
    sparkSession.sparkContext.register(sessionAccumulator)
    //对数据进行过滤并进行累加
    val sessionId2FilterRDD = getSessionFilterRDD(taskJson,sessionId2FullInfo ,sessionAccumulator)
    sessionId2FilterRDD.count()
    //获取到最终我们需要的值并写入mysql
    //getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)

    //需求二：session随机抽取
    /**
      * sessionId2FilterRDD  sessionId-userFullInfo
      */
    //Demand2.sessionRandomExtract(sparkSession, taskUUID, 400, sessionId2FilterRDD)

    //需求三：统计热门品类Top10
    /**
      * 需要先获取到 RDD[(sessionId, Action)] 格式的数据
      *
     */
    val sessid2FilterActionRDD: RDD[(String, UserVisitAction)] = sessid2Action.join(sessionId2FilterRDD).map {
      case (sessionId, (action, fullInfo)) => {
        (sessionId, action)
      }
    }
   val top10CategoryArray = Demand3.staticTop10Goods(sparkSession, taskUUID,sessid2FilterActionRDD)

    /**
      * //需求四
      * 统计 top10热门品类的top10活跃session
      */
    //Demand4.top10ActiveSession(sparkSession, taskUUID, top10CategoryArray, sessid2FilterActionRDD)

    Demand5.pageFlowConvertRate(taskJson,sparkSession,taskUUID,sessid2ActionGroupRDD)
    //Demand5
  }


  def getOriActionRDD(sparkSession: SparkSession, taskJson: JSONObject) = {
    val startDate = ParamUtils.getParam(taskJson, "startDate");
    val endDate = ParamUtils.getParam(taskJson, "endDate");
    val sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'";
    //隐式转换
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  def getFullSessionInfo(sparkSession: SparkSession, sessid2ActionGroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {

    //返回值是一个以userId 为key ，aggrInfo为值的数据
    val userId2AggrInfo = sessid2ActionGroupRDD.map { case (sessionId, iterableAction) => {
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
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
      (userId, aggrInfo)
    }

    }



    val sql = "select * from user_info"

    //隐式转换
    import sparkSession.implicits._
    val userInfoRDDMap = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    val sessionId2FullInfo = userId2AggrInfo.join(userInfoRDDMap).map{
      case (userId, (aggrInfo,userInfo)) => {
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city
        //将剩余数据全部填充完毕
        val fullInfo = aggrInfo +"|"+Constants.FIELD_AGE + "=" +age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" +professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_SESSION_ID);
        (sessionId,fullInfo)
      }
    }
    sessionId2FullInfo
  }


  def getSessionFilterRDD(taskJson: JSONObject, sessionId2FullInfo: RDD[(String, String)], sessionAccumulator: SessionAccumulator) = {
    //根据限制条件对RDD进行筛选
    val params_startAge = ParamUtils.getParam(taskJson,Constants.PARAM_START_AGE)
    val params_endAge = ParamUtils.getParam(taskJson,Constants.PARAM_END_AGE)
    val params_startDate = ParamUtils.getParam(taskJson, Constants.PARAM_START_DATE)
    val params_endDate = ParamUtils.getParam(taskJson, Constants.PARAM_END_DATE)
    val params_professionals = ParamUtils.getParam(taskJson, Constants.PARAM_PROFESSIONALS)
    val params_cities = ParamUtils.getParam(taskJson, Constants.PARAM_CITIES)
    val params_sex = ParamUtils.getParam(taskJson, Constants.PARAM_SEX)
    val params_keywords = ParamUtils.getParam(taskJson, Constants.PARAM_KEYWORDS)
    val params_categoryIds = ParamUtils.getParam(taskJson, Constants.PARAM_CATEGORY_IDS)

    sessionId2FullInfo.filter{
      case (sessionId,fullInfo) => {
        var isSuccess = true
        val age = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_AGE)
        val sex = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_SEX)
        val startTime = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_START_TIME)
        val professional = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PROFESSIONAL)
        val city = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CITY)
        val keywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
        val categoryIds = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

        if(StringUtils.isNotEmpty(params_startAge)){
          val temp_params_startAge = params_startAge.toInt
          if(StringUtils.isNotEmpty(age)){
            val tempAge = age.toInt
            if(tempAge >= temp_params_startAge){
              if(StringUtils.isNotEmpty(params_endAge)){
                val temp_params_endAge = params_endAge.toInt
                if(tempAge > temp_params_endAge){
                  //超过了限制年龄
                  isSuccess = false
                }
              }
            }else{
              isSuccess = false
            }
          }
        }

        if(StringUtils.isNotEmpty(params_startDate) && isSuccess){
          if(StringUtils.isNotEmpty(startTime)){
            if(DateUtils.after(startTime,params_startDate+" 00:00:00")){
              if (StringUtils.isNotEmpty(params_endDate)){
                if(DateUtils.after(startTime,params_endDate+" 23:59:59")){
                  isSuccess = false
                }
              }
            }else{
              isSuccess = false
            }
          }
        }

        if(StringUtils.isNotEmpty(params_sex) && isSuccess){
          if(StringUtils.isNotEmpty(sex)){
            if(params_sex.compareTo(sex) != 0){
              isSuccess = false
            }
          }
        }

        if(StringUtils.isNotEmpty(params_professionals) && isSuccess){
          if(StringUtils.isNotEmpty(professional)){
            if(!params_professionals.contains(professional)){
              isSuccess = false
            }
          }
        }

        if(StringUtils.isNotEmpty(params_cities) && isSuccess){
          if(StringUtils.isNotEmpty(city)){
            if(!params_cities.contains(city)){
              isSuccess = false
            }
          }
        }

        if(StringUtils.isNotEmpty(params_keywords) && isSuccess){
          if(StringUtils.isNotEmpty(keywords)){
            if(!params_keywords.contains(keywords)){
              isSuccess = false
            }
          }
        }

        if(StringUtils.isNotEmpty(params_categoryIds) && isSuccess){
          if(StringUtils.isNotEmpty(categoryIds)){
            if(!params_categoryIds.contains(categoryIds)){
              isSuccess = false
            }
          }
        }

        if(isSuccess){
          //自定义累加器进行累加
          sessionAccumulator.add(Constants.SESSION_COUNT)
          val stepLengthStr = StringUtils.getFieldFromConcatString(fullInfo, "\\|",Constants.FIELD_STEP_LENGTH)
          val visitLengthStr = StringUtils.getFieldFromConcatString(fullInfo, "\\|",Constants.FIELD_VISIT_LENGTH)
          //统计步长
          if(StringUtils.isNotEmpty(stepLengthStr)){
            val stepLength = stepLengthStr.toLong
            if(stepLength>=1 && stepLength <= 3){
              sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
            }else if(stepLength>=4 && stepLength <= 6){
              sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
            }else if(stepLength>=7 && stepLength <= 9){
              sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
            }else if(stepLength>=10 && stepLength < 30){
              sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
            }else if(stepLength>=30 && stepLength <60){
              sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
            }else if(stepLength>=60){
              sessionAccumulator.add(Constants.STEP_PERIOD_60)
            }
          }
          //统计访问时长
          if(StringUtils.isNotEmpty(visitLengthStr)){
            val visitLength = visitLengthStr.toLong
            if(visitLength>=1 && visitLength <= 3){
              sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            }else if(visitLength>=4 && visitLength <= 6){
              sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            }else if(visitLength>=7 && visitLength <= 9){
              sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
            }else if(visitLength>=10 && visitLength < 30){
              sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
            }else if(visitLength>=30 && visitLength <60){
              sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
            }else if(visitLength>=60 && visitLength < 180){
              sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
            }else if(visitLength>=180 && visitLength < 600){
              sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
            }else if(visitLength>=600 && visitLength < 1800){
              sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
            }else if(visitLength>=1800){
              sessionAccumulator.add(Constants.TIME_PERIOD_30m)
            }
          }

        }

        isSuccess
      }
    }
  }

  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble
    //访问时长的数据
    val visitLength1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s,0).toDouble
    val visitLength4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s,0).toDouble
    val visitLength7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s,0).toDouble
    val visitLength10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s,0).toDouble
    val visitLength30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s,0).toDouble
    val visitLength1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m,0).toDouble
    val visitLength3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m,0).toDouble
    val visitLength10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m,0).toDouble
    val visitLength30m = value.getOrElse(Constants.TIME_PERIOD_30m,0).toDouble

    //访问步长的数据
    val stepLength1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3,0).toDouble
    val stepLength4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6,0).toDouble
    val stepLength7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9,0).toDouble
    val stepLength10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30,0).toDouble
    val stepLength30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60,0).toDouble
    val stepLength60 = value.getOrElse(Constants.STEP_PERIOD_60,0).toDouble

    //计算访问时长比例
    val visitLength1s_3s_ratio = NumberUtils.formatDouble(visitLength1s_3s/session_count, 2)
    val visitLength4s_6s_ratio = NumberUtils.formatDouble(visitLength4s_6s/session_count, 2)
    val visitLength7s_9s_ratio = NumberUtils.formatDouble(visitLength7s_9s/session_count, 2)
    val visitLength10s_30s_ratio = NumberUtils.formatDouble(visitLength10s_30s/session_count, 2)
    val visitLength30s_60s_ratio = NumberUtils.formatDouble(visitLength30s_60s/session_count, 2)
    val visitLength1m_3m_ratio = NumberUtils.formatDouble(visitLength1m_3m/session_count, 2)
    val visitLength3m_10m_ratio = NumberUtils.formatDouble(visitLength3m_10m/session_count, 2)
    val visitLength10m_30m_ratio = NumberUtils.formatDouble(visitLength10m_30m/session_count, 2)
    val visitLength30m_ratio = NumberUtils.formatDouble(visitLength30m/session_count, 2)

    //计算访问步长的比例
    val stepLength1_3_ratio = NumberUtils.formatDouble(stepLength1_3/session_count, 2)
    val stepLength4_6_ratio = NumberUtils.formatDouble(stepLength4_6/session_count, 2)
    val stepLength7_9_ratio = NumberUtils.formatDouble(stepLength7_9/session_count, 2)
    val stepLength10_30_ratio = NumberUtils.formatDouble(stepLength10_30/session_count, 2)
    val stepLength30_60_ratio = NumberUtils.formatDouble(stepLength30_60/session_count, 2)
    val stepLength60_ratio = NumberUtils.formatDouble(stepLength60/session_count, 2)

    //写入mysql数据库
    val sessionRatio = new SessionAggrStat(taskUUID,session_count.toLong,
      visitLength1s_3s_ratio,visitLength4s_6s_ratio,visitLength7s_9s_ratio,
      visitLength10s_30s_ratio,visitLength30s_60s_ratio,visitLength1m_3m_ratio,
      visitLength3m_10m_ratio,visitLength10m_30m_ratio,visitLength30m_ratio,
      stepLength1_3_ratio,stepLength4_6_ratio,stepLength7_9_ratio,
      stepLength10_30_ratio,stepLength30_60_ratio,stepLength60_ratio)

//    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(sessionRatio))
//    import  sparkSession.implicits._
//    sessionRatioRDD.toDF().write.format("jdbc")
//      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
//      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
//      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
//      .option("dbtable","session_stat_ratio_0416")
//      .mode(SaveMode.Append)
//      .save()
    println(sessionRatio.toString)
  }

}

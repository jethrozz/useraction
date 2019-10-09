import java.util.Random

import commons.constant.Constants
import commons.utils.{DateUtils, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Demand2 {


  /**
    * sessionId2FilterRDD  sessionId-userFullInfo
    */
  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionTotal: Int,
                           sessionId2FilterRDD: RDD[(String, String)]): Unit ={

    //获取到以dateHour为key，fullInfo为value的RDD
    val dateHourRDD:RDD[(String, String)] = sessionId2FilterRDD.map {
      case (sid, fullInfo) => {
        val dateHourStr = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        // dateHour : yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(dateHourStr)

        (dateHour, fullInfo)
      }
    }

    //返回每个key的数量
    // 每天每个小时的session数量
    val dateHourCountMap = dateHourRDD.countByKey()

    //再按天进行聚合
    val dateMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]
    for((dateHour,count) <- dateHourCountMap){
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      val temp:mutable.HashMap[String, Long] =

        dateMap.get(date) match {
          case None => dateMap(date) = new mutable.HashMap[String, Long]()
            dateMap(date) += (hour -> count)
          case Some(map) => dateMap(date) += (hour->count)
        }
    }

    //一共有多少天：dateMap.size
    //每天抽取多少：100/dateMap.size
    //一天有多少session： dateMap(date).value.sum
    // 每天每个小时的session：dateMap(date).value

    //计算出每天每小时要抽取的数量。再进行随机抽取
    val totalDays = dateMap.size
    //每天抽取的数量
    val extractDayNumber = sessionTotal / totalDays
    //每天的session数
    val sessionDayMap = new mutable.HashMap[String, Long]()
    for( (date, map) <- dateMap ){
      sessionDayMap += (date -> dateMap(date).values.sum)
    }

    //计算比例
    // 一个小时要抽取的数量 = (这个小时的session数量 / 这一天的session数量 ) * 这一天要抽取的session数量
    //按dateHour进行分组的RDD
    val dateHour2GroupInfo:RDD[(String, Iterable[String])] = dateHourRDD.groupByKey()
    //随机选出了每天每个小时所需要的数据
    //返回值为 RDD[(date,Iterable[fullInfo])]
    val extractDateHourInfoRDD:RDD[(String, Iterable[SessionRandomExtract])]  = dateHour2GroupInfo.map(item =>{
      val date = item._1.split("_")(0)
      val hour = item._1.split("_")(1)

      //这个小时需要抽取的数量
      val ratioHour = ((dateMap(date).get(hour).get / sessionDayMap(date).toDouble) * extractDayNumber).toInt
      //开始随机抽取
      val random = new Random()
      val array = ListBuffer[Int]()
      val result = ListBuffer[SessionRandomExtract]()
      //生成一组随机数
      var i=0
      while(i<ratioHour){
        val r = random.nextInt(item._2.size)
        if(!array.contains(r)){
          array.append(r)
          i+=1
        }
      }
      i=0
      for(info <- item._2){
        if(array.contains(i)){
          val sessionId = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_SESSION_ID)
          val startTime = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_START_TIME)
          val searchKeyWords = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_SEARCH_KEYWORDS)
          val clickCategories = StringUtils.getFieldFromConcatString(info,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS)
          val extractSession = SessionRandomExtract(taskUUID,sessionId,startTime,searchKeyWords,clickCategories)
          result.append(extractSession)
        }
        i+=1
      }
      //随机抽取结束
      (date,result)
    })
    //再根据key进行聚合一次，就得到了每天的数据
    val extractDateInfoRDD:RDD[SessionRandomExtract]  = extractDateHourInfoRDD.flatMap{
      case (date:String,fullInfo:Iterable[SessionRandomExtract]) => {
        val extractSessionArrayBuffer= new ArrayBuffer[SessionRandomExtract]()
        fullInfo.foreach(item =>{
          extractSessionArrayBuffer.append(item)
        })
        extractSessionArrayBuffer
      }
    }

    extractDateInfoRDD.collect().foreach(println)
    //保存至数据库
    //        import  sparkSession.implicits._
    //        extractDateInfoRDD.toDF().write.format("jdbc")
    //          .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
    //          .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
    //          .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
    //          .option("dbtable","session_extract_0308")
    //          .mode(SaveMode.Append)
    //          .save()
  }
}

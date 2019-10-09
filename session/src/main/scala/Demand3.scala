import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 需求三：统计热门品类Top10
  */
object Demand3 {

  def staticTop10Goods(sparkSession: SparkSession, taskUUID: String, sessid2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
      //获取发生过点击，下单，付款行为的品类ID
    var categoryIdRDD:RDD[(Long, Long)] = sessid2FilterActionRDD.flatMap{
      case (sid, userAction) =>{
        val categoryIdBuffer = new ArrayBuffer[(Long, Long)]()
        //判断点击行为
        if (userAction.click_category_id != -1) {
          categoryIdBuffer.append((userAction.click_category_id,userAction.click_category_id))
        }
        //判断下单行为
        else if (StringUtils.isNotEmpty(userAction.order_category_ids)) {
          for(cid <- userAction.order_category_ids.split(",")){
            categoryIdBuffer.append((cid.toLong,cid.toLong))
          }
        }
        //判断付款行为
        else if (StringUtils.isNotEmpty(userAction.pay_category_ids)) {
          for(cid <- userAction.pay_category_ids.split(",")){
            categoryIdBuffer.append((cid.toLong,cid.toLong))
          }
        }
        categoryIdBuffer
      }
    }
    //去重
    categoryIdRDD = categoryIdRDD.distinct()

    //获取点击次数
    val clickCountRDD: RDD[(Long, Long)] = getClickCount(sessid2FilterActionRDD)
    //获取下单次数
    val orderCountRDD: RDD[(Long, Long)] = getOrderCount(sessid2FilterActionRDD)
    //获取付款次数
    val payCountRDD: RDD[(Long, Long)] = getPayCount(sessid2FilterActionRDD)

    //将分散的数据拼接成一个完整的数据
    val fullInfoRDD = getFullInfo(clickCountRDD,orderCountRDD,payCountRDD, categoryIdRDD)

    //实现自定义二次排序Key
    //将RDD 转换成 RDD[sortKey, fullInfo] 格式的RDD

    val fullInfo2SortKeyFullInfo = fullInfoRDD.map {
      case (cid, fullInfo) => {
        val clickCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = Top10SortKey(clickCount, orderCount, payCount)
        (sortKey, fullInfo)
      }
    }

    //排序并取出前10个
    val top10CategoryArray =  fullInfo2SortKeyFullInfo.sortByKey(false).take(10)
    //写入数据库
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map(item => {
      //先封装数据
      val cid = StringUtils.getFieldFromConcatString(item._2,"\\|",Constants.FIELD_CATEGORY_ID).toLong
      val clickCount = item._1.clickCount
      val orderCount = item._1.orderCount
      val payCount = item._1.payCount
      val top10Category = Top10Category(taskUUID,cid,clickCount,orderCount,payCount)
      top10Category
    })
    //保存至数据库
//    import  sparkSession.implicits._
//    top10CategoryRDD.toDF().write.format("jdbc")
//              .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
//              .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
//              .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
//              .option("dbtable","top10_category_0308")
//              .mode(SaveMode.Append)
//              .save()
    top10CategoryArray
  }

  def getClickCount(sessid2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val clickFilterRDD = sessid2FilterActionRDD.filter(x => {x._2.click_category_id != -1L})
    val clickNumberRDD = clickFilterRDD.map(x => (x._2.click_category_id,1L))
    clickNumberRDD.reduceByKey(_+_)
  }

  def getOrderCount(sessid2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = sessid2FilterActionRDD.filter(x => {StringUtils.isNotEmpty(x._2.order_category_ids)})
    val  orderNumberRDD = orderFilterRDD.flatMap{
      case (sid, userAction) =>{
        val orderBuffer = new ArrayBuffer[(Long, Long)]()
        for(cid <- userAction.order_category_ids.split(",")){
          orderBuffer.append((cid.toLong,1L))
        }
        orderBuffer
      }
    }
    orderNumberRDD.reduceByKey(_+_)
  }

  def getPayCount(sessid2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD = sessid2FilterActionRDD.filter(x => {StringUtils.isNotEmpty(x._2.pay_category_ids)})
    val  payNumberRDD = payFilterRDD.flatMap{
      case (sid, userAction) =>{
        val payBuffer = new ArrayBuffer[(Long, Long)]()
        for(cid <- userAction.pay_category_ids.split(",")){
          payBuffer.append((cid.toLong,1L))
        }
        payBuffer
      }
    }
    payNumberRDD.reduceByKey(_+_)
  }


  def getFullInfo(clickCountRDD: RDD[(Long, Long)], orderCountRDD: RDD[(Long, Long)], payCountRDD: RDD[(Long, Long)], categoryIdRDD: RDD[(Long, Long)]) = {
    //拼接点击品类
    val clickInfoRDD = categoryIdRDD.leftOuterJoin(clickCountRDD).map{
      case ((cid, (ccid, optClickCount))) =>{
        val clickCount = if(optClickCount.isDefined) optClickCount.get else 0
        val clickInfo = Constants.FIELD_CATEGORY_ID + "=" + + cid+ "|"+
          Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid,clickInfo)
      }
    }
    val clickInfo2orderInfoRDD = clickInfoRDD.leftOuterJoin(orderCountRDD).map{
      case ((cid, (clickInfo, optOrderCount))) =>{
        val orderCount = if(optOrderCount.isDefined) optOrderCount.get else 0
        val orderInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount
        (cid, orderInfo)
      }
    }

    val orderInfo2FullRDD = clickInfo2orderInfoRDD.leftOuterJoin(payCountRDD).map{
      case ((cid, (orderInfo , optPayCount))) => {
        val payCount = if(optPayCount.isDefined) optPayCount.get else  0
        val payInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid, payInfo)
      }
    }
    orderInfo2FullRDD
  }
}

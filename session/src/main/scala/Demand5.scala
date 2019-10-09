import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 需求5：页面单跳转化率统计
  */
object Demand5 {

  def pageFlowConvertRate(taskJson: JSONObject, sparkSession: SparkSession, taskUUID: String, sessid2ActionGroupRDD: RDD[(String, Iterable[UserVisitAction])]): Unit = {

    val targetPageFlowStr = ParamUtils.getParam(taskJson, Constants.PARAM_TARGET_PAGE_FLOW);
    if (StringUtils.isNotEmpty(targetPageFlowStr)) {
      //一、首先获取需要的页面切片
      val targetPageFlow = targetPageFlowStr.split(",").map(x => x.toLong)
      val sectionPageFlow = ListBuffer[String]()
      for (i <- 0.until(targetPageFlow.size - 1)) {
        val tag = targetPageFlow(i) + "_" + targetPageFlow(i + 1)
        sectionPageFlow.append(tag)
      }
      val startPage = targetPageFlow(0)

      //将原始的session切片成我们需要格式 （page1_page2,count）
      val pageSectionRDD: RDD[(String, Long)] = sessid2ActionGroupRDD.flatMap {
        case (sid, iterUserAction) => {
          val pageSectionBuffer = new ArrayBuffer[(String, Long)]()
          //首先按时间对该session的用户行为进行排序，以便我们获得正确的页面访问顺序
          val actionsListSortedByTime: List[UserVisitAction] = iterUserAction.toList.sortWith((item1, item2) => {
            DateUtils.after(item1.action_time, item2.action_time)
          })
          //提取出相应的页面id
          val pageList = actionsListSortedByTime.map(item => {
            item.page_id
          }).toArray
          if(pageList.contains(startPage)){
            pageSectionBuffer.append((startPage.toString,1L))
          }
          val sidSectionPageFlow = ListBuffer[String]()
          for (i <- 0.until(pageList.size - 1)) {
            val tag = pageList(i) + "_" + pageList(i + 1)
            sidSectionPageFlow.append(tag)
          }

          for (section <- sidSectionPageFlow) {
            pageSectionBuffer.append((section, 1L))
          }
          pageSectionBuffer
        }
      }
      //筛选出我们需要统计的页面切片
      val pageSectionFilterRDD: RDD[(String, Long)] = pageSectionRDD.filter {
        case (section, count) => {
          if(sectionPageFlow.contains(section))
            true
          else if(section.compareTo(startPage.toString) == 0){
            true
          }else{
            false
          }
        }
      }

      //对page切片进行统计，获取到各个切片的访问次数
      val pageSectionFilterCount = pageSectionFilterRDD.countByKey()
      //startPage在计算时已经放入rdd中，且在过滤时对其进行了忽略，
      var lastPageCount = pageSectionFilterCount.get(startPage.toString).get.toDouble

      val pageSectionCountMap = new mutable.HashMap[String, Double]()

      for( sectionPage <-sectionPageFlow){
        val currentPageCount = pageSectionFilterCount.get(sectionPage).get.toDouble
        val ratio = currentPageCount / lastPageCount
        pageSectionCountMap += (sectionPage->ratio)
        lastPageCount = currentPageCount
      }


      val convertStr = pageSectionCountMap.map{
        case (sectionPage:String,ratio:Double) => {
          sectionPage+"="+ratio.toString
        }
      }.mkString("|")
      val pageSplit = PageSplitConvertRate(taskUUID,convertStr)
      val pageSplitRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))
      //保存至数据库
      //    import  sparkSession.implicits._
      //    pageSplitRDD.toDF().write.format("jdbc")
      //              .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      //              .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      //              .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      //              .option("dbtable","top10_session_0308")
      //              .mode(SaveMode.Append)
      //              .save()
      println(convertStr)
    }
  }

}

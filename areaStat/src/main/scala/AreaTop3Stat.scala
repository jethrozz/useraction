import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object AreaTop3Stat {



  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)
    val taskUUID = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setAppName("areaStat").setMaster("local[1]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val cityId2pidRDD = getCityAndProductInfo(sparkSession, taskParam)

    val cityId2AreaInfoRDD = getCityAreaInfo(sparkSession)

    getAreaPidBasicInfoTable(sparkSession,cityId2pidRDD,cityId2AreaInfoRDD)

    //注册udf函数
    sparkSession.udf.register("concat_long_string",(v1:Long,v2:String,split:String) =>{
      v1.toString +split+v2
    })
    sparkSession.udf.register("group_concat_distinct", GroupConcatDistinct)
    //udf函数 获取json值
    sparkSession.udf.register("get_json_field", (json:String, field:String) => {
      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(field)
    })

    //创建新的临时表，该表聚合的数据格式为
    /**
      * +----+----------+-----------+----------+
      * |area|product_id|click_count|city_infos|
      * +----+----------+-----------+----------+
      * |  华中|        40|         30| 6:长沙,5:武汉|
      *
      */
    getAreaProductClickCountTable(sparkSession)
    getAreaProductClickCountInfo(sparkSession)
    // 需求一：使用开窗函数获取各个区域内点击次数排名前3的热门商品
    val areaTop3ProductRDD = getAreaTop3ProductRDD(sparkSession)

    // 将数据转换为DF，并保存到MySQL数据库
    import sparkSession.implicits._
    val areaTop3ProductDF = areaTop3ProductRDD.rdd.map(row =>
      AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"), row.getAs[Long]("product_id"), row.getAs[String]("city_infos"), row.getAs[Long]("click_count"), row.getAs[String]("product_name"), row.getAs[String]("product_status"))
    ).toDS
//    areaTop3ProductDF.write
//      .format("jdbc")
//      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
//      .option("dbtable", "area_top3_product")
//      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
//      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
//      .mode(SaveMode.Append)
//      .save()

    sparkSession.close()
  }

  /**
    * 需求一：获取各区域top3热门商品
    * 使用开窗函数先进行一个子查询,按照area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
    * 接着在外层查询中，过滤出各个组内的行号排名前3的数据
    *
    * @return
    */
  def getAreaTop3ProductRDD(spark: SparkSession): DataFrame = {

    // 华北、华东、华南、华中、西北、西南、东北
    // A级：华北、华东
    // B级：华南、华中
    // C级：西北、西南
    // D级：东北

    // case when
    // 根据多个条件，不同的条件对应不同的值
    // case when then ... when then ... else ... end

    val sql = "SELECT " +
      "area," +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A级' " +
      "WHEN area='华南' OR area='华中' THEN 'B级' " +
      "WHEN area='西北' OR area='西南' THEN 'C级' " +
      "ELSE 'D级' " +
      "END area_level," +
      "product_id," +
      "city_infos," +
      "click_count," +
      "product_name," +
      "product_status " +
      "FROM (" +
      "SELECT " +
      "area," +
      "product_id," +
      "click_count," +
      "city_infos," +
      "product_name," +
      "product_status," +
      "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
      "FROM tmp_area_fullprod_click_count " +
      ") t " +
      "WHERE rank<=3"

    spark.sql(sql)
  }


  def getAreaProductClickCountInfo(sparkSession: SparkSession) = {
    //tmp_area_click_count:area, city_infos, pid, click_count  tacc
    //product_info:product_id, product_name,extend_info  pi
    // 将之前得到的各区域各商品点击次数表，product_id
    // 去关联商品信息表，product_id，product_name和product_status
    // product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
    // get_json_object()函数，可以从json串中获取指定的字段的值
    // if()函数，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
    // area, product_id, click_count, city_infos, product_name, product_status

    // 你拿到到了某个区域top3热门的商品，那么其实这个商品是自营的，还是第三方的

    // 技术点：内置if函数的使用

    val sql = "SELECT " +
      "tapcc.area," +
      "tapcc.product_id," +
      "tapcc.click_count," +
      "tapcc.city_infos," +
      "pi.product_name," +
      "if(get_json_field(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
      "FROM tmp_area_click_count tapcc " +
      "JOIN product_info pi ON tapcc.product_id=pi.product_id "
    val df = sparkSession.sql(sql)

    df.createOrReplaceTempView("tmp_area_fullprod_click_count")
  }


  def getAreaProductClickCountTable(sparkSession: SparkSession) = {

    // 按照area和product_id两个字段进行分组
    // 计算出各区域各商品的点击次数
    // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
    val sql = "SELECT " +
      "area," +
      "product_id," +
      "count(*) click_count, " +
      "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
      "FROM tmp_area_basic_info " +
      "GROUP BY area,product_id "

    val df = sparkSession.sql(sql)

    // 各区域各商品的点击次数（以及额外的城市列表）,再次将查询出来的数据注册为一个临时表
    df.createOrReplaceTempView("tmp_area_click_count")
  }


  def getAreaPidBasicInfoTable(sparkSession: SparkSession,
                               cityId2pidRDD: RDD[(Long, Long)],
                               cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]) = {
    val areaPidInfoRDD = cityId2pidRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, cityInfo)) => {
        (cityId, cityInfo.city_name, cityInfo.area, pid)
      }
    }

    //将该RDD转化为一张临时表，表中的一行数据，代表一条点击信息
    import  sparkSession.implicits._
    areaPidInfoRDD.toDF("city_id","city_name","area","product_id").createOrReplaceTempView("tmp_area_basic_info")
  }

  def getCityAreaInfo(sparkSession: SparkSession) = {
    val cityInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))
    sparkSession.sparkContext.makeRDD(cityInfo).map{
      case (cityId, cityName, area) =>{
        (cityId, CityAreaInfo(cityId,cityName,area))
      }
    }
  }

  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    val sql = "select city_id, click_product_id from user_visit_action where date >= '"+startDate+"' and date <= '" +endDate+"' and "+
    "click_product_id != -1"

    import  sparkSession.implicits._

    sparkSession.sql(sql).as[CityClickProduct].rdd.map{
      case cityPid =>{
        (cityPid.city_id,cityPid.click_product_id)
      }
    }

  }
}

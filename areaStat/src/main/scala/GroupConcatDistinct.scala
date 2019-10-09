import commons.utils.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

object GroupConcatDistinct extends  UserDefinedAggregateFunction{
  //UDAF 函数 的输入类型：string
  override def inputSchema: StructType = StructType(StructField("cityInfo",StringType)::Nil)

  //UDAF: 缓冲区类型
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo",StringType)::Nil)
  //UDAF: 输出数据类型
  override def dataType: DataType = StringType

  //为true时，保证每次输入数据的时候输出的数据一定是一样的
  override def deterministic: Boolean = true

  //初始化UDAF函数
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    //获取缓冲区0位置的数据：String类型
    var bufferCityInfo = buffer.getString(0)
    //获取输入数据中的第0个数据
    val cityInfo = input.getString(0)

    if(!bufferCityInfo.contains(cityInfo)){

      if("".equals(bufferCityInfo)){
        bufferCityInfo += cityInfo
      }else{
        bufferCityInfo += ","+cityInfo
      }
      buffer.update(0,bufferCityInfo)
    }
  }

  //将两个UDAF的数据汇总在一起
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //bufferCityInfo1: cityId1:cityName1
    var bufferCityInfo1 = buffer1.getString(0)
    val bufferCityInfo2 = buffer2.getString(0)
    for(cityInfo <- bufferCityInfo2.split(",")){
      if(!bufferCityInfo1.contains(cityInfo)){
        if("".equals(bufferCityInfo1)){
          bufferCityInfo1+=cityInfo
        }else{
          bufferCityInfo1 += ","+cityInfo
        }
      }
    }
    buffer1.update(0, bufferCityInfo1)
  }

  //get方法
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionAccumulator extends AccumulatorV2 [String, mutable.HashMap[String,Int]]{
  private val countMap = new  mutable.HashMap[String, Int]()
  override def isZero: Boolean = {
    //累加器维护的结构是否为空
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    //copy方法：复制本累加器
    //生成一个新的累加器并将当前所维护的状态一并返回
    var acc = new SessionAccumulator
    acc.countMap ++= this.countMap
    acc
  }

  override def reset(): Unit = {
    //重置累加器所维护的结构
    countMap.clear()
  }

  override def add(v: String): Unit = {
    if(!countMap.contains(v)){
      //如果不存在，则新添加一个键值对结构
      countMap += (v -> 0)
    }
    //然后进行加一
    countMap.update(v,countMap(v)+1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    //将传入的累加器所维护的键值对结构进行合并
    //scala 中的++= 就是java中的addAll 合并
    other match {
      case acc:SessionAccumulator => acc.countMap.foldLeft(this.countMap) {
        case (map, (k,v)) => map += (k -> (map.getOrElse(k,0)+v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}

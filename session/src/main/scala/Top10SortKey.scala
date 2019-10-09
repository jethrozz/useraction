case class Top10SortKey(clickCount:Long,
                        orderCount:Long,
                        payCount:Long) extends Ordered[Top10SortKey]{


  /**
    * 复写compare方法，
    * 在排序时首先对clickCount 进行比较，如果相等则比较orderCount，如果再相等则比较payCount
    */
  override def compare(that: Top10SortKey): Int = {
    if(this.clickCount - that.clickCount != 0){
      return (this.clickCount - that.clickCount).toInt
    }else if(this.orderCount - that.orderCount != 0){
      return  (this.orderCount - that.orderCount).toInt
    }else{
      return  (this.payCount - that.payCount).toInt
    }
  }
}

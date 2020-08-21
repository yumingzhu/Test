package August

object sortDemo {

  def main(args: Array[String]): Unit = {
    var li = Array[Int](54, 26, 93, 17, 77, 31, 44, 55, 20)
    //    insert_sort(li)
    quick_sort(li, 0, li.length - 1)
    println(li.toList)
    println("67d36dff6e160467489f351e239e4628".toUpperCase())
    println("9acb1349c44cb153de26d663617046bc".toUpperCase())
    println("fe2338cf5c6566b618ce91a673d55c60".toUpperCase())
    println("d1d5ce35d5b4a4f4f000841abda84768".toUpperCase())
    println("995496f6dbf7cb9bc9fb024980ad38e2".toUpperCase())
    println("5d8fca082ccd09554ed02b7bb5571743".toUpperCase())
    println("ae6be309313971c599ca59c242698979".toUpperCase())
    println("98986d5707a5f239aded4b4c48d2091f".toUpperCase())
    println("1ba54e4af28f6538819d5cbcfb570d65".toUpperCase())
    println("424df6eb045fd81233560b1e28c94456".toUpperCase())
    println("efef4fdb254dc12eec2c2bdca24b96c6".toUpperCase())
    println("a377eb4e3e859ef0d69f89b25e08e7d7".toUpperCase())
    println("b52d77721d013eb9c2ad73d09ab7513b".toUpperCase())
    println("e1bc7e2378ae11720989893b88239ae2".toUpperCase())
    println("85d84793372e957d6e9da45ac4871996".toUpperCase())
    println("d605df6fa8504ab5f28b70710ffca859".toUpperCase())
    println("f2fa722ab76a3169c38a311cf25f1f71".toUpperCase())
    println("8c3e01f7a334652c9d8b4005d7b3e639".toUpperCase())
    println("aa4bb346af6d619f6b690ce32f90f468".toUpperCase())
    println("df76ffcf3ae4a41e38cb7042aee66a60".toUpperCase())
    println("b2cb3cad5957faa8cca19cf4f30df55c".toUpperCase())
    println("3a197035057339e7b73e9b5414dd3d8c".toUpperCase())
    println("c5a4d8c4455f44782019e3c51f8664e5".toUpperCase())
    println("089e49a097ee16becd25e81a6f4a1599".toUpperCase())  }

  /**
    * 冒泡
    *
    * @param array
    */
  def bubble_sort(array: Array[Int]): Unit = {
    for (i <- 0 until array.length - 1) {
      for (j <- 0 until array.length - i - 1) {
        if (array(j) > array(j + 1)) {
          var temp = array(j)
          array(j) = array(j + 1)
          array(j + 1) = temp
        }
      }
    }

  }

  /** *
    * 选择排序
    */
  def selection_sort(array: Array[Int]): Unit = {
    for (i <- 0 until array.length - 1) {
      var min_index = i
      //找到最小的那个元素的下标
      for (j <- 0 + i until array.length) {
        if (array(min_index) > array(j)) {
          min_index = j
        }
      }
      if (min_index != i) {
        var temp = array(i)
        array(i) = array(min_index)
        array(min_index) = temp
      }
    }

  }

  /** *
    * 插入排序
    */
  def insert_sort(array: Array[Int]): Unit = {
    for (i <- 1 until array.length) {
      //不断的从上一个循环中拿去一个 元素  去有序的中间进行插入
      for (j <- (1 to i).reverse) {
        if (array(j) < array(j - 1)) {
          var temp = array(j)
          array(j) = array(j - 1)
          array(j - 1) = temp
        }
      }
    }

  }

  /**
    * 快排核心算法，递归实现
    *
    * @param array
    * @param left
    * @param right
    */
  def quick_sort(array: Array[Int], left: Int, right: Int): Unit = {
    if (left > right) return
    var base = array(left)
    var i = left
    var j = right
    while (i != j) {
      //必须   先从右往左 找出比base 小的元素，  确保最后一次 i=j 时 i 为小于或者 等于base的元素
      while (array(j) >= base && i < j) j = j - 1
      //将 左边大的数据 ，和右边小的数据进行交换
      //从左往右找出 比base 大的元素
      while (array(i) <= base && i < j) i = i + 1
      if (i < j) {
        val temp = array(j)
        array(j) = array(i)
        array(i) = temp
      }
    }
    //跳出循环是当前位置，i 左边的都小于base 右边的都大于 base
    array(left) = array(i)
    array(i) = base

    quick_sort(array, left, i - 1)
    quick_sort(array, i + 1, right)
  }


}

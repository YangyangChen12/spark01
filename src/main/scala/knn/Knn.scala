package knn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.{pow, sqrt}

/**
 * @author Haodong Chen
 * @date 2019/12/27 14:15
 * @version 1.0
 */
object Knn {

  case class TabCoord(tab: String, coord: Array[Double])

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getCanonicalName}")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val K = 9
    val text = sc.textFile("G:\\IdeaProjects\\spark01\\dir01\\iris.dat")
      .map(line => {
        val arr = line.split(",")
        if (arr.length == 4)
          TabCoord("", arr.map(_.toDouble))
        else
          TabCoord(arr.last, arr.init.map(_.toDouble))

      })
    val sample: RDD[TabCoord] = text
      .filter(_.tab != "")
    val test: Array[Array[Double]] = text
      .filter(_.tab == "").collect().map(_.coord)
    println()

    test.foreach(elem => {
      val dists: RDD[(Double, String)] = sample
        .map(tabCoord =>
          (getDistance(elem, tabCoord.coord), tabCoord.tab)
        )
      val min: Array[(Double, String)] = dists.sortBy(_._1).take(K)
      val tab = min.map(_._2)
      print(s"${elem.toBuffer}:")

      tab.groupBy(x => x)
        .mapValues(_.length)
        .foreach(print)
      println()
    })


    sc.stop()
  }
  //获取距离
  def getDistance(x: Array[Double], y: Array[Double]): Double = {
    sqrt(x.zip(y)
      .map(z => pow(z._1 - z._2, 2))
      .sum)
  }

}
package org.apache.spark.examples.streaming
/* kMean.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import util.control.Breaks._
import java.util.Properties
import org.apache.spark.rdd.RDD

object kMean {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input_file = args(0)
    val inputData = sc.textFile(input_file)
    val values = inputData.map(inputData=>inputData.split(" "))
    // values.foreach(x=>println(x.mkString(" ")))
    // println(values.take(3).foreach(x=>println(x.mkString(" "))))
    // val nthVal = values.zipWithIndex.filter(_._2==9).map(_._1).first()
    // println(nthVal.mkString(" "))
    var centroids = genNRands(values, 3, values.count().asInstanceOf[Int])
    //println(centroids.foreach(x=>println(x.mkString(" "))))
    var distDiff:Long = 99999999
    var oldDist:Long = 99999999
    var pairs = values.map( x => ( findCluster(x, centroids)) ) 
    while (distDiff >  100){
        pairs = values.map( x => ( findCluster(x, centroids)) )
        val xpairs = pairs.map(x => (x(0), x(2)))
        val ypairs = pairs.map( x => (x(0), x(3)))
        val xSum = xpairs.reduceByKey(_+_)
        val ySum = ypairs.reduceByKey(_+_)
        val dist = pairs.map(x => (1, x(1))).reduceByKey(_+_).collect()
        val distance = dist(0)._2
        val xCount = pairs.map(x => (x(0), 1)).reduceByKey(_+_)
        val yCount = pairs.map(y => (y(0), 1)).reduceByKey(_+_)
        val xavg = xSum.join(xCount).mapValues{ case (sum, count) => (sum)/count}
        val yavg = ySum.join(yCount).mapValues{ case (sum, count) => (sum)/count}
    
        // pairs.foreach(x=>println(x.mkString(" ")))
        // xavg.foreach(println)
        // yavg.foreach(println)
        val c = xavg.join(yavg)
        // c.foreach(println)
        centroids = c.map(x => Array(x._2._1.toString, x._2._2.toString)).toArray()
        // centroids.foreach(x=>println(x.mkString(" ")))
        // println(distance)
        distDiff = oldDist - distance
        oldDist = distance
        // println(distDiff)
    }
    println("actual x\tactual y\tcentroid x\tcentroid y")
    pairs.foreach(printOut)
    
  }

  def printOut(vals: Array[Long]) = {
    val x = vals(2).toString
    val y = vals(3).toString
    val cx = vals(4).toString
    val cy = vals(5).toString
    println(x + "\t" + y + "\t" + cx + "\t" + cy)
  }

  def genNRands(vals: RDD[Array[String]], x: Int, n: Int) : Array[Array[String]] = {
    val r = scala.util.Random
    var A:Map[Int, Int] = Map()
    var B:Map[(Int, Int), (Int, Int)] = Map()
    var ret:Array[Array[String]] = new Array[Array[String]](x)
    var i:Int = 0
    while( i < x ){
       val gen = r.nextInt(n)
       breakable {
        if (A.contains(gen)){
            break
       } else{
        A += (gen -> gen)
        val nthVal = vals.zipWithIndex.filter(_._2==gen).map(_._1).first()
        if (B.contains((nthVal(0).toInt, nthVal(1).toInt))){
            break
        } else {
            ret(i) = nthVal
            B += ((nthVal(0).toInt, nthVal(1).toInt) -> (nthVal(0).toInt, nthVal(1).toInt))
            i = i + 1
        }
       }
      }
    }
    ret
  }

  def findCluster(p:Array[String], centroids:Array[Array[String]]): Array[Long] = {
    val x = p(0).toInt
    val y = p(1).toInt
    var i = 0
    var min = -1
    var Dist:Long = 99999999;
    var x2:Int = -1
    var y2:Int = -1
    for(i <- 0 until centroids.length){
        val x1 = centroids(i)(0).toInt
        val y1 = centroids(i)(1).toInt
        val d = getDistance(x, y, x1, y1)
        if ( d < Dist ){
            Dist = d
            min = i
            x2 = x1
            y2 = y1
        }
    }
    val r = Array(min, Dist, x, y, x2, y2)
    r
  }

  def getDistance(x1:Int, y1:Int, x2:Int, y2:Int): Long = {
    val ret = (x1 - x2)*(x1 - x2) + (y1 - y2)*(y1 -y2)
    ret
  }
}


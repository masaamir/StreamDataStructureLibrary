package ds.src

import scala.util.Random
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

/**
 * @author Rohit
 */
class CountMinSketch(depth: Int, width: Int, itemCount: Int) {
  val PRIME_MODULUS = (1L << 31) - 1
  var table: Array[Array[Long]] = Array.ofDim(depth, width)
  var hashA: Array[Long] = {
    var r = new Random()
    var hashA: Array[Long] = Array.ofDim(depth)
    for (i <- 0 to depth - 1) {
      hashA(i) = r.nextInt(Int.MaxValue)
    }
    hashA
  }
  var size: Long = 0l
  var eps: Double = 2.0 / width
  var confidence: Double = 1 - 1 / Math.pow(2, depth)
  println(eps)
  println(confidence)
  def hash(item: Long, i: Int): Int = {
    var hash = hashA(i) * item;
    // A super fast way of computing x mod 2^p-1
    // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
    // page 149, right after Proposition 7.
    hash += hash >> 32;
    hash &= PRIME_MODULUS;
    // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
    (hash.toInt) % width;
  }
  def add(item: Long, count: Long) {
    if (count < 0) {
      // Actually for negative increments we'll need to use the median
      // instead of minimum, and accuracy will suffer somewhat.
      // Probably makes sense to add an "allow negative increments"
      // parameter to constructor.
      throw new IllegalArgumentException("Negative increments not implemented");
    }
    for (i <- 0 to depth - 1) {
      table(i)(hash(item, i)) += count
    }
    size += count;
  }
  def add(item: Long) {
    add(item, 1l)
  }
  def estimateCount(item: Long): Long = {
    var res = Long.MaxValue
    for (i <- 0 to depth - 1) {
      res = Math.min(res, table(i)(hash(item, i)))
    }
    res
  }
  def count(freq: Long): Double = {
    var cnt = 0l
    var sum: Double = 0
    var prod: Double = 1
    for (i <- 0 to depth - 1) {
      for (j <- 0 to width - 1) {
        if (table(i)(j) > freq) {
          sum += 1
        }
      }
      prod = prod * (sum / width)
      sum = 0
    }

    prod * itemCount
  }

}
object Test {
  def main(args: Array[String]) {
    val distinct = 2000
    val streamsize = 100000
    val freq = 50
    var cms = new CountMinSketch(100, 1000, distinct)
    var r = new Random()
    var items: HashSet[Long] = HashSet.empty
    var temp = 0
    while (items.size < distinct) {

      items.add(r.nextLong())

    }
    val itemList = items.toList
    var index = new Random()
    var data: HashMap[Long, Int] = HashMap.empty
    for (i <- 0 to streamsize) {
      temp = index.nextInt(distinct)

      data.update(itemList(temp), data.getOrElse(itemList(temp), 0) + 1)
      cms.add(itemList(temp))
    }
    println(data.toList.filter(p => {
      p._2 > freq
    }).size)
    println(cms.count(freq))
    for (i <- 0 to 10) {
      temp = index.nextInt(distinct)
      print(data.getOrElse(itemList(temp), 0))
      println(" "+cms.estimateCount(itemList(temp)))
    }
    
  }
}
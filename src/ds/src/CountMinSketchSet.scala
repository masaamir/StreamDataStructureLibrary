package ds.src

import collection.mutable.HashMap
import java.util.Random

/**
 * @author Rohit
 */
class CountMinSketchSet(depth: Int, width: Int, seed: Int) {
  var table = Array.ofDim[HashMap[Int, Long]](depth, width)
  var hashA = new Array[Long](depth)
  initialize()
  val PRIME_MODULUS = (1L << 31) - 1
  
  private def initialize() {

    var r = new Random(seed)
    for (i <- 0 to depth - 1) {
      hashA(i) = r.nextInt(Int.MaxValue)
    }
  }

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
  def estimate(item: Int): Int = {
    var size = 0
    val sum = get(item)
    if (sum != null) {
      size = sum.size
    }
    size
  }
  def add(item: Int, summary: HashMap[Int, Long]) {
    for (i <- 0 to depth - 1) {
      table(i)(hash(item, i)) = summary
    }
  }
  def add(item: Int, summary: HashMap[Int, Long], i: Int) {

    table(i)(hash(item, i)) = summary

  }
  def get(item: Int, i: Int): HashMap[Int, Long] = {
    table(i)(hash(item, i))

  }
  def get(item: Int): HashMap[Int, Long] = {
    var temp: HashMap[Int, Long] = null
    var size = 0
    var minsize = Int.MaxValue
    var result: HashMap[Int, Long] = null
    for (i <- 0 to depth - 1) {
      temp = table(i)(hash(item, i))
      if (temp != null) {
        size = temp.size
      } else {

        return null
      }
      if (size < minsize) {
        result = temp
        minsize = size
      }

    }

    result
  }

}
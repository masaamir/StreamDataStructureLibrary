package ds.src
import java.util.Date
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer

import ds.util._
/**
 * @author Rohit
 */
class ModifiedHLL(number_of_buck: Int) {
  var buckets: Array[ArrayBuffer[(Byte, Long)]] = new Array(number_of_buck)
  private var currentsum = number_of_buck.toDouble
  private var nonZeroBuckets = 0
  def add(value: Long): Unit = {
    add(value, new Date().getTime)
  }
  def add(value: Long, time: Long): Unit = {
    val bucketAndHash = BucketAndHash.fromHash(
      HyperLogLogUtil.computeHash(value), buckets.length)
    val bucket = bucketAndHash.getBucket();
    val lowestBitPosition: Byte = (java.lang.Long.numberOfTrailingZeros(bucketAndHash
      .getHash()) + 1).toByte

    updateBucket(bucket, lowestBitPosition, time)

  }
  def union(newbuckets: Array[ArrayBuffer[(Byte, Long)]], time: Long, window: Double): Unit = {
    var temp: ArrayBuffer[(Byte, Long)] = null
    for (i <- 0 to newbuckets.length - 1) {
      if (!(newbuckets(i) == null || newbuckets.length == 0)) {
        temp = newbuckets(i)
        for (j <- 0 to temp.length - 1) {
          if ((temp(j)._2 - time) <= window) {
            updateBucket(i, temp(j)._1, temp(j)._2)
          }

        }
      }
    }

  }
  //  private def updateBucket(bucket: Int, lowestBitPosition: Byte, time: Long): Unit = {
  //    var previous = buckets(bucket)
  //
  //    if (previous == null || previous.length == 0) {
  //      nonZeroBuckets = nonZeroBuckets + 1
  //      previous = Array((lowestBitPosition, time))
  //    } else {
  //      //Ignore the new values if it all ready exist or if there exist a greater value at earlier time 
  //      val exists = previous.exists(p => {
  //        if (lowestBitPosition == p._1 && time == p._2) {
  //          true
  //        } else if (lowestBitPosition < p._1 && time > p._2) {
  //          true
  //        } else {
  //          false
  //        }
  //      })
  //      if (!exists) {
  //
  //        //remove those values which are smaller then new value and at older time
  //        previous = previous.filter(p => {
  //          if (lowestBitPosition > p._1 && time < p._2) {
  //            false
  //          } else {
  //            true
  //          }
  //        })
  //        //add the new value and time
  //        previous = previous :+ (lowestBitPosition, time)
  //
  //      }
  //    }
  //    buckets.update(bucket, previous)
  //  }
  private def updateBucket(bucket: Int, lowestBitPosition: Byte, time: Long): Boolean = {
    var previous = buckets(bucket)

    if (previous == null || previous.length == 0) {
      //initial case nothing is there
      nonZeroBuckets = nonZeroBuckets + 1
      currentsum -= 1.0 / (1L << 0)
      previous = ArrayBuffer((lowestBitPosition, time))
      buckets.update(bucket, previous)
      return true
    } else {
      //if some data is there
      var newlist: ArrayBuffer[(Byte, Long)] = ArrayBuffer.empty
      for (i <- 0 to previous.length - 1) {
        if (previous(i)._1 == lowestBitPosition) {
          //if the list contains the new value then no use of newlist
          if (previous(i)._2 == time) {
            return false //nothing needs to be done
          } else if (previous(i)._2 > time) {
            // all the previous elements will remain only time for one entry got updated
            previous.update(i, (lowestBitPosition, time))
            buckets.update(bucket, previous)
            return true
          } else {
            //nothing needs to be done
            return false
          }
        } else if (previous(i)._2 == time) {
          //if the list contains the new time then no use of newlist
          if (previous(i)._1 < lowestBitPosition) {
            // all the previous elements will remain only value for one entry got updated
            previous.update(i, (lowestBitPosition, time))
            buckets.update(bucket, previous)
            return true
          } else {
            return false //nothing needs to be done
          }
        } else {
          //if the list does not contains the new value then copy valid values from previous to new
          if (lowestBitPosition < previous(i)._1 && time > previous(i)._2) {
            return false //nothing needs to be done
          } else {
            if (lowestBitPosition < previous(i)._1 && time < previous(i)._2) {
              newlist.append((previous(i)._1, previous(i)._2))
            } else if (lowestBitPosition > previous(i)._1 && time > previous(i)._2) {
              newlist.append((previous(i)._1, previous(i)._2))
            } else {
              //nothing to do ignore this entry

            }
          }
        }

      } //end of for
      newlist.append((lowestBitPosition, time))
      buckets.update(bucket, newlist)
      return true
    }

  }
  def estimate(): Double = {
    val alpha = HyperLogLogUtil.computeAlpha(buckets.length);
    var result = alpha * buckets.length * buckets.length / getcurrentSum;

    if (result <= 2.5 * buckets.length) {
      // adjust for small cardinalities
      var zeroBuckets = buckets.length - nonZeroBuckets;
      if (zeroBuckets > 0) {
        result = buckets.length * Math.log(buckets.length * 1.0 / zeroBuckets);
      }
    }

    return result;
  }

  private def getcurrentSum(): Double = {
    var list: ArrayBuffer[(Byte, Long)] = null

    for (i <- 0 to buckets.length - 1) {
      list = buckets(i)
      if (list == null || list.length == 0) {

      } else {
        list = list.sortBy(f => (-1 * f._1))

        currentsum += 1.0 / (1L << list(0)._1.toInt)
      }

    }
    currentsum
  }

  def convertToHLL(): HyperLogLog = {

    var list: ArrayBuffer[(Byte, Long)] = null
    var buck: Array[Int] = new Array(number_of_buck)

    for (i <- 0 to buckets.length - 1) {
      list = buckets(i)
      if (list == null || list.length == 0) {
        buck(i) = 0
      } else {
        list = list.sortBy(f => (-1 * f._1))

        buck(i) = list(0)._1.toInt
      }

    }
    new HyperLogLog(buck)

  }

}
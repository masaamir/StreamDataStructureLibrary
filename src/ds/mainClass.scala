package ds

import ds.src.SlidingHLLScala

/**
  * Created by maas_local on 24/10/2017.
  */


object mainClass {

  def exampleSHLL(): Unit ={

    //initialize shll with number of buckets
    val shll=new SlidingHLLScala(256)
    // add values in stream using following function:
    // add reasonably large number of elements to get better approx count
    shll.add(1)
    // get unique count from start using following function
    shll.estimate()
    // get unique count within a window using by giving the window size in milliseconds
    shll.estimate(1)


  }

  def main(args: Array[String]): Unit = {
    exampleSHLL()
  }

}

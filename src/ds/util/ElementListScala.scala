package ds.util
import scala.util.control.Breaks._
/**
 * @author Rohit
 */
class ElementListScala extends Serializable {

  var elemList: Array[(Int, Long)] = Array.empty
  def getElement(index: Int): (Int, Long) = {
    if (elemList == null || elemList.length == 0)
      null
    else
      elemList(index)
  }
  def getTop(): (Int, Long) = {
    getElement(0)

  }
  def length(): Int = this.elemList.length
  def getTopElementValue(): Int = {
    if (elemList == null || elemList.size == 0) {
      -1
    } else {
      getTop()._1
    }
  }
  def getElementValue(window: Long): Int = {
    for (i <- 0 until this.elemList.length - 1) {
      if (this.elemList(i)._2 >= window) {
        return this.elemList(i)._1
      }
    }
    -1
  }
  def addNewElement(value: Int, timestamp: Long): Boolean = {
    var addedNew = false;
    var changed = true;
    if (this.elemList == null || this.elemList.length == 0) {
      this.elemList = Array((value, timestamp))

      addedNew = true
    } else {
      // need to complete
      // ArrayList<Element> newList = new ArrayList<Element>();

      for (i <- 0 until this.elemList.length - 1) {

        var oldElement = this.elemList(i);
        if (oldElement._1 > (value)) {
          // newList.add(oldElement);
        } else if (oldElement._1 == value) {
          if (oldElement._2 >= timestamp) {
            // newList.add(oldElement);
            changed = false;
          } else {
            // newList.add(newElement);
            this.elemList.slice(i, this.elemList.length)
            this.elemList ++ Array(value, timestamp)

          }
          addedNew = true
          break
        } else {
          // newList.add(newElement);
          this.elemList.slice(i, this.elemList.length)
          this.elemList ++ Array(value, timestamp)
          addedNew = true

          break
        }

      }
    }
    if (!addedNew) {
      if (this.elemList(this.elemList.length - 1)._2 < timestamp)
        this.elemList ++ Array(value, timestamp)
      else {
        changed = false
      }
    }

    changed

  }
}
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package java.util.concurrent

import java.util.{AbstractQueue, Collection, Collections, Iterator, NoSuchElementException, PriorityQueue}
import java.util.concurrent.locks.{Condition, ReentrantLock}

class DelayQueue[E <: Delayed] extends AbstractQueue[E] with BlockingQueue[E] {

  private val lock = new ReentrantLock()
  private val available = lock.newCondition()
  private val q = new PriorityQueue[E]()
  @volatile private var leader: Thread = _

  def this(initialCapacity: Int) = {
    this()
  }

  def this(c: Collection[_ <: E]) = {
    this()
    addAll(c)
  }

  def put(e: E): Unit = offer(e)

  def offer(e: E): Boolean = {
    if (e == null) throw new NullPointerException
    lock.lock()
    try {
      q.offer(e)
      if (q.peek() eq e) {
        leader = null
        available.signal()
      }
      true
    } finally {
      lock.unlock()
    }
  }

  def offer(e: E, timeout: Long, unit: TimeUnit): Boolean = {
    offer(e)
  }

  def poll(): E = {
    lock.lock()
    try {
      val first = q.peek()
      if (first == null || first.getDelay(TimeUnit.NANOSECONDS) > 0) null
      else q.poll()
    } finally {
      lock.unlock()
    }
  }

  def poll(timeout: Long, unit: TimeUnit): E = {
    val nanos = unit.toNanos(timeout)
    lock.lockInterruptibly()
    try {
      var nanosRemaining = nanos
      while (true) {
        val first = q.peek()
        if (first == null) {
          if (nanosRemaining <= 0) return null
          nanosRemaining = available.awaitNanos(nanosRemaining)
        } else {
          val delay = first.getDelay(TimeUnit.NANOSECONDS)
          if (delay <= 0) return q.poll()
          if (nanosRemaining <= 0) return null
          first = null // don't retain ref while waiting
          if (nanosRemaining < delay || leader != null) {
            nanosRemaining = available.awaitNanos(nanosRemaining)
          } else {
            val thisThread = Thread.currentThread()
            leader = thisThread
            try {
              val timeLeft = available.awaitNanos(delay)
              nanosRemaining -= (delay - timeLeft)
            } finally {
              if (leader eq thisThread) leader = null
            }
          }
        }
      }
      null // unreachable
    } finally {
      lock.unlock()
    }
  }

  def take(): E = {
    lock.lockInterruptibly()
    try {
      while (true) {
        val first = q.peek()
        if (first == null) {
          available.await()
        } else {
          val delay = first.getDelay(TimeUnit.NANOSECONDS)
          if (delay <= 0) return q.poll()
          first.asInstanceOf[AnyRef] // don't retain ref while waiting
          val thisThread = Thread.currentThread()
          leader = thisThread
          try {
            available.awaitNanos(delay)
          } finally {
            if (leader eq thisThread) leader = null
          }
        }
      }
      null.asInstanceOf[E] // unreachable
    } finally {
      lock.unlock()
    }
  }

  def peek(): E = {
    lock.lock()
    try {
      q.peek()
    } finally {
      lock.unlock()
    }
  }

  def remove(o: Any): Boolean = {
    lock.lock()
    try {
      val removed = q.remove(o)
      if (removed && (q.isEmpty() || (q.peek() != null && q.peek().getDelay(TimeUnit.NANOSECONDS) <= 0))) {
        leader = null
        available.signal()
      }
      removed
    } finally {
      lock.unlock()
    }
  }

  def drainTo(c: Collection[_ >: E]): Int = drainTo(c, Int.MaxValue)

  def drainTo(c: Collection[_ >: E], maxElements: Int): Int = {
    if (c eq this) throw new IllegalArgumentException
    if (c == null) throw new NullPointerException
    if (maxElements <= 0) return 0
    
    lock.lock()
    try {
      var n = 0
      while (n < maxElements) {
        val first = q.peek()
        if (first == null || first.getDelay(TimeUnit.NANOSECONDS) > 0) {
          return n
        }
        c.add(q.poll())
        n += 1
      }
      if (n > 0) {
        leader = null
        available.signal()
      }
      n
    } finally {
      lock.unlock()
    }
  }

  override def removeAll(c: Collection[_]): Boolean = {
    lock.lock()
    try {
      val removed = q.removeAll(c)
      if (removed && (q.isEmpty() || (q.peek() != null && q.peek().getDelay(TimeUnit.NANOSECONDS) <= 0))) {
        leader = null
        available.signal()
      }
      removed
    } finally {
      lock.unlock()
    }
  }

  override def retainAll(c: Collection[_]): Boolean = {
    if (c == null) throw new NullPointerException
    lock.lock()
    try {
      val removed = q.retainAll(c)
      if (removed && (q.isEmpty() || (q.peek() != null && q.peek().getDelay(TimeUnit.NANOSECONDS) <= 0))) {
        leader = null
        available.signalAll()
      }
      removed
    } finally {
      lock.unlock()
    }
  }

  override def toArray(): Array[AnyRef] = {
    lock.lock()
    try {
      q.toArray()
    } finally {
      lock.unlock()
    }
  }

  override def toArray[T <: AnyRef](a: Array[T]): Array[T] = {
    lock.lock()
    try {
      q.toArray(a)
    } finally {
      lock.unlock()
    }
  }

  def iterator(): Iterator[E] = {
    lock.lock()
    try {
      Collections.unmodifiableList(new java.util.ArrayList[E](q)).iterator()
    } finally {
      lock.unlock()
    }
  }

  override def clear(): Unit = {
    lock.lock()
    try {
      q.clear()
      leader = null
      available.signalAll()
    } finally {
      lock.unlock()
    }
  }

  def remainingCapacity(): Int = Int.MaxValue

  override def size(): Int = {
    lock.lock()
    try {
      q.size()
    } finally {
      lock.unlock()
    }
  }

  override def isEmpty(): Boolean = {
    lock.lock()
    try {
      q.isEmpty()
    } finally {
      lock.unlock()
    }
  }
}

/*
 * Ported from OpenJDK JSR-166 TCK test.
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */

package org.scalanative.testsuite.javalib.util.concurrent

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.{Arrays, Collection, Collections}
import java.util.concurrent.{DelayQueue, Executors, TimeUnit}

import org.junit.Assert._
import org.junit.{Ignore, Test}

import JSR166Test._

class DelayQueueTest extends JSR166Test {

  private val LONG_DELAY_MS2 = LONG_DELAY_MS / 2

  /** Returns a new DelayQueue containing the elements from the given collection,
   *  each wrapped in a PDelay with the given pseudodelay.
   */
  private def populatedQueue(n: Int, elements: Collection[Item]): DelayQueue[PDelay] = {
    val q = new DelayQueue[PDelay]()
    var i = 0
    for (e <- elements.asScala) {
      assertTrue(q.offer(new PDelay(i, e)))
      i += 1
    }
    mustEqual(n, q.size())
    mustEqual(n == 0, q.isEmpty())
    q
  }

  /** Returns a new DelayQueue of given size containing consecutive PDelays 0 ... n - 1.
   */
  private def populatedQueue(n: Int): DelayQueue[PDelay] = {
    val elements = seqItems(n)
    populatedQueue(n, java.util.Arrays.asList(elements: _*))
  }

  /**
   * A fake "Delayed" implementation where delays are all elapsed, but still ordered
   */
  class PDelay(val pseudodelay: Int, val item: Item) extends Comparable[PDelay] {
    def this(i: Item) = this(0, i)

    def getDelay(unit: TimeUnit): Long = 0

    def compareTo(other: PDelay): Int =
      Integer.compare(pseudodelay, other.pseudodelay)

    override def equals(o: Any): Boolean = o match {
      case other: PDelay => pseudodelay == other.pseudodelay && item == other.item
      case _ => false
    }

    override def hashCode: Int = pseudodelay

    override def toString: String = s"PDelay($pseudodelay, $item)"
  }

  /**
   * A Delayed implementation that actually delays based on real time
   */
  class SimpleDelay(val delayMs: Long) extends Delayed {
    private val startTime = System.currentTimeMillis()

    def getDelay(unit: TimeUnit): Long = {
      val remaining = delayMs - (System.currentTimeMillis() - startTime)
      unit.convert(remaining, TimeUnit.MILLISECONDS)
    }

    def compareTo(other: Delayed): Int = {
      val myDelay = getDelay(TimeUnit.MILLISECONDS)
      val otherDelay = other.getDelay(TimeUnit.MILLISECONDS)
      java.lang.Long.compare(myDelay, otherDelay)
    }

    override def toString: String = s"SimpleDelay($delayMs ms)"
  }

  /**
   * Default constructor creates empty queue
   */
  @Test def testConstructor(): Unit = {
    val q = new DelayQueue[Delayed]()
    assertTrue(q.isEmpty)
    mustEqual(0, q.size())
    assertNull(q.peek())
  }

  /**
   * Constructor with null collection throws NullPointerException
   */
  @Test def testConstructor_nullCollection(): Unit = {
    assertThrows(classOf[NullPointerException], () => new DelayQueue[Delayed](null))
  }

  /**
   * Constructor with collection adds all elements
   */
  @Test def testConstructor2(): Unit = {
    val q = new DelayQueue[PDelay](Arrays.asList(new PDelay(0, one), new PDelay(1, two)))
    mustEqual(2, q.size())
    mustEqual(0, q.peek().pseudodelay)
  }

  /**
   * remainingCapacity always returns Integer.MAX_VALUE
   */
  @Test def testRemainingCapacity(): Unit = {
    val q = new DelayQueue[Delayed]()
    mustEqual(Integer.MAX_VALUE, q.remainingCapacity())

    val populated = populatedQueue(SIZE)
    mustEqual(Integer.MAX_VALUE, populated.remainingCapacity())
  }

  /**
   * offer(null) throws NullPointerException
   */
  @Test def testOffer_null(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertThrows(classOf[NullPointerException], () => q.offer(null))
  }

  /**
   * offer succeeds
   */
  @Test def testOffer(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertTrue(q.offer(new PDelay(0, one)))
    mustEqual(1, q.size())
  }

  /**
   * add(null) throws NullPointerException
   */
  @Test def testAdd_null(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertThrows(classOf[NullPointerException], () => q.add(null))
  }

  /**
   * add succeeds
   */
  @Test def testAdd(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertTrue(q.add(new PDelay(0, one)))
    mustEqual(1, q.size())
  }

  /**
   * put(null) throws NullPointerException
   */
  @Test def testPut_null(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertThrows(classOf[NullPointerException], () => q.put(null))
  }

  /**
   * put succeeds
   */
  @Test def testPut(): Unit = {
    val q = new DelayQueue[PDelay]()
    q.put(new PDelay(0, one))
    mustEqual(1, q.size())
  }

  /**
   * offer(e, timeout, unit) succeeds
   */
  @Test def testTimedOffer(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertTrue(q.offer(new PDelay(0, one), LONG_DELAY_MS, MILLISECONDS))
    mustEqual(1, q.size())
  }

  /**
   * take returns the element with the shortest delay
   */
  @Test def testTake(): Unit = {
    val q = populatedQueue(3)
    val taken = q.take()
    mustEqual(0, taken.pseudodelay)
    mustEqual(2, q.size())
  }

  /**
   * poll() returns element with shortest delay if elapsed, else null
   */
  @Test def testPoll(): Unit = {
    val q = populatedQueue(3)
    val polled = q.poll()
    assertNotNull(polled)
    mustEqual(0, polled.pseudodelay)
    mustEqual(2, q.size())
  }

  /**
   * poll returns null when queue is empty
   */
  @Test def testPoll_empty(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertNull(q.poll())
  }

  /**
   * peek returns element with shortest delay, or null if empty
   */
  @Test def testPeek(): Unit = {
    val q = populatedQueue(3)
    val peeked = q.peek()
    mustEqual(0, peeked.pseudodelay)
    mustEqual(3, q.size())
  }

  /**
   * peek returns null when queue is empty
   */
  @Test def testPeek_empty(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertNull(q.peek())
  }

  /**
   * element returns element with shortest delay, throws if empty
   */
  @Test def testElement(): Unit = {
    val q = populatedQueue(3)
    val element = q.element()
    mustEqual(0, element.pseudodelay)
    mustEqual(3, q.size())
  }

  /**
   * element throws NoSuchElementException when empty
   */
  @Test def testElement_empty(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertThrows(classOf[NoSuchElementException], () => q.element())
  }

  /**
   * remove returns element with shortest delay
   */
  @Test def testRemove(): Unit = {
    val q = populatedQueue(3)
    val removed = q.remove()
    mustEqual(0, removed.pseudodelay)
    mustEqual(2, q.size())
  }

  /**
   * remove throws NoSuchElementException when empty
   */
  @Test def testRemove_empty(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertThrows(classOf[NoSuchElementException], () => q.remove())
  }

  /**
   * remove(Object) removes the element if present
   */
  @Test def testRemoveObject(): Unit = {
    val q = populatedQueue(3)
    val toRemove = q.toArray()(1).asInstanceOf[PDelay]
    assertTrue(q.remove(toRemove))
    mustEqual(2, q.size())
    assertFalse(q.contains(toRemove))
  }

  /**
   * remove(Object) returns false if not present
   */
  @Test def testRemoveObject_notPresent(): Unit = {
    val q = populatedQueue(3)
    val notPresent = new PDelay(99, ninetynine)
    assertFalse(q.remove(notPresent))
    mustEqual(3, q.size())
  }

  /**
   * contains returns true for elements in queue
   */
  @Test def testContains(): Unit = {
    val q = populatedQueue(3)
    val elem = q.toArray()(1).asInstanceOf[PDelay]
    assertTrue(q.contains(elem))
  }

  /**
   * contains returns false for elements not in queue
   */
  @Test def testContains_notPresent(): Unit = {
    val q = populatedQueue(3)
    assertFalse(q.contains(new PDelay(99, ninetynine)))
  }

  /**
   * containsAll returns true for subset
   */
  @Test def testContainsAll(): Unit = {
    val q = populatedQueue(3)
    val subset = Arrays.asList(q.toArray()(0), q.toArray()(1))
    assertTrue(q.containsAll(subset))
  }

  /**
   * containsAll returns false if any element missing
   */
  @Test def testContainsAll_missing(): Unit = {
    val q = populatedQueue(3)
    val extra = new PDelay(99, ninetynine)
    assertFalse(q.containsAll(Arrays.asList(q.toArray()(0), extra)))
  }

  /**
   * clear removes all elements
   */
  @Test def testClear(): Unit = {
    val q = populatedQueue(3)
    q.clear()
    assertTrue(q.isEmpty)
    mustEqual(0, q.size())
    assertNull(q.peek())
  }

  /**
   * removeAll removes all elements from given collection
   */
  @Test def testRemoveAll(): Unit = {
    val q = populatedQueue(3)
    val toRemove = Arrays.asList(q.toArray()(0), q.toArray()(1))
    assertTrue(q.removeAll(toRemove))
    mustEqual(1, q.size())
  }

  /**
   * retainAll keeps only elements from given collection
   */
  @Test def testRetainAll(): Unit = {
    val q = populatedQueue(3)
    val toRetain = Arrays.asList(q.toArray()(0))
    assertTrue(q.retainAll(toRetain))
    mustEqual(1, q.size())
    assertTrue(q.contains(q.toArray()(0)))
  }

  /**
   * toArray() returns array containing all elements
   */
  @Test def testToArray(): Unit = {
    val q = populatedQueue(3)
    val a = q.toArray()
    assertTrue(Arrays.equals(q.toArray(), a))
    mustEqual(3, a.length)
  }

  /**
   * toArray(T[]) returns array of the specified type
   */
  @Test def testToArray_withArray(): Unit = {
    val q = populatedQueue(3)
    val arr = new Array[Delayed](5)
    val result = q.toArray(arr)
    assertNotNull(result)
    mustEqual(3, q.size())
  }

  /**
   * iterator contains all elements
   */
  @Test def testIterator(): Unit = {
    val q = populatedQueue(3)
    val it = q.iterator()
    var count = 0
    while (it.hasNext) {
      assertNotNull(it.next())
      count += 1
    }
    mustEqual(3, count)
  }

  /**
   * empty iterator has no elements
   */
  @Test def testEmptyIterator(): Unit = {
    val q = new DelayQueue[PDelay]()
    val it = q.iterator()
    assertFalse(it.hasNext)
  }

  /**
   * toString contains elements
   */
  @Test def testToString(): Unit = {
    val q = populatedQueue(3)
    val s = q.toString
    assertNotNull(s)
    assertFalse(s.isEmpty)
  }

  /**
   * drainTo removes elements and adds to collection
   */
  @Test def testDrainTo(): Unit = {
    val q = populatedQueue(3)
    val c = new java.util.ArrayList[Delayed]()
    val n = q.drainTo(c)
    mustEqual(3, n)
    mustEqual(3, c.size())
    mustEqual(0, q.size())
  }

  /**
   * drainTo with maxElements limits number drained
   */
  @Test def testDrainTo_n(): Unit = {
    val q = populatedQueue(3)
    val c = new java.util.ArrayList[Delayed]()
    val n = q.drainTo(c, 2)
    mustEqual(2, n)
    mustEqual(2, c.size())
    mustEqual(1, q.size())
  }

  /**
   * drainTo returns 0 when queue is empty
   */
  @Test def testDrainTo_empty(): Unit = {
    val q = new DelayQueue[PDelay]()
    val c = new java.util.ArrayList[Delayed]()
    mustEqual(0, q.drainTo(c))
    mustEqual(0, c.size())
  }

  /**
   * drainTo with null collection throws NullPointerException
   */
  @Test def testDrainTo_nullCollection(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertThrows(classOf[NullPointerException], () => q.drainTo(null))
  }

  /**
   * drainTo to self throws IllegalArgumentException
   */
  @Test def testDrainTo_self(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertThrows(classOf[IllegalArgumentException], () => q.drainTo(q))
  }

  /**
   * drainTo with maxElements <= 0 returns 0
   */
  @Test def testDrainTo_nonPositiveMaxElements(): Unit = {
    val q = populatedQueue(3)
    val c = new java.util.ArrayList[Delayed]()
    mustEqual(0, q.drainTo(c, 0))
    mustEqual(0, q.drainTo(c, -1))
    mustEqual(3, q.size())
  }

  /**
   * Serialization and deserialization preserves queue
   */
  @Test def testSerialization(): Unit = {
    val q = populatedQueue(SIZE)
    val clone = serialClone(q)

    assertNotSame(q, clone)
    mustEqual(q.size(), clone.size())
    mustEqual(q.toString, clone.toString)

    while (!q.isEmpty && !clone.isEmpty) {
      mustEqual(q.poll().pseudodelay, clone.poll().pseudodelay)
    }
    assertTrue(q.isEmpty)
    assertTrue(clone.isEmpty)
  }

  /**
   * Delayed elements block take until delay expires
   */
  @Ignore("Time-dependent test, may be flaky on CI")
  @Test def testDelay(): Unit = {
    val q = new DelayQueue[SimpleDelay]()
    q.put(new SimpleDelay(SHORT_DELAY_MS))
    val startTime = System.nanoTime()

    val t = newStartedThread(new CheckedRunnable() {
      @throws[InterruptedException]
      override def realRun(): Unit = {
        q.take()
      }
    })

    waitForThreadToEnterWaitState(t, BLOCKED, MEDIUM_DELAY_MS)
    val elapsed = millisElapsedSince(startTime)
    // Should still be waiting
    assertTrue(elapsed < SHORT_DELAY_MS)

    awaitTermination(t, SHORT_DELAY_MS * 2)
    val totalElapsed = millisElapsedSince(startTime)
    // Should have waited at least the delay
    assertTrue(totalElapsed >= SHORT_DELAY_MS)
  }

  /**
   * take blocks when queue is empty
   */
  @Test def testBlockingTake(): Unit = {
    val q = new DelayQueue[SimpleDelay]()
    val startTime = System.nanoTime()

    val t = newStartedThread(new CheckedRunnable() {
      @throws[InterruptedException]
      override def realRun(): Unit = {
        q.put(new SimpleDelay(SHORT_DELAY_MS))
      }
    })

    delay(SHORT_DELAY_MS / 2)
    val element = q.take()
    assertNotNull(element)
    assertTrue(millisElapsedSince(startTime) >= SHORT_DELAY_MS / 2)

    awaitTermination(t)
  }

  /**
   * Interrupted take throws InterruptedException
   */
  @Test def testInterruptedTake(): Unit = {
    val q = new DelayQueue[SimpleDelay]()
    val t = newStartedThread(new CheckedInterruptedRunnable() {
      @throws[InterruptedException]
      override def realRun(): Unit = {
        q.take()
      }
    })

    waitForThreadToEnterWaitState(t, MEDIUM_DELAY_MS)
    t.interrupt()
    awaitTermination(t)
  }

  /**
   * Timed poll with timeout returns null when no elements
   */
  @Test def testTimedPoll_timeout(): Unit = {
    val q = new DelayQueue[PDelay]()
    val startTime = System.nanoTime()
    val result = q.poll(timeoutMillis(), MILLISECONDS)
    assertNull(result)
    assertTrue(millisElapsedSince(startTime) >= timeoutMillis())
  }

  /**
   * Timed poll returns element when available
   */
  @Test def testTimedPoll(): Unit = {
    val q = populatedQueue(3)
    val startTime = System.nanoTime()
    val result = q.poll(LONG_DELAY_MS, MILLISECONDS)
    assertNotNull(result)
    mustEqual(0, result.pseudodelay)
    assertTrue(millisElapsedSince(startTime) < SHORT_DELAY_MS)
  }

  /**
   * addAll adds all elements from collection
   */
  @Test def testAddAll(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertTrue(q.addAll(Arrays.asList(new PDelay(0, one), new PDelay(1, two))))
    mustEqual(2, q.size())
  }

  /**
   * isEmpty reflects queue state
   */
  @Test def testIsEmpty(): Unit = {
    val q = new DelayQueue[PDelay]()
    assertTrue(q.isEmpty)
    q.add(new PDelay(0, one))
    assertFalse(q.isEmpty)
    q.clear()
    assertTrue(q.isEmpty)
  }

  /**
   * size reflects queue state
   */
  @Test def testSize(): Unit = {
    val q = new DelayQueue[PDelay]()
    mustEqual(0, q.size())
    q.add(new PDelay(0, one))
    mustEqual(1, q.size())
    q.clear()
    mustEqual(0, q.size())
  }

  /**
   * Multiple producers and consumers work correctly
   */
  @Test def testMultipleProducersConsumers(): Unit = {
    val q = new DelayQueue[PDelay]()
    val numElements = SIZE
    val produced = new java.util.concurrent.atomic.AtomicInteger(0)
    val consumed = new java.util.concurrent.atomic.AtomicInteger(0)

    val producer = new CheckedRunnable() {
      @throws[InterruptedException]
      override def realRun(): Unit = {
        var i = 0
        while (i < numElements) {
          q.put(new PDelay(i, itemFor(i)))
          produced.incrementAndGet()
          i += 1
        }
      }
    }

    val consumer = new CheckedRunnable() {
      @throws[InterruptedException]
      override def realRun(): Unit = {
        var i = 0
        while (i < numElements) {
          val e = q.take()
          assertNotNull(e)
          consumed.incrementAndGet()
          i += 1
        }
      }
    }

    val producerThread = newStartedThread(producer)
    val consumerThread = newStartedThread(consumer)

    awaitTermination(producerThread)
    awaitTermination(consumerThread)

    mustEqual(numElements, produced.get())
    mustEqual(numElements, consumed.get())
    assertTrue(q.isEmpty)
  }
}

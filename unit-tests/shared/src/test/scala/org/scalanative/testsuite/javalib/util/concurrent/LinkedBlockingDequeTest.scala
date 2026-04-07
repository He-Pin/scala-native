/*
 * Written by Doug Lea and Martin Buchholz with assistance from
 * members of JCP JSR-166 Expert Group and released to the public
 * domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package org.scalanative.testsuite.javalib.util.concurrent

import java.util._
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent._

import org.junit.Assert._
import org.junit.{Ignore, Test}

import JSR166Test._

class LinkedBlockingDequeUnboundedTest extends BlockingQueueTest {
  override protected def emptyCollection(): BlockingQueue[Any] =
    new LinkedBlockingDeque[Any]
}
class LinkedBlockingDequeBoundedTest extends BlockingQueueTest {
  override protected def emptyCollection(): BlockingQueue[Any] =
    new LinkedBlockingDeque[Any](SIZE)
}

object LinkedBlockingDequeTest {

  /** Returns a new deque of given size containing consecutive Items 0 ... n - 1.
   */
  private def populatedDeque(n: Int): LinkedBlockingDeque[Item] = {
    val q = new LinkedBlockingDeque[Item](n)
    assertTrue(q.isEmpty)
    var i = 0
    while (i < n) {
      mustOffer(q, i)
      i += 1
    }
    assertFalse(q.isEmpty)
    mustEqual(0, q.remainingCapacity())
    mustEqual(n, q.size())
    mustEqual(0, q.peekFirst())
    mustEqual((n - 1), q.peekLast())
    q
  }
}

class LinkedBlockingDequeTest extends JSR166Test {

  /** isEmpty is true before add, false after
   */
  @Test def testEmpty(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    assertTrue(q.isEmpty)
    q.add(one)
    assertFalse(q.isEmpty)
    q.add(two)
    q.removeFirst()
    q.removeFirst()
    assertTrue(q.isEmpty)
  }

  /** size changes when elements added and removed
   */
  @Test def testSize(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(SIZE - i, q.size())
      q.removeFirst()
      i += 1
    }
    i = 0
    while (i < SIZE) {
      mustEqual(i, q.size())
      mustAdd(q, one)
      i += 1
    }
  }

  /** offerFirst(null) throws NullPointerException
   */
  @Test def testOfferFirstNull(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    try {
      q.offerFirst(null)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** offerLast(null) throws NullPointerException
   */
  @Test def testOfferLastNull(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    try {
      q.offerLast(null)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** OfferFirst succeeds
   */
  @Test def testOfferFirst(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    assertTrue(q.offerFirst(zero))
    assertTrue(q.offerFirst(two))
  }

  /** OfferLast succeeds
   */
  @Test def testOfferLast(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    assertTrue(q.offerLast(zero))
    assertTrue(q.offerLast(one))
  }

  /** pollFirst succeeds unless empty
   */
  @Test def testPollFirst(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.pollFirst())
      i += 1
    }
    assertNull(q.pollFirst())
  }

  /** pollLast succeeds unless empty
   */
  @Test def testPollLast(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = SIZE - 1
    while (i >= 0) {
      mustEqual(i, q.pollLast())
      i -= 1
    }
    assertNull(q.pollLast())
  }

  /** peekFirst returns next element, or null if empty
   */
  @Test def testPeekFirst(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.peekFirst())
      mustEqual(i, q.pollFirst())
      assertTrue(q.peekFirst() == null || !q.peekFirst().equals(i))
      i += 1
    }
    assertNull(q.peekFirst())
  }

  /** peek returns next element, or null if empty
   */
  @Test def testPeek(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.peek())
      mustEqual(i, q.pollFirst())
      assertTrue(q.peek() == null || !q.peek().equals(i))
      i += 1
    }
    assertNull(q.peek())
  }

  /** peekLast returns next element, or null if empty
   */
  @Test def testPeekLast(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = SIZE - 1
    while (i >= 0) {
      mustEqual(i, q.peekLast())
      mustEqual(i, q.pollLast())
      assertTrue(q.peekLast() == null || !q.peekLast().equals(i))
      i -= 1
    }
    assertNull(q.peekLast())
  }

  /** getFirst() returns first element, or throws NSEE if empty
   */
  @Test def testFirstElement(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.getFirst())
      mustEqual(i, q.pollFirst())
      i += 1
    }
    try {
      q.getFirst()
      shouldThrow()
    } catch {
      case success: NoSuchElementException =>
    }
    assertNull(q.peekFirst())
  }

  /** getLast() returns last element, or throws NSEE if empty
   */
  @Test def testLastElement(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = SIZE - 1
    while (i >= 0) {
      mustEqual(i, q.getLast())
      mustEqual(i, q.pollLast())
      i -= 1
    }
    try {
      q.getLast()
      shouldThrow()
    } catch {
      case success: NoSuchElementException =>
    }
    assertNull(q.peekLast())
  }

  /** removeFirst() removes first element, or throws NSEE if empty
   */
  @Test def testRemoveFirst(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.removeFirst())
      i += 1
    }
    try {
      q.removeFirst()
      shouldThrow()
    } catch {
      case success: NoSuchElementException =>
    }
    assertNull(q.peekFirst())
  }

  /** removeLast() removes last element, or throws NSEE if empty
   */
  @Test def testRemoveLast(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = SIZE - 1
    while (i >= 0) {
      mustEqual(i, q.removeLast())
      i -= 1
    }
    try {
      q.removeLast()
      shouldThrow()
    } catch {
      case success: NoSuchElementException =>
    }
    assertNull(q.peekLast())
  }

  /** remove removes next element, or throws NSEE if empty
   */
  @Test def testRemove(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.remove())
      i += 1
    }
    try {
      q.remove()
      shouldThrow()
    } catch {
      case success: NoSuchElementException =>
    }
  }

  /** removeFirstOccurrence(x) removes x and returns true if present
   */
  @Test def testRemoveFirstOccurrence(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 1
    while (i < SIZE) {
      assertTrue(q.removeFirstOccurrence(itemFor(i)))
      i += 2
    }
    i = 0
    while (i < SIZE) {
      assertTrue(q.removeFirstOccurrence(itemFor(i)))
      assertFalse(q.removeFirstOccurrence(itemFor(i + 1)))
      i += 2
    }
    assertTrue(q.isEmpty)
  }

  /** removeLastOccurrence(x) removes x and returns true if present
   */
  @Test def testRemoveLastOccurrence(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 1
    while (i < SIZE) {
      assertTrue(q.removeLastOccurrence(itemFor(i)))
      i += 2
    }
    i = 0
    while (i < SIZE) {
      assertTrue(q.removeLastOccurrence(itemFor(i)))
      assertFalse(q.removeLastOccurrence(itemFor(i + 1)))
      i += 2
    }
    assertTrue(q.isEmpty)
  }

  /** peekFirst returns element inserted with addFirst
   */
  @Test def testAddFirst(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(3)
    q.pollLast()
    q.addFirst(four)
    assertSame(four, q.peekFirst())
  }

  /** peekLast returns element inserted with addLast
   */
  @Test def testAddLast(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(3)
    q.pollLast()
    q.addLast(four)
    assertSame(four, q.peekLast())
  }

  /** A new deque has the indicated capacity, or Integer.MAX_VALUE if none given
   */
  @Test def testConstructor1(): Unit = {
    mustEqual(SIZE, new LinkedBlockingDeque[Item](SIZE).remainingCapacity())
    mustEqual(Integer.MAX_VALUE, new LinkedBlockingDeque[Item]().remainingCapacity())
  }

  /** Constructor throws IllegalArgumentException if capacity argument nonpositive
   */
  @Test def testConstructor2(): Unit = {
    try {
      new LinkedBlockingDeque[Item](0)
      shouldThrow()
    } catch {
      case success: IllegalArgumentException =>
    }
  }

  /** Initializing from null Collection throws NullPointerException
   */
  @Test def testConstructor3(): Unit = {
    try {
      new LinkedBlockingDeque[Item](null)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** Initializing from Collection of null elements throws NullPointerException
   */
  @Test def testConstructor4(): Unit = {
    val elements: Collection[Item] = Arrays.asList(new Array[Item](SIZE): _*)
    try {
      new LinkedBlockingDeque[Item](elements)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** Initializing from Collection with some null elements throws NullPointerException
   */
  @Test def testConstructor5(): Unit = {
    val items = new Array[Item](2)
    items(0) = zero
    val elements: Collection[Item] = Arrays.asList(items: _*)
    try {
      new LinkedBlockingDeque[Item](elements)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** Deque contains all elements of collection used to initialize
   */
  @Test def testConstructor6(): Unit = {
    val items = defaultItems
    val q = new LinkedBlockingDeque[Item](Arrays.asList(items: _*))
    var i = 0
    while (i < SIZE) {
      mustEqual(items(i), q.poll())
      i += 1
    }
  }

  /** Deque transitions from empty to full when elements added
   */
  @Test def testEmptyFull(): Unit = {
    val q = new LinkedBlockingDeque[Item](2)
    assertTrue(q.isEmpty)
    mustEqual(2, q.remainingCapacity())
    q.add(one)
    assertFalse(q.isEmpty)
    q.add(two)
    assertFalse(q.isEmpty)
    mustEqual(0, q.remainingCapacity())
    assertFalse(q.offer(three))
  }

  /** remainingCapacity decreases on add, increases on remove
   */
  @Test def testRemainingCapacity(): Unit = {
    val q: BlockingQueue[Item] = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.remainingCapacity())
      mustEqual(SIZE, q.size() + q.remainingCapacity())
      mustEqual(i, q.remove())
      i += 1
    }
    i = 0
    while (i < SIZE) {
      mustEqual(SIZE - i, q.remainingCapacity())
      mustEqual(SIZE, q.size() + q.remainingCapacity())
      mustAdd(q, i)
      i += 1
    }
  }

  /** push(null) throws NPE
   */
  @Test def testPushNull(): Unit = {
    val q = new LinkedBlockingDeque[Item](1)
    try {
      q.push(null)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** push succeeds if not full; throws IllegalStateException if full
   */
  @Test def testPush(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    var i = 0
    while (i < SIZE) {
      val x = itemFor(i)
      q.push(x)
      mustEqual(x, q.peek())
      i += 1
    }
    mustEqual(0, q.remainingCapacity())
    try {
      q.push(itemFor(SIZE))
      shouldThrow()
    } catch {
      case success: IllegalStateException =>
    }
  }

  /** peekFirst returns element inserted with push
   */
  @Test def testPushWithPeek(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(3)
    q.pollLast()
    q.push(four)
    assertSame(four, q.peekFirst())
  }

  /** pop removes next element, or throws NSEE if empty
   */
  @Test def testPop(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.pop())
      i += 1
    }
    try {
      q.pop()
      shouldThrow()
    } catch {
      case success: NoSuchElementException =>
    }
  }

  /** Offer succeeds if not full; fails if full
   */
  @Test def testOffer(): Unit = {
    val q = new LinkedBlockingDeque[Item](1)
    assertTrue(q.offer(zero))
    assertFalse(q.offer(one))
  }

  /** add succeeds if not full; throws IllegalStateException if full
   */
  @Test def testAdd(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    var i = 0
    while (i < SIZE) {
      mustAdd(q, i)
      i += 1
    }
    mustEqual(0, q.remainingCapacity())
    try {
      q.add(itemFor(SIZE))
      shouldThrow()
    } catch {
      case success: IllegalStateException =>
    }
  }

  /** addAll(this) throws IllegalArgumentException
   */
  @Test def testAddAllSelf(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    try {
      q.addAll(q)
      shouldThrow()
    } catch {
      case success: IllegalArgumentException =>
    }
  }

  /** addAll of a collection with any null elements throws NPE after possibly adding some elements
   */
  @Test def testAddAll3(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    val items = new Array[Item](2)
    items(0) = zero
    val elements: Collection[Item] = Arrays.asList(items: _*)
    try {
      q.addAll(elements)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** addAll throws IllegalStateException if not enough room
   */
  @Test def testAddAll4(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE - 1)
    val items = defaultItems
    val elements: Collection[Item] = Arrays.asList(items: _*)
    try {
      q.addAll(elements)
      shouldThrow()
    } catch {
      case success: IllegalStateException =>
    }
  }

  /** Deque contains all elements, in traversal order, of successful addAll
   */
  @Test def testAddAll5(): Unit = {
    val empty = new Array[Item](0)
    val items = defaultItems
    val q = new LinkedBlockingDeque[Item](SIZE)
    assertFalse(q.addAll(Arrays.asList(empty: _*)))
    assertTrue(q.addAll(Arrays.asList(items: _*)))
    var i = 0
    while (i < SIZE) {
      mustEqual(items(i), q.poll())
      i += 1
    }
  }

  /** all elements successfully put are contained
   */
  @Test def testPut(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    var i = 0
    while (i < SIZE) {
      val x = itemFor(i)
      q.put(x)
      mustContain(q, x)
      i += 1
    }
    mustEqual(0, q.remainingCapacity())
  }

  /** put blocks interruptibly if full
   */
  @Test def testBlockingPut(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          q.put(itemFor(i))
          i += 1
        }
        mustEqual(SIZE, q.size())
        mustEqual(0, q.remainingCapacity())

        Thread.currentThread().interrupt()
        try {
          q.put(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.put(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
    mustEqual(SIZE, q.size())
    mustEqual(0, q.remainingCapacity())
  }

  /** put blocks interruptibly waiting for take when full
   */
  @Test def testPutWithTake(): Unit = {
    val capacity = 2
    val q = new LinkedBlockingDeque[Item](capacity)
    val pleaseTake = new CountDownLatch(1)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < capacity) {
          q.put(itemFor(i))
          i += 1
        }
        pleaseTake.countDown()
        q.put(eightysix)

        Thread.currentThread().interrupt()
        try {
          q.put(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.put(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseTake)
    mustEqual(0, q.remainingCapacity())
    mustEqual(0, q.take())

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
    mustEqual(0, q.remainingCapacity())
  }

  /** timed offer times out if full and elements not taken
   */
  @Test def testTimedOffer(): Unit = {
    val q = new LinkedBlockingDeque[Item](2)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        q.put(zero)
        q.put(one)
        val startTime = System.nanoTime()

        assertFalse(q.offer(two, timeoutMillis(), MILLISECONDS))
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis())

        Thread.currentThread().interrupt()
        try {
          q.offer(three, randomTimeout(), randomTimeUnit())
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.offer(four, LONGER_DELAY_MS, MILLISECONDS)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.TIMED_WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** take retrieves elements in FIFO order
   */
  @Test def testTake(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.take())
      i += 1
    }
  }

  /** take removes existing elements until empty, then blocks interruptibly
   */
  @Test def testBlockingTake(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          mustEqual(i, q.take())
          i += 1
        }

        Thread.currentThread().interrupt()
        try {
          q.take()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.take()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** poll succeeds unless empty
   */
  @Test def testPoll(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.poll())
      i += 1
    }
    assertNull(q.poll())
  }

  /** timed poll with zero timeout succeeds when non-empty, else times out
   */
  @Test def testTimedPoll0(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.poll(0, MILLISECONDS))
      i += 1
    }
    assertNull(q.poll(0, MILLISECONDS))
  }

  /** timed poll with nonzero timeout succeeds when non-empty, else times out
   */
  @Test def testTimedPoll(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      val startTime = System.nanoTime()
      mustEqual(i, q.poll(LONG_DELAY_MS, MILLISECONDS))
      assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS)
      i += 1
    }
    val startTime = System.nanoTime()
    assertNull(q.poll(timeoutMillis(), MILLISECONDS))
    assertTrue(millisElapsedSince(startTime) >= timeoutMillis())
    checkEmpty(q)
  }

  /** Interrupted timed poll throws InterruptedException instead of returning timeout status
   */
  @Test def testInterruptedTimedPoll(): Unit = {
    val q: BlockingQueue[Item] = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          mustEqual(i, q.poll(LONG_DELAY_MS, MILLISECONDS))
          i += 1
        }

        Thread.currentThread().interrupt()
        try {
          q.poll(randomTimeout(), randomTimeUnit())
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.poll(LONGER_DELAY_MS, MILLISECONDS)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.TIMED_WAITING)
    t.interrupt()
    awaitTermination(t)
    checkEmpty(q)
  }

  /** putFirst(null) throws NPE
   */
  @Test def testPutFirstNull(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    try {
      q.putFirst(null)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** all elements successfully putFirst are contained
   */
  @Test def testPutFirst(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    var i = 0
    while (i < SIZE) {
      val x = itemFor(i)
      q.putFirst(x)
      mustContain(q, x)
      i += 1
    }
    mustEqual(0, q.remainingCapacity())
  }

  /** putFirst blocks interruptibly if full
   */
  @Test def testBlockingPutFirst(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          q.putFirst(itemFor(i))
          i += 1
        }
        mustEqual(SIZE, q.size())
        mustEqual(0, q.remainingCapacity())

        Thread.currentThread().interrupt()
        try {
          q.putFirst(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.putFirst(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
    mustEqual(SIZE, q.size())
    mustEqual(0, q.remainingCapacity())
  }

  /** putFirst blocks interruptibly waiting for take when full
   */
  @Test def testPutFirstWithTake(): Unit = {
    val capacity = 2
    val q = new LinkedBlockingDeque[Item](capacity)
    val pleaseTake = new CountDownLatch(1)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < capacity) {
          q.putFirst(itemFor(i))
          i += 1
        }
        pleaseTake.countDown()
        q.putFirst(eightysix)

        pleaseInterrupt.countDown()
        try {
          q.putFirst(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseTake)
    mustEqual(0, q.remainingCapacity())
    mustEqual(capacity - 1, q.take())

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
    mustEqual(0, q.remainingCapacity())
  }

  /** timed offerFirst times out if full and elements not taken
   */
  @Test def testTimedOfferFirst(): Unit = {
    val q = new LinkedBlockingDeque[Item](2)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        q.putFirst(zero)
        q.putFirst(one)
        val startTime = System.nanoTime()

        assertFalse(q.offerFirst(two, timeoutMillis(), MILLISECONDS))
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis())

        Thread.currentThread().interrupt()
        try {
          q.offerFirst(three, randomTimeout(), randomTimeUnit())
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.offerFirst(four, LONGER_DELAY_MS, MILLISECONDS)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.TIMED_WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** takeFirst retrieves elements in FIFO order
   */
  @Test def testTakeFirst(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.takeFirst())
      i += 1
    }
  }

  /** takeFirst() blocks interruptibly when empty
   */
  @Test def testTakeFirstFromEmptyBlocksInterruptibly(): Unit = {
    val q: BlockingDeque[Item] = new LinkedBlockingDeque[Item]
    val threadStarted = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        threadStarted.countDown()
        try {
          q.takeFirst()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(threadStarted)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** takeFirst() throws InterruptedException immediately if interrupted before waiting
   */
  @Test def testTakeFirstFromEmptyAfterInterrupt(): Unit = {
    val q: BlockingDeque[Item] = new LinkedBlockingDeque[Item]
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        Thread.currentThread().interrupt()
        try {
          q.takeFirst()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    awaitTermination(t)
  }

  /** takeLast() blocks interruptibly when empty
   */
  @Test def testTakeLastFromEmptyBlocksInterruptibly(): Unit = {
    val q: BlockingDeque[Item] = new LinkedBlockingDeque[Item]
    val threadStarted = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        threadStarted.countDown()
        try {
          q.takeLast()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(threadStarted)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** takeLast() throws InterruptedException immediately if interrupted before waiting
   */
  @Test def testTakeLastFromEmptyAfterInterrupt(): Unit = {
    val q: BlockingDeque[Item] = new LinkedBlockingDeque[Item]
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        Thread.currentThread().interrupt()
        try {
          q.takeLast()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    awaitTermination(t)
  }

  /** takeFirst removes existing elements until empty, then blocks interruptibly
   */
  @Test def testBlockingTakeFirst(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          mustEqual(i, q.takeFirst())
          i += 1
        }

        Thread.currentThread().interrupt()
        try {
          q.takeFirst()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.takeFirst()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** timed pollFirst with zero timeout succeeds when non-empty, else times out
   */
  @Test def testTimedPollFirst0(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.pollFirst(0, MILLISECONDS))
      i += 1
    }
    assertNull(q.pollFirst(0, MILLISECONDS))
  }

  /** timed pollFirst with nonzero timeout succeeds when non-empty, else times out
   */
  @Test def testTimedPollFirst(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      val startTime = System.nanoTime()
      mustEqual(i, q.pollFirst(LONG_DELAY_MS, MILLISECONDS))
      assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS)
      i += 1
    }
    val startTime = System.nanoTime()
    assertNull(q.pollFirst(timeoutMillis(), MILLISECONDS))
    assertTrue(millisElapsedSince(startTime) >= timeoutMillis())
    checkEmpty(q)
  }

  /** Interrupted timed pollFirst throws InterruptedException instead of returning timeout status
   */
  @Test def testInterruptedTimedPollFirst(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          mustEqual(i, q.pollFirst(LONG_DELAY_MS, MILLISECONDS))
          i += 1
        }

        Thread.currentThread().interrupt()
        try {
          q.pollFirst(randomTimeout(), randomTimeUnit())
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.pollFirst(LONGER_DELAY_MS, MILLISECONDS)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.TIMED_WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** timed pollFirst before a delayed offerFirst fails; after offerFirst succeeds; on interruption throws
   */
  @Test def testTimedPollFirstWithOfferFirst(): Unit = {
    val q = new LinkedBlockingDeque[Item](2)
    val barrier = new CheckedBarrier(2)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        val startTime = System.nanoTime()
        assertNull(q.pollFirst(timeoutMillis(), MILLISECONDS))
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis())

        barrier.await()

        assertSame(zero, q.pollFirst(LONG_DELAY_MS, MILLISECONDS))

        Thread.currentThread().interrupt()
        try {
          q.pollFirst(randomTimeout(), randomTimeUnit())
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }

        barrier.await()
        try {
          q.pollFirst(LONG_DELAY_MS, MILLISECONDS)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS)
      }
    })

    barrier.await()
    val startTime = System.nanoTime()
    assertTrue(q.offerFirst(zero, LONG_DELAY_MS, MILLISECONDS))
    assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS)
    barrier.await()
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.TIMED_WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** putLast(null) throws NPE
   */
  @Test def testPutLastNull(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    try {
      q.putLast(null)
      shouldThrow()
    } catch {
      case success: NullPointerException =>
    }
  }

  /** all elements successfully putLast are contained
   */
  @Test def testPutLast(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    var i = 0
    while (i < SIZE) {
      val x = itemFor(i)
      q.putLast(x)
      mustContain(q, x)
      i += 1
    }
    mustEqual(0, q.remainingCapacity())
  }

  /** putLast blocks interruptibly if full
   */
  @Test def testBlockingPutLast(): Unit = {
    val q = new LinkedBlockingDeque[Item](SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          q.putLast(itemFor(i))
          i += 1
        }
        mustEqual(SIZE, q.size())
        mustEqual(0, q.remainingCapacity())

        Thread.currentThread().interrupt()
        try {
          q.putLast(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.putLast(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
    mustEqual(SIZE, q.size())
    mustEqual(0, q.remainingCapacity())
  }

  /** putLast blocks interruptibly waiting for take when full
   */
  @Test def testPutLastWithTake(): Unit = {
    val capacity = 2
    val q = new LinkedBlockingDeque[Item](capacity)
    val pleaseTake = new CountDownLatch(1)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < capacity) {
          q.putLast(itemFor(i))
          i += 1
        }
        pleaseTake.countDown()
        q.putLast(eightysix)

        Thread.currentThread().interrupt()
        try {
          q.putLast(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.putLast(ninetynine)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseTake)
    mustEqual(0, q.remainingCapacity())
    mustEqual(0, q.take())

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
    mustEqual(0, q.remainingCapacity())
  }

  /** timed offerLast times out if full and elements not taken
   */
  @Test def testTimedOfferLast(): Unit = {
    val q = new LinkedBlockingDeque[Item](2)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        q.putLast(zero)
        q.putLast(one)
        val startTime = System.nanoTime()

        assertFalse(q.offerLast(two, timeoutMillis(), MILLISECONDS))
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis())

        Thread.currentThread().interrupt()
        try {
          q.offerLast(three, randomTimeout(), randomTimeUnit())
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }

        pleaseInterrupt.countDown()
        try {
          q.offerLast(four, LONGER_DELAY_MS, MILLISECONDS)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.TIMED_WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** takeLast retrieves elements in FIFO order
   */
  @Test def testTakeLast(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(SIZE - i - 1, q.takeLast())
      i += 1
    }
  }

  /** takeLast removes existing elements until empty, then blocks interruptibly
   */
  @Test def testBlockingTakeLast(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          mustEqual(SIZE - i - 1, q.takeLast())
          i += 1
        }

        Thread.currentThread().interrupt()
        try {
          q.takeLast()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.takeLast()
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** timed pollLast with zero timeout succeeds when non-empty, else times out
   */
  @Test def testTimedPollLast0(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(SIZE - i - 1, q.pollLast(0, MILLISECONDS))
      i += 1
    }
    assertNull(q.pollLast(0, MILLISECONDS))
  }

  /** timed pollLast with nonzero timeout succeeds when non-empty, else times out
   */
  @Test def testTimedPollLast(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      val startTime = System.nanoTime()
      mustEqual(SIZE - i - 1, q.pollLast(LONG_DELAY_MS, MILLISECONDS))
      assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS)
      i += 1
    }
    val startTime = System.nanoTime()
    assertNull(q.pollLast(timeoutMillis(), MILLISECONDS))
    assertTrue(millisElapsedSince(startTime) >= timeoutMillis())
    checkEmpty(q)
  }

  /** Interrupted timed pollLast throws InterruptedException instead of returning timeout status
   */
  @Test def testInterruptedTimedPollLast(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val pleaseInterrupt = new CountDownLatch(1)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        var i = 0
        while (i < SIZE) {
          mustEqual(SIZE - i - 1, q.pollLast(LONG_DELAY_MS, MILLISECONDS))
          i += 1
        }

        Thread.currentThread().interrupt()
        try {
          q.pollLast(randomTimeout(), randomTimeUnit())
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        pleaseInterrupt.countDown()
        try {
          q.pollLast(LONGER_DELAY_MS, MILLISECONDS)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())
      }
    })

    await(pleaseInterrupt)
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.TIMED_WAITING)
    t.interrupt()
    awaitTermination(t)
    checkEmpty(q)
  }

  /** timed poll before a delayed offerLast fails; after offerLast succeeds; on interruption throws
   */
  @Test def testTimedPollWithOfferLast(): Unit = {
    val q = new LinkedBlockingDeque[Item](2)
    val barrier = new CheckedBarrier(2)
    val t = newStartedThread(new CheckedRunnable() {
      override def realRun(): Unit = {
        val startTime = System.nanoTime()
        assertNull(q.poll(timeoutMillis(), MILLISECONDS))
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis())

        barrier.await()

        assertSame(zero, q.poll(LONG_DELAY_MS, MILLISECONDS))

        Thread.currentThread().interrupt()
        try {
          q.poll(randomTimeout(), randomTimeUnit())
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        barrier.await()
        try {
          q.poll(LONG_DELAY_MS, MILLISECONDS)
          shouldThrow()
        } catch {
          case success: InterruptedException =>
        }
        assertFalse(Thread.interrupted())

        assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS)
      }
    })

    barrier.await()
    val startTime = System.nanoTime()
    assertTrue(q.offerLast(zero, LONG_DELAY_MS, MILLISECONDS))
    assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS)

    barrier.await()
    if (randomBoolean()) assertThreadBlocks(t, Thread.State.TIMED_WAITING)
    t.interrupt()
    awaitTermination(t)
  }

  /** element returns next element, or throws NSEE if empty
   */
  @Test def testElement(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(i, q.element())
      q.poll()
      i += 1
    }
    try {
      q.element()
      shouldThrow()
    } catch {
      case success: NoSuchElementException =>
    }
  }

  /** contains(x) reports true when elements added but not yet removed
   */
  @Test def testContains(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      mustContain(q, i)
      q.poll()
      mustNotContain(q, i)
      i += 1
    }
  }

  /** clear removes all elements
   */
  @Test def testClear(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    q.clear()
    assertTrue(q.isEmpty)
    mustEqual(0, q.size())
    mustEqual(SIZE, q.remainingCapacity())
    q.add(one)
    assertFalse(q.isEmpty)
    mustContain(q, one)
    q.clear()
    assertTrue(q.isEmpty)
  }

  /** containsAll(c) is true when c contains a subset of elements
   */
  @Test def testContainsAll(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val p = new LinkedBlockingDeque[Item](SIZE)
    var i = 0
    while (i < SIZE) {
      assertTrue(q.containsAll(p))
      assertFalse(p.containsAll(q))
      mustAdd(p, i)
      i += 1
    }
    assertTrue(p.containsAll(q))
  }

  /** retainAll(c) retains only those elements of c and reports true if changed
   */
  @Test def testRetainAll(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val p = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    while (i < SIZE) {
      val changed = q.retainAll(p)
      if (i == 0)
        assertFalse(changed)
      else
        assertTrue(changed)

      assertTrue(q.containsAll(p))
      mustEqual(SIZE - i, q.size())
      p.remove()
      i += 1
    }
  }

  /** removeAll(c) removes only those elements of c and reports true if changed
   */
  @Test def testRemoveAll(): Unit = {
    var i = 1
    while (i < SIZE) {
      val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
      val p = LinkedBlockingDequeTest.populatedDeque(i)
      assertTrue(q.removeAll(p))
      mustEqual(SIZE - i, q.size())
      var j = 0
      while (j < i) {
        mustNotContain(q, p.remove())
        j += 1
      }
      i += 1
    }
  }

  /** toArray contains all elements in FIFO order
   */
  @Test def testToArray(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val a = q.toArray()
    assertSame(classOf[Array[AnyRef]], a.getClass)
    val it = a.iterator()
    while (it.hasNext()) {
      val o = it.next()
      assertSame(o, q.poll())
    }
    assertTrue(q.isEmpty)
  }

  /** toArray(a) contains all elements in FIFO order
   */
  @Test def testToArray2(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val items = new Array[Item](SIZE)
    val array = q.toArray(items)
    assertSame(items, array)
    var i = 0
    while (i < items.length) {
      val o = items(i)
      assertSame(o, q.remove())
      i += 1
    }
    assertTrue(q.isEmpty)
  }

  /** toArray(incompatible array type) throws ArrayStoreException
   */
  @Test def testToArray_incompatibleArrayType(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    try {
      q.toArray(new Array[String](10))
      shouldThrow()
    } catch {
      case success: ArrayStoreException =>
    }
  }

  /** iterator iterates through all elements
   */
  @Test def testIterator(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val it = q.iterator()
    var i = 0
    while (it.hasNext()) {
      mustContain(q, it.next())
      i += 1
    }
    mustEqual(i, SIZE)
    assertIteratorExhausted(it)

    val it2 = q.iterator()
    i = 0
    while (it2.hasNext()) {
      mustEqual(it2.next(), q.take())
      i += 1
    }
    mustEqual(i, SIZE)
    assertIteratorExhausted(it2)
  }

  /** iterator of empty collection has no elements
   */
  @Test def testEmptyIterator(): Unit = {
    val c: Deque[Item] = new LinkedBlockingDeque[Item]
    assertIteratorExhausted(c.iterator())
    assertIteratorExhausted(c.descendingIterator())
  }

  /** iterator.remove removes current element
   */
  @Test def testIteratorRemove(): Unit = {
    val q = new LinkedBlockingDeque[Item](3)
    q.add(two)
    q.add(one)
    q.add(three)

    val it = q.iterator()
    it.next()
    it.remove()

    val it2 = q.iterator()
    assertSame(it2.next(), one)
    assertSame(it2.next(), three)
    assertFalse(it2.hasNext())
  }

  /** iterator ordering is FIFO
   */
  @Test def testIteratorOrdering(): Unit = {
    val q = new LinkedBlockingDeque[Item](3)
    q.add(one)
    q.add(two)
    q.add(three)
    mustEqual(0, q.remainingCapacity())
    var k = 0
    val it = q.iterator()
    while (it.hasNext()) {
      k += 1
      mustEqual(k, it.next())
    }
    mustEqual(3, k)
  }

  /** Modifications do not cause iterators to fail
   */
  @Test def testWeaklyConsistentIteration(): Unit = {
    val q = new LinkedBlockingDeque[Item](3)
    q.add(one)
    q.add(two)
    q.add(three)
    val it = q.iterator()
    while (it.hasNext()) {
      q.remove()
      it.next()
    }
    mustEqual(0, q.size())
  }

  /** Descending iterator iterates through all elements
   */
  @Test def testDescendingIterator(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    var i = 0
    val it = q.descendingIterator()
    while (it.hasNext()) {
      mustContain(q, it.next())
      i += 1
    }
    mustEqual(i, SIZE)
    assertFalse(it.hasNext())
    try {
      it.next()
      shouldThrow()
    } catch {
      case success: NoSuchElementException =>
    }
  }

  /** Descending iterator ordering is reverse FIFO
   */
  @Test def testDescendingIteratorOrdering(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    var iters = 0
    while (iters < 100) {
      mustAdd(q, three)
      mustAdd(q, two)
      mustAdd(q, one)

      var k = 0
      val it = q.descendingIterator()
      while (it.hasNext()) {
        k += 1
        mustEqual(k, it.next())
      }

      mustEqual(3, k)
      q.remove()
      q.remove()
      q.remove()
      iters += 1
    }
  }

  /** descendingIterator.remove removes current element
   */
  @Test def testDescendingIteratorRemove(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    var iters = 0
    while (iters < 100) {
      mustAdd(q, three)
      mustAdd(q, two)
      mustAdd(q, one)
      var it = q.descendingIterator()
      mustEqual(it.next(), itemFor(1))
      it.remove()
      mustEqual(it.next(), itemFor(2))
      it = q.descendingIterator()
      mustEqual(it.next(), itemFor(2))
      mustEqual(it.next(), itemFor(3))
      it.remove()
      assertFalse(it.hasNext())
      q.remove()
      iters += 1
    }
  }

  /** toString contains toStrings of elements
   */
  @Test def testToString(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val s = q.toString()
    var i = 0
    while (i < SIZE) {
      assertTrue(s.contains(String.valueOf(i)))
      i += 1
    }
  }

  /** offer transfers elements across Executor tasks
   */
  @Test def testOfferInExecutor(): Unit = {
    val q = new LinkedBlockingDeque[Item](2)
    q.add(one)
    q.add(two)
    val threadsStarted = new CheckedBarrier(2)
    usingPoolCleaner(Executors.newFixedThreadPool(2)) { executor =>
      executor.execute(new CheckedRunnable() {
        override def realRun(): Unit = {
          assertFalse(q.offer(three))
          threadsStarted.await()
          assertTrue(q.offer(three, LONG_DELAY_MS, MILLISECONDS))
          mustEqual(0, q.remainingCapacity())
        }
      })

      executor.execute(new CheckedRunnable() {
        override def realRun(): Unit = {
          threadsStarted.await()
          assertSame(one, q.take())
        }
      })
    }
  }

  /** timed poll retrieves elements across Executor threads
   */
  @Test def testPollInExecutor(): Unit = {
    val q = new LinkedBlockingDeque[Item](2)
    val threadsStarted = new CheckedBarrier(2)
    usingPoolCleaner(Executors.newFixedThreadPool(2)) { executor =>
      executor.execute(new CheckedRunnable() {
        override def realRun(): Unit = {
          assertNull(q.poll())
          threadsStarted.await()
          assertSame(one, q.poll(LONG_DELAY_MS, MILLISECONDS))
          checkEmpty(q)
        }
      })

      executor.execute(new CheckedRunnable() {
        override def realRun(): Unit = {
          threadsStarted.await()
          q.put(one)
        }
      })
    }
  }

  /** A deserialized/reserialized deque has same elements in same order
   */
  @Ignore("No ObjectInputStream in Scala Native")
  @Test def testSerialization(): Unit = {
    // Serialization not supported in Scala Native
  }

  /** drainTo(c) empties deque into another collection c
   */
  @Test def testDrainTo(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val l = new ArrayList[Item]
    q.drainTo(l)
    mustEqual(0, q.size())
    mustEqual(SIZE, l.size())
    var i = 0
    while (i < SIZE) {
      mustEqual(l.get(i), i)
      i += 1
    }
    q.add(zero)
    q.add(one)
    assertFalse(q.isEmpty)
    mustContain(q, zero)
    mustContain(q, one)
    l.clear()
    q.drainTo(l)
    mustEqual(0, q.size())
    mustEqual(2, l.size())
    i = 0
    while (i < 2) {
      mustEqual(l.get(i), i)
      i += 1
    }
  }

  /** drainTo empties full deque, unblocking a waiting put.
   */
  @Test def testDrainToWithActivePut(): Unit = {
    val q = LinkedBlockingDequeTest.populatedDeque(SIZE)
    val t = new Thread(new CheckedRunnable() {
      override def realRun(): Unit = {
        q.put(new Item(SIZE + 1))
      }
    })

    t.start()
    val l = new ArrayList[Item]
    q.drainTo(l)
    assertTrue(l.size() >= SIZE)
    var i = 0
    while (i < SIZE) {
      mustEqual(l.get(i), i)
      i += 1
    }
    t.join()
    assertTrue(q.size() + l.size() >= SIZE)
  }

  /** drainTo(c, n) empties first min(n, size) elements of queue into c
   */
  @Test def testDrainToN(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    var i = 0
    while (i < SIZE + 2) {
      var j = 0
      while (j < SIZE) {
        mustOffer(q, j)
        j += 1
      }
      val l = new ArrayList[Item]
      q.drainTo(l, i)
      val k = if (i < SIZE) i else SIZE
      mustEqual(k, l.size())
      mustEqual(SIZE - k, q.size())
      j = 0
      while (j < k) {
        mustEqual(l.get(j), j)
        j += 1
      }
      while (q.poll() != null) {}
      i += 1
    }
  }

  /** remove(null), contains(null) always return false
   */
  @Test def testNeverContainsNull(): Unit = {
    val qs: Array[Deque[_]] = Array(
      new LinkedBlockingDeque[AnyRef],
      LinkedBlockingDequeTest.populatedDeque(2)
    )

    for (q <- qs) {
      assertFalse(q.contains(null))
      assertFalse(q.remove(null))
      assertFalse(q.removeFirstOccurrence(null))
      assertFalse(q.removeLastOccurrence(null))
    }
  }

  /** Ensure that putFirst(), putLast(), takeFirst(), and takeLast()
   *  immediately throw an InterruptedException if the thread is
   *  interrupted, to be consistent with other blocking queues such as
   *  ArrayBlockingQueue and LinkedBlockingQueue
   */
  @Test def testInterruptedExceptionThrownInBlockingMethods(): Unit = {
    val pool = Executors.newSingleThreadExecutor()
    try {
      val success = pool.submit(new java.util.concurrent.Callable[Void] {
        override def call(): Void = {
          val queue = new LinkedBlockingDeque[AnyRef]
          Thread.currentThread().interrupt()
          try {
            queue.putFirst(new Object)
            fail("Expected InterruptedException in putFirst()")
          } catch {
            case expected: InterruptedException =>
              assertFalse(Thread.currentThread().isInterrupted())
          }

          Thread.currentThread().interrupt()
          try {
            queue.putLast(new Object)
            fail("Expected InterruptedException in putLast()")
          } catch {
            case expected: InterruptedException =>
              assertFalse(Thread.currentThread().isInterrupted())
          }

          queue.add(new Object)
          Thread.currentThread().interrupt()
          try {
            queue.takeFirst()
            fail("Expected InterruptedException in takeFirst()")
          } catch {
            case expected: InterruptedException =>
              assertFalse(Thread.currentThread().isInterrupted())
          }

          queue.add(new Object)
          Thread.currentThread().interrupt()
          try {
            queue.takeLast()
            fail("Expected InterruptedException in takeLast()")
          } catch {
            case expected: InterruptedException =>
              assertFalse(Thread.currentThread().isInterrupted())
          }
          null
        }
      })
      try {
        success.get()
      } catch {
        case e: java.util.concurrent.ExecutionException =>
          throw e.getCause() match {
            case rt: RuntimeException => throw rt
            case err: Error => throw err
            case cause => throw new AssertionError(cause)
          }
      }
    } finally {
      pool.shutdown()
    }
  }

  @Test def testWeaklyConsistentIterationWithClear(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    q.add(one)
    q.add(two)
    q.add(three)
    val it = q.iterator()
    mustEqual(one, it.next())
    q.clear()
    q.add(four)
    q.add(five)
    q.add(six)
    mustEqual(two, it.next())
    mustEqual(four, it.next())
    mustEqual(five, it.next())
    mustEqual(six, it.next())
    mustEqual(3, q.size())
  }

  @Test def testWeaklyConsistentIterationWithIteratorRemove(): Unit = {
    val q = new LinkedBlockingDeque[Item]
    q.add(one)
    q.add(two)
    q.add(three)
    q.add(four)
    q.add(five)
    val it1 = q.iterator()
    val it2 = q.iterator()
    val it3 = q.iterator()
    mustEqual(one, it1.next())
    mustEqual(two, it1.next())
    it1.remove() // removing "two"
    mustEqual(one, it2.next())
    it2.remove() // removing "one"
    mustEqual(three, it2.next())
    mustEqual(four, it2.next())
    it2.remove() // removing "four"
    mustEqual(one, it3.next())
    mustEqual(three, it3.next())
    mustEqual(five, it3.next())
    assertFalse(it3.hasNext())
    mustEqual(three, it1.next())
    mustEqual(five, it1.next())
    assertFalse(it1.hasNext())
    mustEqual(five, it2.next())
    assertFalse(it2.hasNext())
    mustEqual(2, q.size())
  }
}

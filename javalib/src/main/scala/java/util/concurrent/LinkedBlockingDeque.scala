/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package java.util.concurrent

import java.util
import java.util._
import java.util.concurrent.locks._
import java.util.function._

import scala.scalanative.annotation.safePublish

@SerialVersionUID(-387911632671998426L)
object LinkedBlockingDeque {

  /** Doubly-linked list node class */
  private[concurrent] final class Node[E] private[concurrent] (var item: E) {
    private[concurrent] var prev: Node[E] = _
    private[concurrent] var next: Node[E] = _
  }
}

/** An optionally-bounded {@linkplain BlockingDeque blocking deque} based on
  * linked nodes.
  *
  * The optional capacity bound constructor argument serves as a way to prevent
  * excessive expansion. The capacity, if unspecified, is equal to
  * {@link Integer#MAX_VALUE}. Linked nodes are dynamically created upon each
  * insertion unless this would bring the deque above capacity.
  *
  * Most operations run in constant time (ignoring time spent blocking).
  * Exceptions include {@link #remove(Object) remove},
  * {@link #removeFirstOccurrence removeFirstOccurrence},
  * {@link #removeLastOccurrence removeLastOccurrence}, {@link #contains
  * contains}, and the bulk operations, all of which run in linear time.
  *
  * This class and its iterator implement all of the <em>optional</em> methods
  * of the {@link Collection} and {@link Iterator} interfaces.
  *
  * This class is a member of the
  * <a href="{@docRoot}/java.base/java/util/package-summary.html#CollectionsFramework">
  * Java Collections Framework</a>.
  *
  * @since 1.6
  * @author Doug Lea
  * @param <E>
  *   the type of elements held in this deque
  */
@SerialVersionUID(-387911632671998426L)
class LinkedBlockingDeque[E <: AnyRef](_capacity: Int)
    extends util.AbstractQueue[E]
    with BlockingDeque[E]
    with Serializable {
  import LinkedBlockingDeque._

  if (_capacity <= 0) throw new IllegalArgumentException

  /** Pointer to first node. Invariant: (first == null && last == null) ||
    * (first.prev == null && first.item != null)
    */
  private var first: Node[E] = _

  /** Pointer to last node. Invariant: (first == null && last == null) ||
    * (last.next == null && last.item != null)
    */
  private var last: Node[E] = _

  /** Number of items in the deque */
  @safePublish
  private var count: Int = 0

  /** Main lock guarding all access */
  @safePublish
  private final val lock = new ReentrantLock()

  /** Condition for waiting takes */
  @safePublish
  private final val notEmpty: Condition = lock.newCondition()

  /** Condition for waiting puts */
  @safePublish
  private final val notFull: Condition = lock.newCondition()

  def this() = this(Integer.MAX_VALUE)

  def this(c: util.Collection[_ <: E]) = {
    this(Integer.MAX_VALUE)
    addAll(c)
  }

  // Basic linking and unlinking operations, called only while holding lock

  /** Links node as first element, or returns false if full. */
  private def linkFirst(node: Node[E]): Boolean = {
    // assert lock.isHeldByCurrentThread();
    val c = count
    if (c >= _capacity) return false
    val f = first
    node.next = f
    first = node
    if (last == null) last = node
    else f.prev = node
    count = c + 1
    notEmpty.signal()
    true
  }

  /** Links node as last element, or returns false if full. */
  private def linkLast(node: Node[E]): Boolean = {
    // assert lock.isHeldByCurrentThread();
    val c = count
    if (c >= _capacity) return false
    val l = last
    node.prev = l
    last = node
    if (first == null) first = node
    else l.next = node
    count = c + 1
    notEmpty.signal()
    true
  }

  /** Removes and returns first element, or null if empty. */
  private def unlinkFirst(): E = {
    // assert lock.isHeldByCurrentThread();
    val f = first
    if (f == null) return null.asInstanceOf[E]
    val n = f.next
    val item = f.item
    f.item = null.asInstanceOf[E]
    f.next = f // help GC
    first = n
    if (n == null) last = null
    else n.prev = null
    count -= 1
    notFull.signal()
    item
  }

  /** Removes and returns last element, or null if empty. */
  private def unlinkLast(): E = {
    // assert lock.isHeldByCurrentThread();
    val l = last
    if (l == null) return null.asInstanceOf[E]
    val p = l.prev
    val item = l.item
    l.item = null.asInstanceOf[E]
    l.prev = l // help GC
    last = p
    if (p == null) first = null
    else p.next = null
    count -= 1
    notFull.signal()
    item
  }

  /** Unlinks x. */
  private def unlink(x: Node[E]): Unit = {
    // assert lock.isHeldByCurrentThread();
    // assert x.item != null;
    val p = x.prev
    val n = x.next
    if (p == null) {
      unlinkFirst()
    } else if (n == null) {
      unlinkLast()
    } else {
      p.next = n
      n.prev = p
      x.item = null.asInstanceOf[E]
      // Don't mess with x's links. They may still be in use by
      // an iterator.
      count -= 1
      notFull.signal()
    }
  }

  // BlockingDeque methods

  /** @throws IllegalStateException
    *   if this deque is full
    * @throws NullPointerException
    *   {@inheritDoc}
    */
  override def addFirst(e: E): Unit = {
    if (!offerFirst(e))
      throw new IllegalStateException("Deque full")
  }

  /** @throws IllegalStateException
    *   if this deque is full
    * @throws NullPointerException
    *   {@inheritDoc}
    */
  override def addLast(e: E): Unit = {
    if (!offerLast(e))
      throw new IllegalStateException("Deque full")
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    */
  override def offerFirst(e: E): Boolean = {
    if (e == null) throw new NullPointerException()
    if (count >= _capacity) return false
    val node = new Node[E](e)
    lock.lock()
    try linkFirst(node)
    finally lock.unlock()
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    */
  override def offerLast(e: E): Boolean = {
    if (e == null) throw new NullPointerException()
    if (count >= _capacity) return false
    val node = new Node[E](e)
    lock.lock()
    try linkLast(node)
    finally lock.unlock()
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    * @throws InterruptedException
    *   {@inheritDoc}
    */
  @throws[InterruptedException]
  override def putFirst(e: E): Unit = {
    if (e == null) throw new NullPointerException()
    val node = new Node[E](e)
    lock.lockInterruptibly()
    try {
      while (!linkFirst(node))
        notFull.await()
    } finally lock.unlock()
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    * @throws InterruptedException
    *   {@inheritDoc}
    */
  @throws[InterruptedException]
  override def putLast(e: E): Unit = {
    if (e == null) throw new NullPointerException()
    val node = new Node[E](e)
    lock.lockInterruptibly()
    try {
      while (!linkLast(node))
        notFull.await()
    } finally lock.unlock()
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    * @throws InterruptedException
    *   {@inheritDoc}
    */
  @throws[InterruptedException]
  override def offerFirst(e: E, timeout: Long, unit: TimeUnit): Boolean = {
    if (e == null) throw new NullPointerException()
    val node = new Node[E](e)
    var nanos = unit.toNanos(timeout)
    lock.lockInterruptibly()
    try {
      while (!linkFirst(node)) {
        if (nanos <= 0L) return false
        nanos = notFull.awaitNanos(nanos)
      }
      true
    } finally lock.unlock()
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    * @throws InterruptedException
    *   {@inheritDoc}
    */
  @throws[InterruptedException]
  override def offerLast(e: E, timeout: Long, unit: TimeUnit): Boolean = {
    if (e == null) throw new NullPointerException()
    val node = new Node[E](e)
    var nanos = unit.toNanos(timeout)
    lock.lockInterruptibly()
    try {
      while (!linkLast(node)) {
        if (nanos <= 0L) return false
        nanos = notFull.awaitNanos(nanos)
      }
      true
    } finally lock.unlock()
  }

  /** @throws NoSuchElementException
    *   {@inheritDoc}
    */
  override def removeFirst(): E = {
    val x = pollFirst()
    if (x == null) throw new NoSuchElementException()
    x
  }

  /** @throws NoSuchElementException
    *   {@inheritDoc}
    */
  override def removeLast(): E = {
    val x = pollLast()
    if (x == null) throw new NoSuchElementException()
    x
  }

  override def pollFirst(): E = {
    if (count == 0) return null.asInstanceOf[E]
    lock.lock()
    try unlinkFirst()
    finally lock.unlock()
  }

  override def pollLast(): E = {
    if (count == 0) return null.asInstanceOf[E]
    lock.lock()
    try unlinkLast()
    finally lock.unlock()
  }

  @throws[InterruptedException]
  override def takeFirst(): E = {
    lock.lockInterruptibly()
    try {
      var x: E = null.asInstanceOf[E]
      while ({ x = unlinkFirst(); x == null })
        notEmpty.await()
      x
    } finally lock.unlock()
  }

  @throws[InterruptedException]
  override def takeLast(): E = {
    lock.lockInterruptibly()
    try {
      var x: E = null.asInstanceOf[E]
      while ({ x = unlinkLast(); x == null })
        notEmpty.await()
      x
    } finally lock.unlock()
  }

  @throws[InterruptedException]
  override def pollFirst(timeout: Long, unit: TimeUnit): E = {
    var nanos = unit.toNanos(timeout)
    lock.lockInterruptibly()
    try {
      var x: E = null.asInstanceOf[E]
      while ({ x = unlinkFirst(); x == null }) {
        if (nanos <= 0L) return null.asInstanceOf[E]
        nanos = notEmpty.awaitNanos(nanos)
      }
      x
    } finally lock.unlock()
  }

  @throws[InterruptedException]
  override def pollLast(timeout: Long, unit: TimeUnit): E = {
    var nanos = unit.toNanos(timeout)
    lock.lockInterruptibly()
    try {
      var x: E = null.asInstanceOf[E]
      while ({ x = unlinkLast(); x == null }) {
        if (nanos <= 0L) return null.asInstanceOf[E]
        nanos = notEmpty.awaitNanos(nanos)
      }
      x
    } finally lock.unlock()
  }

  /** @throws NoSuchElementException
    *   {@inheritDoc}
    */
  override def getFirst(): E = {
    val x = peekFirst()
    if (x == null) throw new NoSuchElementException()
    x
  }

  /** @throws NoSuchElementException
    *   {@inheritDoc}
    */
  override def getLast(): E = {
    val x = peekLast()
    if (x == null) throw new NoSuchElementException()
    x
  }

  override def peekFirst(): E = {
    if (count == 0) return null.asInstanceOf[E]
    lock.lock()
    try if (first == null) null.asInstanceOf[E] else first.item
    finally lock.unlock()
  }

  override def peekLast(): E = {
    if (count == 0) return null.asInstanceOf[E]
    lock.lock()
    try if (last == null) null.asInstanceOf[E] else last.item
    finally lock.unlock()
  }

  override def removeFirstOccurrence(o: Any): Boolean = {
    if (o == null) return false
    lock.lock()
    try {
      var p = first
      while (p != null) {
        if (o.equals(p.item)) {
          unlink(p)
          return true
        }
        p = p.next
      }
      false
    } finally lock.unlock()
  }

  override def removeLastOccurrence(o: Any): Boolean = {
    if (o == null) return false
    lock.lock()
    try {
      var p = last
      while (p != null) {
        if (o.equals(p.item)) {
          unlink(p)
          return true
        }
        p = p.prev
      }
      false
    } finally lock.unlock()
  }

  // BlockingQueue methods

  /** Inserts the specified element at the end of this deque unless it would
    * violate capacity restrictions. When using a capacity-restricted deque, it
    * is generally preferable to use method {@link #offer(Object) offer}.
    *
    * This method is equivalent to {@link #addLast}.
    *
    * @throws IllegalStateException
    *   if this deque is full
    * @throws NullPointerException
    *   if the specified element is null
    */
  override def add(e: E): Boolean = {
    addLast(e)
    true
  }

  /** {@inheritDoc BlockingDeque}
    * @throws NullPointerException
    *   if the specified element is null
    * @return
    *   {@inheritDoc BlockingDeque}
    */
  override def offer(e: E): Boolean =
    offerLast(e)

  /** @throws NullPointerException
    *   {@inheritDoc}
    * @throws InterruptedException
    *   {@inheritDoc}
    */
  @throws[InterruptedException]
  override def put(e: E): Unit =
    putLast(e)

  /** @throws NullPointerException
    *   {@inheritDoc}
    * @throws InterruptedException
    *   {@inheritDoc}
    */
  @throws[InterruptedException]
  override def offer(e: E, timeout: Long, unit: TimeUnit): Boolean =
    offerLast(e, timeout, unit)

  /** Retrieves and removes the head of the queue represented by this deque.
    * This method differs from {@link #poll() poll()} only in that it throws an
    * exception if this deque is empty.
    *
    * This method is equivalent to {@link #removeFirst() removeFirst}.
    *
    * @return
    *   the head of the queue represented by this deque
    * @throws NoSuchElementException
    *   if this deque is empty
    */
  override def remove(): E =
    removeFirst()

  /** {@inheritDoc BlockingDeque}
    * @return
    *   {@inheritDoc BlockingDeque}
    */
  override def poll(): E =
    pollFirst()

  @throws[InterruptedException]
  override def take(): E =
    takeFirst()

  @throws[InterruptedException]
  override def poll(timeout: Long, unit: TimeUnit): E =
    pollFirst(timeout, unit)

  /** Retrieves, but does not remove, the head of the queue represented by this
    * deque. This method differs from {@link #peek() peek()} only in that it
    * throws an exception if this deque is empty.
    *
    * This method is equivalent to {@link #getFirst() getFirst}.
    *
    * @return
    *   the head of the queue represented by this deque
    * @throws NoSuchElementException
    *   if this deque is empty
    */
  override def element(): E =
    getFirst()

  /** {@inheritDoc BlockingDeque}
    * @return
    *   {@inheritDoc BlockingDeque}
    */
  override def peek(): E =
    peekFirst()

  /** Returns the number of additional elements that this deque can ideally (in
    * the absence of memory or resource constraints) accept without blocking.
    * This is always equal to the initial capacity of this deque less the current
    * {@code size} of this deque.
    *
    * Note that you <em>cannot</em> always tell if an attempt to insert an
    * element will succeed by inspecting {@code remainingCapacity} because it may
    * be the case that another thread is about to insert or remove an element.
    */
  override def remainingCapacity(): Int =
    _capacity - count

  /** @throws UnsupportedOperationException
    *   {@inheritDoc}
    * @throws ClassCastException
    *   {@inheritDoc}
    * @throws NullPointerException
    *   {@inheritDoc}
    * @throws IllegalArgumentException
    *   {@inheritDoc}
    */
  override def drainTo(c: util.Collection[_ >: E]): Int =
    drainTo(c, Integer.MAX_VALUE)

  /** @throws UnsupportedOperationException
    *   {@inheritDoc}
    * @throws ClassCastException
    *   {@inheritDoc}
    * @throws NullPointerException
    *   {@inheritDoc}
    * @throws IllegalArgumentException
    *   {@inheritDoc}
    */
  override def drainTo(c: util.Collection[_ >: E], maxElements: Int): Int = {
    Objects.requireNonNull(c)
    if (c eq this) throw new IllegalArgumentException()
    if (maxElements <= 0) return 0
    lock.lock()
    try {
      val n = Math.min(maxElements, count)
      var i = 0
      while (i < n) {
        c.add(first.item) // In this order, in case add() throws.
        unlinkFirst()
        i += 1
      }
      n
    } finally lock.unlock()
  }

  // Stack methods

  /** @throws IllegalStateException
    *   if this deque is full
    * @throws NullPointerException
    *   {@inheritDoc}
    */
  override def push(e: E): Unit =
    addFirst(e)

  /** @throws NoSuchElementException
    *   {@inheritDoc}
    */
  override def pop(): E =
    removeFirst()

  // Collection methods

  /** Removes the first occurrence of the specified element from this deque. If
    * the deque does not contain the element, it is unchanged. More formally,
    * removes the first element {@code e} such that {@code o.equals(e)} (if such
    * an element exists). Returns {@code true} if this deque contained the
    * specified element (or equivalently, if this deque changed as a result of
    * the call).
    *
    * This method is equivalent to {@link #removeFirstOccurrence(Object)
    * removeFirstOccurrence}.
    *
    * @param o
    *   element to be removed from this deque, if present
    * @return
    *   {@code true} if this deque changed as a result of the call
    */
  override def remove(o: Any): Boolean =
    removeFirstOccurrence(o)

  /** Returns the number of elements in this deque.
    *
    * @return
    *   the number of elements in this deque
    */
  override def size(): Int = count

  /** Returns {@code true} if this deque contains the specified element. More
    * formally, returns {@code true} if and only if this deque contains at least
    * one element {@code e} such that {@code o.equals(e)}.
    *
    * @param o
    *   object to be checked for containment in this deque
    * @return
    *   {@code true} if this deque contains the specified element
    */
  override def contains(o: Any): Boolean = {
    if (o == null) return false
    lock.lock()
    try {
      var p = first
      while (p != null) {
        if (o.equals(p.item)) return true
        p = p.next
      }
      false
    } finally lock.unlock()
  }

  /** Appends all of the elements in the specified collection to the end of this
    * deque, in the order that they are returned by the specified collection's
    * iterator. Attempts to {@code addAll} of a deque to itself result in
    * {@code IllegalArgumentException}.
    *
    * @param c
    *   the elements to be inserted into this deque
    * @return
    *   {@code true} if this deque changed as a result of the call
    * @throws NullPointerException
    *   if the specified collection or any of its elements are null
    * @throws IllegalArgumentException
    *   if the collection is this deque
    * @throws IllegalStateException
    *   if this deque is full
    * @see
    *   #add(Object)
    */
  override def addAll(c: util.Collection[_ <: E]): Boolean = {
    if (c eq this)
      // As historically specified in AbstractQueue#addAll
      throw new IllegalArgumentException()

    // Copy c into a private chain of Nodes
    var beg: Node[E] = null
    var end: Node[E] = null
    var n = 0
    val it = c.iterator()
    while (it.hasNext()) {
      val e = it.next()
      Objects.requireNonNull(e)
      n += 1
      val newNode = new Node[E](e)
      if (beg == null) beg = newNode
      else {
        end.next = newNode
        newNode.prev = end
      }
      end = newNode
    }
    if (beg == null) return false

    // Atomically append the chain at the end
    lock.lock()
    try {
      val cnt = count + n
      if (cnt <= _capacity) {
        beg.prev = last
        if (first == null) first = beg
        else last.next = beg
        last = end
        count = cnt
        notEmpty.signalAll()
        return true
      }
    } finally lock.unlock()

    // Fall back to historic non-atomic implementation, failing
    // with IllegalStateException when the capacity is exceeded.
    beg = null
    end = null // help GC
    super.addAll(c)
  }

  /** Returns an array containing all of the elements in this deque, in proper
    * sequence (from first to last element).
    *
    * The returned array will be "safe" in that no references to it are
    * maintained by this deque. (In other words, this method must allocate a new
    * array). The caller is thus free to modify the returned array.
    *
    * This method acts as bridge between array-based and collection-based APIs.
    *
    * @return
    *   an array containing all of the elements in this deque
    */
  override def toArray(): Array[AnyRef] = {
    lock.lock()
    try {
      val a = new Array[AnyRef](count)
      var k = 0
      var p = first
      while (p != null) {
        val idx = k
        k += 1
        a(idx) = p.item
        p = p.next
      }
      a
    } finally lock.unlock()
  }

  /** Returns an array containing all of the elements in this deque, in proper
    * sequence; the runtime type of the returned array is that of the specified
    * array. If the deque fits in the specified array, it is returned therein.
    * Otherwise, a new array is allocated with the runtime type of the specified
    * array and the size of this deque.
    *
    * If this deque fits in the specified array with room to spare (i.e., the
    * array has more elements than this deque), the element in the array
    * immediately following the end of the deque is set to {@code null}.
    *
    * Like the {@link #toArray()} method, this method acts as bridge between
    * array-based and collection-based APIs. Further, this method allows precise
    * control over the runtime type of the output array, and may, under certain
    * circumstances, be used to save allocation costs.
    *
    * Suppose {@code x} is a deque known to contain only strings. The following
    * code can be used to dump the deque into a newly allocated array of {@code
    * String}:
    *
    * <pre> {@code String[] y = x.toArray(new String[0]);}</pre>
    *
    * Note that {@code toArray(new Object[0])} is identical in function to
    * {@code toArray()}.
    *
    * @param a
    *   the array into which the elements of the deque are to be stored, if it is
    *   big enough; otherwise, a new array of the same runtime type is allocated
    *   for this purpose
    * @return
    *   an array containing all of the elements in this deque
    * @throws ArrayStoreException
    *   if the runtime type of the specified array is not a supertype of the
    *   runtime type of every element in this deque
    * @throws NullPointerException
    *   if the specified array is null
    */
  override def toArray[T <: AnyRef](a: Array[T]): Array[T] = {
    var aa = a
    lock.lock()
    try {
      if (aa.length < count)
        aa = java.lang.reflect.Array
          .newInstance(aa.getClass.getComponentType, count)
          .asInstanceOf[Array[T]]

      var k = 0
      var p = first
      while (p != null) {
        val idx = k
        k += 1
        aa(idx) = p.item.asInstanceOf[T]
        p = p.next
      }
      if (aa.length > k) aa(k) = null.asInstanceOf[T]
      aa
    } finally lock.unlock()
  }

  override def toString: String = Helpers.collectionToString(this)

  /** Atomically removes all of the elements from this deque. The deque will be
    * empty after this call returns.
    */
  override def clear(): Unit = {
    lock.lock()
    try {
      var f = first
      while (f != null) {
        f.item = null.asInstanceOf[E]
        val n = f.next
        f.prev = f
        f.next = f
        f = n
      }
      first = null
      last = null
      count = 0
      notFull.signalAll()
    } finally lock.unlock()
  }

  /** Used for any element traversal that is not entirely under lock. Such
    * traversals must handle both:
    *   - dequeued nodes (p.next == p)
    *   - (possibly multiple) interior removed nodes (p.item == null)
    */
  private[concurrent] def succ(p: Node[E]): Node[E] = {
    val next = p.next
    if (p eq next) first
    else next
  }

  /** Returns an iterator over the elements in this deque in proper sequence.
    * The elements will be returned in order from first (head) to last (tail).
    *
    * The returned iterator is <a
    * href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
    *
    * @return
    *   an iterator over the elements in this deque in proper sequence
    */
  override def iterator(): util.Iterator[E] = new Itr()

  /** Returns an iterator over the elements in this deque in reverse sequential
    * order. The elements will be returned in order from last (tail) to first
    * (head).
    *
    * The returned iterator is <a
    * href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
    *
    * @return
    *   an iterator over the elements in this deque in reverse order
    */
  override def descendingIterator(): util.Iterator[E] = new DescendingItr()

  /** Base class for LinkedBlockingDeque iterators. */
  private abstract class AbstractItr extends util.Iterator[E] {

    /** The next node to return in next(). */
    private var nextNode: Node[E] = _

    /** nextItem holds on to item fields because once we claim that an element
      * exists in hasNext(), we must return item read under lock even if it was
      * in the process of being removed when hasNext() was called.
      */
    private var nextItem: E = _

    /** Node returned by most recent call to next. Needed by remove. Reset to
      * null if this element is deleted by a call to remove.
      */
    private var lastRet: Node[E] = _

    protected def firstNode(): Node[E]
    protected def nextNode(n: Node[E]): Node[E]

    private def succ(p: Node[E]): Node[E] = {
      val next = nextNode(p)
      if (p eq next) firstNode()
      else next
    }

    // set to initial position
    lock.lock()
    try {
      nextNode = firstNode()
      if (nextNode != null) nextItem = nextNode.item
    } finally lock.unlock()

    override def hasNext(): Boolean = nextNode != null

    override def next(): E = {
      val p = nextNode
      if (p == null) throw new NoSuchElementException()
      lastRet = p
      val x = nextItem
      lock.lock()
      try {
        var e: E = null.asInstanceOf[E]
        var q = nextNode(p)
        while (q != null && { e = q.item; e == null })
          q = succ(q)
        nextNode = q
        nextItem = e
      } finally lock.unlock()
      x
    }

    override def forEachRemaining(action: Consumer[_ >: E]): Unit = {
      // A variant of forEachFrom
      Objects.requireNonNull(action)
      var p = nextNode
      if (p == null) return
      lastRet = p
      nextNode = null
      val batchSize = 64
      var es: Array[AnyRef] = null
      var n = 0
      var len = 1
      while ({
        lock.lock()
        try {
          if (es == null) {
            p = nextNode(p)
            var q = p
            var break = false
            while (q != null && !break) {
              if (q.item != null && { len += 1; len } == batchSize)
                break = true
              else
                q = succ(q)
            }
            es = new Array[AnyRef](len)
            es(0) = nextItem
            nextItem = null.asInstanceOf[E]
            n = 1
          } else n = 0

          var i = 0
          while (p != null && n < len) {
            val elem = p.item
            es(n) = elem
            if (elem != null) {
              lastRet = p
              n += 1
            }
            p = succ(p)
            i += 1
          }
        } finally lock.unlock()

        var i = 0
        while (i < n) {
          action.accept(es(i).asInstanceOf[E])
          i += 1
        }

        n > 0 && p != null
      }) ()
    }

    override def remove(): Unit = {
      val n = lastRet
      if (n == null) throw new IllegalStateException()
      lastRet = null
      lock.lock()
      try {
        if (n.item != null)
          unlink(n)
      } finally lock.unlock()
    }
  }

  /** Forward iterator */
  private class Itr extends AbstractItr {
    override protected def firstNode(): Node[E] = first
    override protected def nextNode(n: Node[E]): Node[E] = n.next
  }

  /** Descending iterator */
  private class DescendingItr extends AbstractItr {
    override protected def firstNode(): Node[E] = last
    override protected def nextNode(n: Node[E]): Node[E] = n.prev
  }

  /** A customized variant of Spliterators.IteratorSpliterator. Keep this class
    * in sync with (very similar) LBQSpliterator.
    */
  private final class LBDSpliterator extends Spliterator[E] {
    private val MAX_BATCH = 1 << 25 // max batch array size;
    private var current: Node[E] = _ // current node; null until initialized
    private var batch = 0 // batch size for splits
    private var exhausted = false // true when no more nodes
    private var est: Long = size() // size estimate

    override def estimateSize(): Long = est

    override def trySplit(): Spliterator[E] = {
      var h: Node[E] = null
      if (!exhausted &&
          ({ h = current; h != null } || { h = first; h != null }) &&
          h.next != null) {
        batch = Math.min(batch + 1, MAX_BATCH)
        val n = batch
        val a = new Array[AnyRef](n)
        var i = 0
        var p: Node[E] = current
        lock.lock()
        try {
          if (p != null || { p = first; p != null })
            while (p != null && i < n) {
              val elem = p.item
              if (elem != null) {
                a(i) = elem
                i += 1
              }
              p = succ(p)
            }
        } finally lock.unlock()

        if ({ current = p; current == null }) {
          est = 0L
          exhausted = true
        } else if ({ est -= i; est < 0L }) est = 0L

        if (i > 0)
          return Spliterators.spliterator(
            a,
            0,
            i,
            Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.CONCURRENT
          )
      }
      null
    }

    override def tryAdvance(action: Consumer[_ >: E]): Boolean = {
      Objects.requireNonNull(action)
      if (!exhausted) {
        var e: E = null.asInstanceOf[E]
        lock.lock()
        try {
          var p: Node[E] = current
          if (p != null || { p = first; p != null })
            while ({
              e = p.item
              p = succ(p)
              e == null && p != null
            }) ()
          current = p
          if (current == null) exhausted = true
        } finally lock.unlock()

        if (e != null) {
          action.accept(e)
          return true
        }
      }
      false
    }

    override def forEachRemaining(action: Consumer[_ >: E]): Unit = {
      Objects.requireNonNull(action)
      if (!exhausted) {
        exhausted = true
        val p = current
        current = null
        forEachFrom(action, p)
      }
    }

    override def characteristics(): Int =
      Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.CONCURRENT
  }

  /** Returns a {@link Spliterator} over the elements in this deque.
    *
    * The returned spliterator is <a
    * href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
    *
    * The {@code Spliterator} reports {@link Spliterator#CONCURRENT}, {@link
    * Spliterator#ORDERED}, and {@link Spliterator#NONNULL}.
    *
    * @implNote
    *   The {@code Spliterator} implements {@code trySplit} to permit limited
    *   parallelism.
    *
    * @return
    *   a {@code Spliterator} over the elements in this deque
    * @since 1.8
    */
  override def spliterator(): Spliterator[E] = new LBDSpliterator()

  /** @throws NullPointerException
    *   {@inheritDoc}
    */
  override def forEach(action: Consumer[_ >: E]): Unit = {
    Objects.requireNonNull(action)
    forEachFrom(action, null)
  }

  /** Runs action on each element found during a traversal starting at p. If p
    * is null, traversal starts at head.
    */
  private[concurrent] def forEachFrom(
      action: Consumer[_ >: E],
      _p: Node[E]
  ): Unit = {
    // Extract batches of elements while holding the lock; then
    // run the action on the elements while not
    var p = _p
    val batchSize = 64 // max number of elements per batch
    var es: Array[AnyRef] = null // container for batch of elements
    var n = 0
    var len = 0
    while ({
      lock.lock()
      try {
        if (es == null) {
          if (p == null) p = first
          var q = p
          var break = false
          while (q != null && !break) {
            if (q.item != null && { len += 1; len } == batchSize)
              break = true
            else
              q = succ(q)
          }
          es = new Array[AnyRef](len)
        }

        n = 0
        while (p != null && n < len) {
          val elem = p.item
          es(n) = elem
          if (elem != null) n += 1
          p = succ(p)
        }
      } finally lock.unlock()

      var i = 0
      while (i < n) {
        action.accept(es(i).asInstanceOf[E])
        i += 1
      }

      n > 0 && p != null
    }) ()
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    */
  override def removeIf(filter: Predicate[_ >: E]): Boolean = {
    Objects.requireNonNull(filter)
    bulkRemove(filter)
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    */
  override def removeAll(c: util.Collection[_]): Boolean = {
    Objects.requireNonNull(c)
    bulkRemove((e: E) => c.contains(e))
  }

  /** @throws NullPointerException
    *   {@inheritDoc}
    */
  override def retainAll(c: util.Collection[_]): Boolean = {
    Objects.requireNonNull(c)
    bulkRemove((e: E) => !c.contains(e))
  }

  /** Implementation of bulk remove methods. */
  private def bulkRemove(filter: Predicate[_ >: E]): Boolean = {
    var removed = false
    var p: Node[E] = null
    var nodes: Array[Node[E]] = null
    var n = 0
    var len = 0
    while ({
      // 1. Extract batch of up to 64 elements while holding the lock.
      lock.lock()
      try {
        if (nodes == null) { // first batch; initialize
          p = first
          var q = p
          var break = false
          while (q != null && !break) {
            if (q.item != null && { len += 1; len } == 64)
              break = true
            else
              q = succ(q)
          }
          nodes = new Array[Node[AnyRef]](len).asInstanceOf[Array[Node[E]]]
        }
        n = 0
        while (p != null && n < len) {
          val idx = n
          n += 1
          nodes(idx) = p
          p = succ(p)
        }
      } finally lock.unlock()

      // 2. Run the filter on the elements while lock is free.
      var deathRow = 0L // "bitset" of size 64
      var i = 0
      while (i < n) {
        val e = nodes(i).item
        if (e != null && filter.test(e))
          deathRow |= 1L << i
        i += 1
      }

      // 3. Remove any filtered elements while holding the lock.
      if (deathRow != 0) {
        lock.lock()
        try {
          var i = 0
          while (i < n) {
            val mask = 1L << i
            if ((deathRow & mask) != 0L) {
              val q = nodes(i)
              if (q.item != null) {
                unlink(q)
                removed = true
              }
            }
            nodes(i) = null // help GC
            i += 1
          }
        } finally lock.unlock()
      }

      n > 0 && p != null
    }) ()
    removed
  }

  /** Capacity accessor (for API compatibility with LinkedBlockingQueue).
    *
    * @return
    *   the capacity of this deque
    */
  def capacity(): Int = _capacity
}

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package java.util.concurrent.locks

import java.io.{ObjectInputStream, IOException}
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec

import scala.scalanative.libc.stdatomic.memory_order._
import scala.scalanative.libc.stdatomic.{AtomicInt, AtomicLongLong, AtomicRef}
import scala.scalanative.runtime.{Intrinsics, fromRawPtr}

/**
 * A capability-based lock with three modes for controlling read/write access.
 *
 * The state of a StampedLock consists of a version and mode. Lock acquisition
 * methods return a stamp that represents and controls access with respect to a
 * lock state; "try" versions of these methods may instead return the special
 * value zero to represent failure to acquire access. Lock release and conversion
 * methods require stamps as arguments, and fail if they do not match the state
 * of the lock. The three modes are:
 *
 *  - Writing. Method `writeLock` possibly blocks waiting for exclusive access,
 *    returning a stamp that can be used in method `unlockWrite` to release the
 *    lock. Untimed and timed versions of `tryWriteLock` are also provided. When
 *    the lock is held in write mode, no read locks may be obtained, and all
 *    optimistic read validations will fail.
 *
 *  - Reading. Method `readLock` possibly blocks waiting for non-exclusive access,
 *    returning a stamp that can be used in method `unlockRead` to release the
 *    lock. Untimed and timed versions of `tryReadLock` are also provided.
 *
 *  - Optimistic Reading. Method `tryOptimisticRead` returns a non-zero stamp only
 *    if the lock is not currently held in write mode. Method `validate` returns
 *    true if the lock has not been acquired in write mode since obtaining a given
 *    stamp, in which case all actions prior to the most recent write lock release
 *    happen-before actions following the call to `tryOptimisticRead`. This mode
 *    can be thought of as an extremely weak version of a read-lock, that can be
 *    broken by a writer at any time.
 *
 * This class also supports methods that conditionally provide conversions across
 * the three modes. For example, method `tryConvertToWriteLock` attempts to
 * "upgrade" a mode, returning a valid write stamp if (1) already in writing mode
 * (2) in reading mode and there are no other readers or (3) in optimistic read
 * mode and the lock is available.
 *
 * StampedLocks are designed for use as internal utilities in the development of
 * thread-safe components. Their use relies on knowledge of the internal properties
 * of the data, objects, and methods they are protecting. They are not reentrant,
 * so locked bodies should not call other unknown methods that may try to re-acquire
 * locks.
 *
 * @since 1.8
 * @author Doug Lea
 */
@SerialVersionUID(-6001602636862214147L)
class StampedLock() extends Serializable {

  import StampedLock._

  /** Lock sequence/state */
  @volatile private var state: Long = ORIGIN

  /** Head of CLH queue */
  @volatile private var head: Node = _

  /** Tail (last) of CLH queue */
  @volatile private var tail: Node = _

  /** extra reader count when state read count saturated */
  @volatile private var readerOverflow: Int = 0

  // views
  @transient private var readLockView: ReadLockView = _
  @transient private var writeLockView: WriteLockView = _
  @transient private var readWriteLockView: ReadWriteLockView = _

  // Support for atomic ops
  private val stateAtomic = new AtomicLongLong(
    fromRawPtr(Intrinsics.classFieldRawPtr(this, "state"))
  )
  private val headAtomic = new AtomicRef[Node](
    fromRawPtr(Intrinsics.classFieldRawPtr(this, "head"))
  )
  private val tailAtomic = new AtomicRef[Node](
    fromRawPtr(Intrinsics.classFieldRawPtr(this, "tail"))
  )

  private def casState(expect: Long, update: Long): Boolean =
    stateAtomic.compareExchangeStrong(expect, update)

  private def casTail(c: Node, v: Node): Boolean =
    tailAtomic.compareExchangeStrong(c, v)

  /**
   * Creates a new lock, initially in unlocked state.
   */
  def this() = {
    this()
    state = ORIGIN
  }

  // internal lock methods

  private def tryAcquireWrite(): Long = {
    val s = state
    val nextState = s | WBIT
    if ((s & ABITS) == 0L && casState(s, nextState)) {
      storeStoreFence()
      nextState
    } else 0L
  }

  private def tryAcquireRead(): Long = {
    @tailrec
    def loop(): Long = {
      val s = state
      val m = s & ABITS
      if (m < RFULL) {
        val nextState = s + RUNIT
        if (casState(s, nextState)) nextState
        else loop()
      } else if (m == WBIT) 0L
      else {
        val nextState = tryIncReaderOverflow(s)
        if (nextState != 0L) nextState
        else loop()
      }
    }
    loop()
  }

  /**
   * Returns an unlocked state, incrementing the version and
   * avoiding special failure value 0L.
   *
   * @param s a write-locked state (or stamp)
   */
  private def unlockWriteState(s: Long): Long = {
    val next = s + WBIT
    if (next == 0L) ORIGIN else next
  }

  private def releaseWrite(s: Long): Long = {
    val nextState = unlockWriteState(s)
    state = nextState
    signalNext(head)
    nextState
  }

  /**
   * Exclusively acquires the lock, blocking if necessary
   * until available.
   *
   * @return a write stamp that can be used to unlock or convert mode
   */
  def writeLock(): Long = {
    // try unconditional CAS confirming weak read
    val s = state & ~ABITS
    val nextState = s | WBIT
    if (casState(s, nextState)) {
      storeStoreFence()
      nextState
    } else acquireWrite(interruptible = false, timed = false, time = 0L)
  }

  /**
   * Exclusively acquires the lock if it is immediately available.
   *
   * @return a write stamp that can be used to unlock or convert mode,
   *         or zero if the lock is not available
   */
  def tryWriteLock(): Long = tryAcquireWrite()

  /**
   * Exclusively acquires the lock if it is available within the
   * given time and the current thread has not been interrupted.
   * Behavior under timeout and interruption matches that specified
   * for method `Lock.tryLock(long, TimeUnit)`.
   *
   * @param time the maximum time to wait for the lock
   * @param unit the time unit of the `time` argument
   * @return a write stamp that can be used to unlock or convert mode,
   *         or zero if the lock is not available
   * @throws InterruptedException if the current thread is interrupted
   *                              before acquiring the lock
   */
  @throws[InterruptedException]
  def tryWriteLock(time: Long, unit: TimeUnit): Long = {
    val nanos = unit.toNanos(time)
    if (!Thread.interrupted()) {
      val nextState = tryAcquireWrite()
      if (nextState != 0L) return nextState
      if (nanos <= 0L) return 0L
      val result = acquireWrite(interruptible = true, timed = true, System.nanoTime() + nanos)
      if (result != INTERRUPTED) return result
    }
    throw new InterruptedException()
  }

  /**
   * Exclusively acquires the lock, blocking if necessary
   * until available or the current thread is interrupted.
   * Behavior under interruption matches that specified
   * for method `Lock.lockInterruptibly()`.
   *
   * @return a write stamp that can be used to unlock or convert mode
   * @throws InterruptedException if the current thread is interrupted
   *                              before acquiring the lock
   */
  @throws[InterruptedException]
  def writeLockInterruptibly(): Long = {
    val nextState = tryAcquireWrite()
    if (nextState != 0L) return nextState
    if (Thread.interrupted()) throw new InterruptedException()
    val result = acquireWrite(interruptible = true, timed = false, time = 0L)
    if (result != INTERRUPTED) return result
    throw new InterruptedException()
  }

  /**
   * Non-exclusively acquires the lock, blocking if necessary
   * until available.
   *
   * @return a read stamp that can be used to unlock or convert mode
   */
  def readLock(): Long = {
    // unconditionally optimistically try non-overflow case once
    val s = state & RSAFE
    val nextState = s + RUNIT
    if (casState(s, nextState)) nextState
    else acquireRead(interruptible = false, timed = false, time = 0L)
  }

  /**
   * Non-exclusively acquires the lock if it is immediately available.
   *
   * @return a read stamp that can be used to unlock or convert mode,
   *         or zero if the lock is not available
   */
  def tryReadLock(): Long = tryAcquireRead()

  /**
   * Non-exclusively acquires the lock if it is available within the
   * given time and the current thread has not been interrupted.
   * Behavior under timeout and interruption matches that specified
   * for method `Lock.tryLock(long, TimeUnit)`.
   *
   * @param time the maximum time to wait for the lock
   * @param unit the time unit of the `time` argument
   * @return a read stamp that can be used to unlock or convert mode,
   *         or zero if the lock is not available
   * @throws InterruptedException if the current thread is interrupted
   *                              before acquiring the lock
   */
  @throws[InterruptedException]
  def tryReadLock(time: Long, unit: TimeUnit): Long = {
    val nanos = unit.toNanos(time)
    if (!Thread.interrupted()) {
      if (tail == head) {
        val nextState = tryAcquireRead()
        if (nextState != 0L) return nextState
      }
      if (nanos <= 0L) return 0L
      val result = acquireRead(interruptible = true, timed = true, System.nanoTime() + nanos)
      if (result != INTERRUPTED) return result
    }
    throw new InterruptedException()
  }

  /**
   * Non-exclusively acquires the lock, blocking if necessary
   * until available or the current thread is interrupted.
   * Behavior under interruption matches that specified
   * for method `Lock.lockInterruptibly()`.
   *
   * @return a read stamp that can be used to unlock or convert mode
   * @throws InterruptedException if the current thread is interrupted
   *                              before acquiring the lock
   */
  @throws[InterruptedException]
  def readLockInterruptibly(): Long = {
    val nextState = tryAcquireRead()
    if (nextState != 0L) return nextState
    if (Thread.interrupted()) throw new InterruptedException()
    val result = acquireRead(interruptible = true, timed = false, time = 0L)
    if (result != INTERRUPTED) return result
    throw new InterruptedException()
  }

  /**
   * Returns a stamp that can later be validated, or zero
   * if exclusively locked.
   *
   * @return a valid optimistic read stamp, or zero if exclusively locked
   */
  def tryOptimisticRead(): Long = {
    val s = state
    if ((s & WBIT) == 0L) (s & SBITS) else 0L
  }

  /**
   * Returns true if the lock has not been exclusively acquired
   * since issuance of the given stamp. Always returns false if the
   * stamp is zero. Always returns true if the stamp represents a
   * currently held lock. Invoking this method with a value not
   * obtained from `tryOptimisticRead` or a locking method
   * for this lock has no defined effect or result.
   *
   * @param stamp a stamp
   * @return `true` if the lock has not been exclusively acquired
   *         since issuance of the given stamp; else false
   */
  def validate(stamp: Long): Boolean = {
    loadFence()
    (stamp & SBITS) == (state & SBITS)
  }

  /**
   * If the lock state matches the given stamp, releases the
   * exclusive lock.
   *
   * @param stamp a stamp returned by a write-lock operation
   * @throws IllegalMonitorStateException if the stamp does
   *                                      not match the current state of this lock
   */
  def unlockWrite(stamp: Long): Unit = {
    if (state != stamp || (stamp & WBIT) == 0L)
      throw new IllegalMonitorStateException()
    releaseWrite(stamp)
  }

  /**
   * If the lock state matches the given stamp, releases the
   * non-exclusive lock.
   *
   * @param stamp a stamp returned by a read-lock operation
   * @throws IllegalMonitorStateException if the stamp does
   *                                      not match the current state of this lock
   */
  def unlockRead(stamp: Long): Unit = {
    if ((stamp & RBITS) != 0L) {
      @tailrec
      def loop(): Unit = {
        val s = state
        if ((s & SBITS) == (stamp & SBITS)) {
          val m = s & RBITS
          if (m != 0L) {
            if (m < RFULL) {
              if (casState(s, s - RUNIT)) {
                if (m == RUNIT) signalNext(head)
                return
              } else loop()
            } else if (tryDecReaderOverflow(s) != 0L) return
            else loop()
          }
        }
      }
      loop()
      return
    }
    throw new IllegalMonitorStateException()
  }

  /**
   * If the lock state matches the given stamp, releases the
   * corresponding mode of the lock.
   *
   * @param stamp a stamp returned by a lock operation
   * @throws IllegalMonitorStateException if the stamp does
   *                                      not match the current state of this lock
   */
  def unlock(stamp: Long): Unit = {
    if ((stamp & WBIT) != 0L) unlockWrite(stamp)
    else unlockRead(stamp)
  }

  /**
   * If the lock state matches the given stamp, atomically performs one of
   * the following actions. If the stamp represents holding a write
   * lock, returns it. Or, if a read lock, if the write lock is
   * available, releases the read lock and returns a write stamp.
   * Or, if an optimistic read, returns a write stamp only if
   * immediately available. This method returns zero in all other
   * cases.
   *
   * @param stamp a stamp
   * @return a valid write stamp, or zero on failure
   */
  def tryConvertToWriteLock(stamp: Long): Long = {
    val a = stamp & ABITS
    @tailrec
    def loop(): Long = {
      val s = state
      if ((s & SBITS) == (stamp & SBITS)) {
        val m = s & ABITS
        if (m == 0L) {
          if (a != 0L) 0L
          else {
            val nextState = s | WBIT
            if (casState(s, nextState)) {
              storeStoreFence()
              nextState
            } else loop()
          }
        } else if (m == WBIT) {
          if (a != m) 0L
          else stamp
        } else if (m == RUNIT && a != 0L) {
          val nextState = s - RUNIT + WBIT
          if (casState(s, nextState)) nextState
          else loop()
        } else 0L
      } else 0L
    }
    loop()
  }

  /**
   * If the lock state matches the given stamp, atomically performs one of
   * the following actions. If the stamp represents holding a write
   * lock, releases it and obtains a read lock. Or, if a read lock,
   * returns it. Or, if an optimistic read, acquires a read lock and
   * returns a read stamp only if immediately available. This method
   * returns zero in all other cases.
   *
   * @param stamp a stamp
   * @return a valid read stamp, or zero on failure
   */
  def tryConvertToReadLock(stamp: Long): Long = {
    @tailrec
    def loop(): Long = {
      val s = state
      if ((s & SBITS) == (stamp & SBITS)) {
        val a = stamp & ABITS
        if (a >= WBIT) {
          // write stamp
          if (s != stamp) 0L
          else {
            val nextState = unlockWriteState(s) + RUNIT
            state = nextState
            signalNext(head)
            nextState
          }
        } else if (a == 0L) {
          // optimistic read stamp
          val m = s & ABITS
          if (m < RFULL) {
            val nextState = s + RUNIT
            if (casState(s, nextState)) nextState
            else loop()
          } else {
            val nextState = tryIncReaderOverflow(s)
            if (nextState != 0L) nextState
            else loop()
          }
        } else {
          // already a read stamp
          if ((s & ABITS) == 0L) 0L
          else stamp
        }
      } else 0L
    }
    loop()
  }

  /**
   * If the lock state matches the given stamp then, atomically, if the stamp
   * represents holding a lock, releases it and returns an
   * observation stamp. Or, if an optimistic read, returns it if
   * validated. This method returns zero in all other cases, and so
   * may be useful as a form of "tryUnlock".
   *
   * @param stamp a stamp
   * @return a valid optimistic read stamp, or zero on failure
   */
  def tryConvertToOptimisticRead(stamp: Long): Long = {
    loadFence()
    val a = stamp & ABITS
    @tailrec
    def loop(): Long = {
      val s = state
      if ((s & SBITS) == (stamp & SBITS)) {
        if (a >= WBIT) {
          // write stamp
          if (s != stamp) 0L
          else releaseWrite(s)
        } else if (a == 0L) {
          // already an optimistic read stamp
          stamp
        } else {
          val m = s & ABITS
          if (m == 0L) 0L // invalid read stamp
          else if (m < RFULL) {
            val nextState = s - RUNIT
            if (casState(s, nextState)) {
              if (m == RUNIT) signalNext(head)
              nextState & SBITS
            } else loop()
          } else {
            val nextState = tryDecReaderOverflow(s)
            if (nextState != 0L) nextState & SBITS
            else loop()
          }
        }
      } else 0L
    }
    loop()
  }

  /**
   * Releases the write lock if it is held, without requiring a
   * stamp value. This method may be useful for recovery after
   * errors.
   *
   * @return `true` if the lock was held, else false
   */
  def tryUnlockWrite(): Boolean = {
    val s = state
    if ((s & WBIT) != 0L) {
      releaseWrite(s)
      true
    } else false
  }

  /**
   * Releases one hold of the read lock if it is held, without
   * requiring a stamp value. This method may be useful for recovery
   * after errors.
   *
   * @return `true` if the read lock was held, else false
   */
  def tryUnlockRead(): Boolean = {
    @tailrec
    def loop(): Boolean = {
      val s = state
      val m = s & ABITS
      if (m != 0L && m < WBIT) {
        if (m < RFULL) {
          if (casState(s, s - RUNIT)) {
            if (m == RUNIT) signalNext(head)
            true
          } else loop()
        } else if (tryDecReaderOverflow(s) != 0L) true
        else loop()
      } else false
    }
    loop()
  }

  // status monitoring methods

  /**
   * Returns combined state-held and overflow read count for given
   * state s.
   */
  private def getReadLockCount(s: Long): Int = {
    val readers = s & RBITS
    if (readers >= RFULL) (RFULL + readerOverflow).toInt
    else readers.toInt
  }

  /**
   * Returns `true` if the lock is currently held exclusively.
   *
   * @return `true` if the lock is currently held exclusively
   */
  def isWriteLocked(): Boolean = (state & WBIT) != 0L

  /**
   * Returns `true` if the lock is currently held non-exclusively.
   *
   * @return `true` if the lock is currently held non-exclusively
   */
  def isReadLocked(): Boolean = (state & RBITS) != 0L

  /**
   * Tells whether a stamp represents holding a lock exclusively.
   *
   * @param stamp a stamp returned by a previous StampedLock operation
   * @return `true` if the stamp was returned by a successful
   *         write-lock operation
   * @since 10
   */
  def isWriteLockStamp(stamp: Long): Boolean = (stamp & ABITS) == WBIT

  /**
   * Tells whether a stamp represents holding a lock non-exclusively.
   *
   * @param stamp a stamp returned by a previous StampedLock operation
   * @return `true` if the stamp was returned by a successful
   *         read-lock operation
   * @since 10
   */
  def isReadLockStamp(stamp: Long): Boolean = (stamp & RBITS) != 0L

  /**
   * Tells whether a stamp represents holding a lock.
   *
   * @param stamp a stamp returned by a previous StampedLock operation
   * @return `true` if the stamp was returned by a successful
   *         read-lock or write-lock operation
   * @since 10
   */
  def isLockStamp(stamp: Long): Boolean = (stamp & ABITS) != 0L

  /**
   * Tells whether a stamp represents a successful optimistic read.
   *
   * @param stamp a stamp returned by a previous StampedLock operation
   * @return `true` if the stamp was returned by a successful
   *         optimistic read operation, that is, a non-zero return from
   *         `tryOptimisticRead()` or `tryConvertToOptimisticRead(long)`
   * @since 10
   */
  def isOptimisticReadStamp(stamp: Long): Boolean = (stamp & ABITS) == 0L && stamp != 0L

  /**
   * Queries the number of read locks held for this lock. This
   * method is designed for use in monitoring system state, not for
   * synchronization control.
   * @return the number of read locks held
   */
  def getReadLockCount(): Int = getReadLockCount(state)

  /**
   * Returns a string identifying this lock, as well as its lock
   * state. The state, in brackets, includes the String `"Unlocked"`
   * or the String `"Write-locked"` or the String `"Read-locks:"`
   * followed by the current number of read-locks held.
   *
   * @return a string identifying this lock, as well as its lock state
   */
  override def toString(): String = {
    val s = state
    super.toString() + (
      if ((s & ABITS) == 0L) "[Unlocked]"
      else if ((s & WBIT) != 0L) "[Write-locked]"
      else "[Read-locks:" + getReadLockCount(s) + "]"
    )
  }

  // views

  /**
   * Returns a plain `Lock` view of this StampedLock in which
   * the `Lock.lock` method is mapped to `readLock`,
   * and similarly for other methods. The returned Lock does not
   * support a `Condition`; method `Lock.newCondition()`
   * throws `UnsupportedOperationException`.
   *
   * @return the lock
   */
  def asReadLock(): Lock = {
    val v = readLockView
    if (v != null) v
    else {
      val newView = new ReadLockView()
      readLockView = newView
      newView
    }
  }

  /**
   * Returns a plain `Lock` view of this StampedLock in which
   * the `Lock.lock` method is mapped to `writeLock`,
   * and similarly for other methods. The returned Lock does not
   * support a `Condition`; method `Lock.newCondition()`
   * throws `UnsupportedOperationException`.
   *
   * @return the lock
   */
  def asWriteLock(): Lock = {
    val v = writeLockView
    if (v != null) v
    else {
      val newView = new WriteLockView()
      writeLockView = newView
      newView
    }
  }

  /**
   * Returns a `ReadWriteLock` view of this StampedLock in
   * which the `ReadWriteLock.readLock()` method is mapped to
   * `asReadLock()`, and `ReadWriteLock.writeLock()` to
   * `asWriteLock()`.
   *
   * @return the lock
   */
  def asReadWriteLock(): ReadWriteLock = {
    val v = readWriteLockView
    if (v != null) v
    else {
      val newView = new ReadWriteLockView()
      readWriteLockView = newView
      newView
    }
  }

  // view classes

  final class ReadLockView extends Lock {
    def lock(): Unit = readLock()
    @throws[InterruptedException]
    def lockInterruptibly(): Unit = readLockInterruptibly()
    def tryLock(): Boolean = tryReadLock() != 0L
    @throws[InterruptedException]
    def tryLock(time: Long, unit: TimeUnit): Boolean = tryReadLock(time, unit) != 0L
    def unlock(): Unit = unstampedUnlockRead()
    def newCondition(): Condition = throw new UnsupportedOperationException()
  }

  final class WriteLockView extends Lock {
    def lock(): Unit = writeLock()
    @throws[InterruptedException]
    def lockInterruptibly(): Unit = writeLockInterruptibly()
    def tryLock(): Boolean = tryWriteLock() != 0L
    @throws[InterruptedException]
    def tryLock(time: Long, unit: TimeUnit): Boolean = tryWriteLock(time, unit) != 0L
    def unlock(): Unit = unstampedUnlockWrite()
    def newCondition(): Condition = throw new UnsupportedOperationException()
  }

  final class ReadWriteLockView extends ReadWriteLock {
    def readLock(): Lock = asReadLock()
    def writeLock(): Lock = asWriteLock()
  }

  // Unlock methods without stamp argument checks for view classes.
  // Needed because view-class lock methods throw away stamps.

  private def unstampedUnlockWrite(): Unit = {
    val s = state
    if ((s & WBIT) == 0L)
      throw new IllegalMonitorStateException()
    releaseWrite(s)
  }

  private def unstampedUnlockRead(): Unit = {
    @tailrec
    def loop(): Unit = {
      val s = state
      val m = s & RBITS
      if (m > 0L) {
        if (m < RFULL) {
          if (casState(s, s - RUNIT)) {
            if (m == RUNIT) signalNext(head)
            return
          } else loop()
        } else if (tryDecReaderOverflow(s) != 0L) return
        else loop()
      }
    }
    loop()
    return
    throw new IllegalMonitorStateException()
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(s: ObjectInputStream): Unit = {
    s.defaultReadObject()
    state = ORIGIN // reset to unlocked state
  }

  // overflow handling methods

  /**
   * Tries to increment readerOverflow by first setting state
   * access bits value to RBITS, indicating hold of spinlock,
   * then updating, then releasing.
   *
   * @param s a reader overflow stamp: (s & ABITS) >= RFULL
   * @return new stamp on success, else zero
   */
  private def tryIncReaderOverflow(s: Long): Long = {
    if ((s & ABITS) != RFULL) {
      Thread.onSpinWait()
      0L
    } else if (casState(s, s | RBITS)) {
      readerOverflow += 1
      state = s
      s
    } else 0L
  }

  /**
   * Tries to decrement readerOverflow.
   *
   * @param s a reader overflow stamp: (s & ABITS) >= RFULL
   * @return new stamp on success, else zero
   */
  private def tryDecReaderOverflow(s: Long): Long = {
    if ((s & ABITS) != RFULL) {
      Thread.onSpinWait()
      0L
    } else if (casState(s, s | RBITS)) {
      val r = readerOverflow
      val nextState = if (r > 0) {
        readerOverflow = r - 1
        s
      } else s - RUNIT
      state = nextState
      nextState
    } else 0L
  }

  // release methods

  /**
   * Wakes up the successor of given node, if one exists, and unsets its
   * WAITING status to avoid park race. This may fail to wake up an
   * eligible thread when one or more have been cancelled, but
   * cancelAcquire ensures liveness.
   */
  private def signalNext(h: Node): Unit = {
    if (h != null) {
      val s = h.next
      if (s != null && s.status > 0) {
        s.getAndUnsetStatus(WAITING)
        LockSupport.unpark(s.waiter)
      }
    }
  }

  /**
   * Removes and unparks all cowaiters of node, if it exists.
   */
  private def signalCowaiters(node: ReaderNode): Unit = {
    if (node != null) {
      @tailrec
      def loop(): Unit = {
        val c = node.cowaiters
        if (c != null) {
          if (node.casCowaiters(c, c.cowaiters))
            LockSupport.unpark(c.waiter)
          loop()
        }
      }
      loop()
    }
  }

  // queue link methods

  /** tries once to CAS a new dummy node for head */
  private def tryInitializeHead(h: WriterNode): Unit = {
    if (headAtomic.compareExchangeStrong(null, h))
      tail = h
  }

  /**
   * For explanation, see above and AbstractQueuedSynchronizer
   * internal documentation.
   *
   * @param interruptible true if should check interrupts and if so
   *                      return INTERRUPTED
   * @param timed         if true use timed waits
   * @param time          the System.nanoTime value to timeout at (and return zero)
   * @return next state, or INTERRUPTED
   */
  private def acquireWrite(interruptible: Boolean, timed: Boolean, time: Long): Long = {
    var spins: Byte = 0 // retries upon unpark of first thread
    var postSpins: Byte = 0
    var interrupted = false
    var first = false
    var node: WriterNode = null
    var pred: Node = null
    var nanos: Long = 0L

    while (true) {
      // Check predecessor status
      if (!first && node != null) {
        pred = node.prev
        if (pred != null && !(head == pred)) {
          first = false
          if (pred.status < 0) {
            cleanQueue()
            // continue
          } else if (pred.prev == null) {
            Thread.onSpinWait()
            // continue
          } else {
            // fall through to acquire attempt
          }
        } else {
          if (pred != null && head == pred) first = true
          // fall through
        }
      }

      // Try to acquire
      if ((first || pred == null) && {
            val s = state
            (s & ABITS) == 0L && casState(s, s | WBIT)
          }) {
        storeStoreFence()
        if (first && node != null) {
          node.prev = null
          head = node
          if (pred != null) pred.next = null
          node.waiter = null
          if (interrupted) Thread.currentThread().interrupt()
        }
        return state
      }

      // Create node if needed
      if (node == null) {
        try {
          node = new WriterNode()
        } catch {
          case _: OutOfMemoryError =>
            return spinLockOnOOME(write = true, interruptible, timed, time)
        }
      } else if (pred == null) {
        // try to enqueue
        val t = tail
        node.setPrevRelaxed(t)
        if (t == null) {
          // try to initialize
          try {
            val h = new WriterNode()
            tryInitializeHead(h)
          } catch {
            case _: OutOfMemoryError =>
              return spinLockOnOOME(write = true, interruptible, timed, time)
          }
        } else if (!casTail(t, node)) {
          node.setPrevRelaxed(null) // back out
        } else {
          t.next = node
        }
      } else if (first && spins != 0) {
        // reduce unfairness
        spins = (spins - 1).toByte
        Thread.onSpinWait()
      } else if (node.status == 0) {
        // enable signal
        if (node.waiter == null) node.waiter = Thread.currentThread()
        node.status = WAITING
      } else if (!timed || { nanos = time - System.nanoTime(); nanos > 0L }) {
        try {
          if (!timed) LockSupport.park(this)
          else LockSupport.parkNanos(this, nanos)
        } catch {
          case ex: RuntimeException =>
            cancelAcquire(node)
            throw ex
          case ex: Error =>
            cancelAcquire(node)
            throw ex
        }
        node.clearStatus()
        interrupted |= Thread.interrupted()
        if (interrupted && interruptible) {
          cancelAcquire(node)
          return if (interrupted || Thread.interrupted()) INTERRUPTED else 0L
        }
        postSpins = ((postSpins << 1) | 1).toByte
        spins = postSpins
      } else {
        // timeout
        cancelAcquire(node)
        return if (interrupted || Thread.interrupted()) INTERRUPTED else 0L
      }
    }
    0L // unreachable
  }

  /**
   * See above for explanation.
   *
   * @param interruptible true if should check interrupts and if so
   *                      return INTERRUPTED
   * @param timed         if true use timed waits
   * @param time          the System.nanoTime value to timeout at (and return zero)
   * @return next state, or INTERRUPTED
   */
  private def acquireRead(interruptible: Boolean, timed: Boolean, time: Long): Long = {
    var interrupted = false
    var node: ReaderNode = null

    // Loop:
    //   if empty, try to acquire
    //   if tail is Reader, try to cowait; restart if leader stale or cancels
    //   else try to create and enqueue node, and wait in 2nd loop below
    while (true) {
      val t = tail
      val tailPred = if (t != null) t.prev else null

      if ((t == null || tailPred == null) && {
            val ns = tryAcquireRead()
            ns != 0L
          }) {
        // try now if empty
        if (interrupted) Thread.currentThread().interrupt()
        return tryAcquireRead()
      }

      if (t == null) {
        try {
          val h = new WriterNode()
          tryInitializeHead(h)
        } catch {
          case _: OutOfMemoryError =>
            return spinLockOnOOME(write = false, interruptible, timed, time)
        }
      } else if (tailPred == null || !t.isInstanceOf[ReaderNode]) {
        if (node == null) {
          try {
            node = new ReaderNode()
          } catch {
            case _: OutOfMemoryError =>
              return spinLockOnOOME(write = false, interruptible, timed, time)
          }
        }
        if (tail == t) {
          node.setPrevRelaxed(t)
          if (casTail(t, node)) {
            t.next = node
            // node is leader; wait in loop below
            return waitAsReaderLeader(node, interrupted, interruptible, timed, time)
          }
          node.setPrevRelaxed(null)
        }
      } else {
        // try to cowait
        val leader = t.asInstanceOf[ReaderNode]
        if (leader == tail) {
          val cowaitResult = tryCowait(leader, node, interruptible, timed, time)
          if (cowaitResult.interrupted) interrupted = true
          node = cowaitResult.node
          val ns = tryAcquireRead()
          signalCowaiters(leader)
          if (interrupted) Thread.currentThread().interrupt()
          if (ns != 0L) return ns
          // else restart if stale, missed, or leader cancelled
        }
      }
    }
    0L // unreachable
  }

  /**
   * Try to join a cowait group under the given leader.
   * Returns the updated node and whether interrupted.
   */
  private case class CowaitResult(node: ReaderNode, interrupted: Boolean)

  private def tryCowait(
      leader: ReaderNode,
      node: ReaderNode,
      interruptible: Boolean,
      timed: Boolean,
      time: Long
  ): CowaitResult = {
    var myNode = node
    var attached = false
    var interrupted = false
    var nanos: Long = 0L

    while (true) {
      if (leader.status < 0 || leader.prev == null) {
        if (myNode != null) myNode.waiter = null
        return CowaitResult(myNode, interrupted)
      } else if (myNode == null) {
        try {
          myNode = new ReaderNode()
        } catch {
          case _: OutOfMemoryError =>
            return CowaitResult(null, interrupted)
        }
      } else if (myNode.waiter == null) {
        myNode.waiter = Thread.currentThread()
      } else if (!attached) {
        val c = leader.cowaiters
        myNode.setCowaitersRelaxed(c)
        attached = leader.casCowaiters(c, myNode)
        if (!attached) myNode.setCowaitersRelaxed(null)
      } else {
        nanos = if (timed) time - System.nanoTime() else 0L
        try {
          if (!timed) LockSupport.park(this)
          else if (nanos > 0L) LockSupport.parkNanos(this, nanos)
        } catch {
          case ex: RuntimeException =>
            cancelCowaiter(myNode, leader)
            throw ex
          case ex: Error =>
            cancelCowaiter(myNode, leader)
            throw ex
        }
        interrupted |= Thread.interrupted()
        if ((interrupted && interruptible) || (timed && nanos <= 0L)) {
          cancelCowaiter(myNode, leader)
          return CowaitResult(myNode, interrupted)
        }
      }
    }
    CowaitResult(myNode, interrupted) // unreachable
  }

  /**
   * Wait as the leader of a cowait group; almost same as acquireWrite
   */
  private def waitAsReaderLeader(
      node: ReaderNode,
      interrupted: Boolean,
      interruptible: Boolean,
      timed: Boolean,
      time: Long
  ): Long = {
    var spins: Byte = 0
    var postSpins: Byte = 0
    var first = false
    var pred: Node = null
    var nanos: Long = 0L
    var wasInterrupted = interrupted

    while (true) {
      // Check predecessor status
      if (!first) {
        pred = node.prev
        if (pred != null && !(head == pred)) {
          first = false
          if (pred.status < 0) {
            cleanQueue()
            // continue
          } else if (pred.prev == null) {
            Thread.onSpinWait()
            // continue
          } else {
            // fall through to acquire attempt
          }
        } else {
          if (pred != null && head == pred) first = true
          // fall through
        }
      }

      // Try to acquire read
      if ((first || pred == null) && {
            val ns = tryAcquireRead()
            ns != 0L
          }) {
        val nextState = tryAcquireRead()
        if (first) {
          node.prev = null
          head = node
          if (pred != null) pred.next = null
          node.waiter = null
        }
        signalCowaiters(node)
        if (wasInterrupted) Thread.currentThread().interrupt()
        return nextState
      }

      if (first && spins != 0) {
        spins = (spins - 1).toByte
        Thread.onSpinWait()
      } else if (node.status == 0) {
        if (node.waiter == null) node.waiter = Thread.currentThread()
        node.status = WAITING
      } else if (!timed || { nanos = time - System.nanoTime(); nanos > 0L }) {
        try {
          if (!timed) LockSupport.park(this)
          else LockSupport.parkNanos(this, nanos)
        } catch {
          case ex: RuntimeException =>
            cancelAcquire(node)
            throw ex
          case ex: Error =>
            cancelAcquire(node)
            throw ex
        }
        node.clearStatus()
        wasInterrupted |= Thread.interrupted()
        if (wasInterrupted && interruptible) {
          cancelAcquire(node)
          return if (wasInterrupted || Thread.interrupted()) INTERRUPTED else 0L
        }
        postSpins = ((postSpins << 1) | 1).toByte
        spins = postSpins
      } else {
        // timeout
        cancelAcquire(node)
        return if (wasInterrupted || Thread.interrupted()) INTERRUPTED else 0L
      }
    }
    0L // unreachable
  }

  // Cancellation support

  /**
   * Possibly repeatedly traverses from tail, unsplicing cancelled
   * nodes until none are found. Unparks nodes that may have been
   * relinked to be next eligible acquirer.
   */
  private def cleanQueue(): Unit = {
    while (true) { // restart point
      var s: Node = null
      var q: Node = tail
      while (true) {
        val p = if (q != null) q.prev else null
        if (q == null || p == null) return () // end of list

        val inconsistent =
          if (s == null) tail ne q
          else (s.prev ne q) || s.status < 0

        if (inconsistent) {
          // break to outer loop (restart)
          q = null // force restart
        } else if (q.status < 0) {
          // cancelled
          val casResult =
            if (s == null) casTail(q, p)
            else s.casPrev(q, p)
          if (casResult && (q.prev eq p)) {
            p.casNext(q, s) // OK if fails
            if (p.prev == null) signalNext(p)
          }
        } else {
          val n = p.next
          if (n != q) {
            // help finish
            if (n != null && (q.prev eq p) && q.status >= 0) {
              p.casNext(n, q)
              if (p.prev == null) signalNext(p)
            }
          } else {
            s = q
            q = q.prev
            // continue inner loop
          }
        }
      }
    }
  }

  /**
   * If leader exists, possibly repeatedly traverses cowaiters,
   * unsplicing the given cancelled node until not found.
   */
  private def unlinkCowaiter(node: ReaderNode, leader: ReaderNode): Unit = {
    if (leader != null) {
      while (leader.prev != null && leader.status >= 0) {
        var p: ReaderNode = leader
        while (true) {
          val q = p.cowaiters
          if (q == null) return
          if (q eq node) {
            p.casCowaiters(q, q.cowaiters)
          }
          p = q
        }
      }
    }
  }

  /**
   * If node non-null, forces cancel status and unsplices it from
   * queue, wakes up any cowaiters, and possibly wakes up successor
   * to recheck status.
   *
   * @param node the waiter (may be null if not yet enqueued)
   */
  private def cancelAcquire(node: Node): Unit = {
    if (node != null) {
      node.waiter = null
      node.status = CANCELLED
      cleanQueue()
      node match {
        case rn: ReaderNode => signalCowaiters(rn)
        case _              => ()
      }
    }
  }

  /**
   * If node non-null, forces cancel status and unsplices from
   * leader's cowaiters list unless/until it is also cancelled.
   *
   * @param node   if non-null, the waiter
   * @param leader if non-null, the node heading cowaiters list
   */
  private def cancelCowaiter(node: ReaderNode, leader: ReaderNode): Unit = {
    if (node != null) {
      node.waiter = null
      node.status = CANCELLED
      unlinkCowaiter(node, leader)
    }
  }

  /**
   * Fallback upon encountering OutOfMemoryErrors
   */
  private def spinLockOnOOME(
      write: Boolean,
      interruptible: Boolean,
      timed: Boolean,
      time: Long
  ): Long = {
    val startTime = if (timed) System.nanoTime() else 0L
    var spins = 0
    while (true) {
      val s = if (write) tryAcquireWrite() else tryAcquireRead()
      if (s != 0L) return s
      Thread.onSpinWait()
      spins += 1
      if ((spins & (1 << 8)) == 0) { // occasionally check
        if (interruptible && Thread.interrupted()) return INTERRUPTED
        if (timed && System.nanoTime() - startTime > time) return 0L
      }
    }
    0L // unreachable
  }

  // Fences for memory ordering

  @inline private def storeStoreFence(): Unit =
    scala.scalanative.libc.stdatomic.atomic_thread_fence(memory_order_seq_cst)

  @inline private def loadFence(): Unit =
    scala.scalanative.libc.stdatomic.atomic_thread_fence(memory_order_seq_cst)
}

object StampedLock {

  /** The number of bits to use for reader count before overflowing */
  private val LG_READERS = 7 // 127 readers

  // Values for lock state and stamp operations
  private val RUNIT = 1L
  private val WBIT = 1L << LG_READERS
  private val RBITS = WBIT - 1L
  private val RFULL = RBITS - 1L
  private val ABITS = RBITS | WBIT
  private val SBITS = ~RBITS // note overlap with ABITS
  // not writing and conservatively non-overflowing
  private val RSAFE = ~(3L << (LG_READERS - 1))

  /*
   * 3 stamp modes can be distinguished by examining (m = stamp & ABITS):
   * write mode: m == WBIT
   * optimistic read mode: m == 0L (even when read lock is held)
   * read mode: m > 0L && m <= RFULL (the stamp is a copy of state, but the
   * read hold count in the stamp is unused other than to determine mode)
   *
   * This differs slightly from the encoding of state:
   * (state & ABITS) == 0L indicates the lock is currently unlocked.
   * (state & ABITS) == RBITS is a special transient value
   * indicating spin-locked to manipulate reader bits overflow.
   */

  /** Initial value for lock state; avoids failure value zero. */
  private val ORIGIN = WBIT << 1

  // Special value from cancelled acquire methods so caller can throw IE
  private val INTERRUPTED = 1L

  // Bits for Node.status
  private val WAITING = 1
  private val CANCELLED = 0x80000000 // must be negative

  /** CLH nodes */
  private abstract class Node {
    @volatile var prev: Node = _ // initially attached via casTail
    @volatile var next: Node = _ // visibly nonnull when signallable
    @volatile var waiter: Thread = _ // visibly nonnull when enqueued
    @volatile var status: Int = 0 // written by owner, atomic bit ops by others

    // methods for atomic operations
    private def prevAtomic = new AtomicRef[Node](
      fromRawPtr(Intrinsics.classFieldRawPtr(this, "prev"))
    )
    private def nextAtomic = new AtomicRef[Node](
      fromRawPtr(Intrinsics.classFieldRawPtr(this, "next"))
    )
    private def statusAtomic = new AtomicInt(
      fromRawPtr(Intrinsics.classFieldRawPtr(this, "status"))
    )

    final def casPrev(c: Node, v: Node): Boolean = // for cleanQueue
      prevAtomic.compareExchangeWeak(c, v)

    final def casNext(c: Node, v: Node): Boolean = // for cleanQueue
      nextAtomic.compareExchangeWeak(c, v)

    final def getAndUnsetStatus(v: Int): Int = // for signalling
      statusAtomic.fetchAnd(~v)

    final def setPrevRelaxed(p: Node): Unit = // for off-queue assignment
      prevAtomic.store(p)

    final def setStatusRelaxed(s: Int): Unit = // for off-queue assignment
      statusAtomic.store(s)

    final def clearStatus(): Unit = // for reducing unneeded signals
      statusAtomic.store(0, memory_order_relaxed)
  }

  final class WriterNode extends Node // node for writers

  final class ReaderNode extends Node { // node for readers
    @volatile var cowaiters: ReaderNode = _ // list of linked readers

    final def casCowaiters(c: ReaderNode, v: ReaderNode): Boolean = {
      val cowaitersAtomic = new AtomicRef[ReaderNode](
        fromRawPtr(Intrinsics.classFieldRawPtr(this, "cowaiters"))
      )
      cowaitersAtomic.compareExchangeWeak(c, v)
    }

    final def setCowaitersRelaxed(p: ReaderNode): Unit = {
      val cowaitersAtomic = new AtomicRef[ReaderNode](
        fromRawPtr(Intrinsics.classFieldRawPtr(this, "cowaiters"))
      )
      cowaitersAtomic.store(p)
    }
  }
}

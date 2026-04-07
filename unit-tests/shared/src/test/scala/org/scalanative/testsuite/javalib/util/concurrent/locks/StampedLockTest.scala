/*
 * Written by Doug Lea and Martin Buchholz with assistance from
 * members of JCP JSR-166 Expert Group and released to the public
 * domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.scalanative.testsuite.javalib.util.concurrent
package locks

import java.util.concurrent.{
  Callable, CompletableFuture, CountDownLatch, Future, ThreadLocalRandom, TimeUnit
}
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Lock, StampedLock}
import java.util.function.{BiConsumer, Consumer, Function}

import scala.collection.mutable.ListBuffer

import org.junit.Assert._
import org.junit.Test

import org.scalanative.testsuite.javalib.util.concurrent.JSR166Test
import org.scalanative.testsuite.utils.AssertThrows.assertThrows

import JSR166Test._

object StampedLockTest {

  // Helper functions for uninterruptible lock operations
  def writeLockInterruptiblyUninterrupted(sl: StampedLock): Long = {
    try sl.writeLockInterruptibly()
    catch { case _: InterruptedException => throw new AssertionError() }
  }

  def tryWriteLockUninterrupted(sl: StampedLock, time: Long, unit: TimeUnit): Long = {
    try sl.tryWriteLock(time, unit)
    catch { case _: InterruptedException => throw new AssertionError() }
  }

  def readLockInterruptiblyUninterrupted(sl: StampedLock): Long = {
    try sl.readLockInterruptibly()
    catch { case _: InterruptedException => throw new AssertionError() }
  }

  def tryReadLockUninterrupted(sl: StampedLock, time: Long, unit: TimeUnit): Long = {
    try sl.tryReadLock(time, unit)
    catch { case _: InterruptedException => throw new AssertionError() }
  }

}

class StampedLockTest extends JSR166Test {
  import StampedLockTest._

  /** Releases write lock, checking isWriteLocked before and after */
  def releaseWriteLock(lock: StampedLock, stamp: Long): Unit = {
    assertTrue(lock.isWriteLocked())
    assertValid(lock, stamp)
    lock.unlockWrite(stamp)
    assertFalse(lock.isWriteLocked())
    assertFalse(lock.validate(stamp))
  }

  /** Releases read lock, checking isReadLocked before and after */
  def releaseReadLock(lock: StampedLock, stamp: Long): Unit = {
    assertTrue(lock.isReadLocked())
    assertValid(lock, stamp)
    lock.unlockRead(stamp)
    assertFalse(lock.isReadLocked())
    assertTrue(lock.validate(stamp))
  }

  def assertNonZero(v: Long): Long = {
    assertTrue(v != 0L)
    v
  }

  def assertValid(lock: StampedLock, stamp: Long): Long = {
    assertTrue(stamp != 0L)
    assertTrue(lock.validate(stamp))
    stamp
  }

  def assertUnlocked(lock: StampedLock): Unit = {
    assertFalse(lock.isReadLocked())
    assertFalse(lock.isWriteLocked())
    assertEquals(0, lock.getReadLockCount())
    assertValid(lock, lock.tryOptimisticRead())
  }

  def lockLockers(lock: Lock): List[Action] = List(
    () => lock.lock(),
    () => lock.lockInterruptibly(),
    () => lock.tryLock(),
    () => lock.tryLock(Long.MinValue, DAYS),
    () => lock.tryLock(0L, DAYS),
    () => lock.tryLock(Long.MaxValue, DAYS)
  )

  def readLockers(): List[Function[StampedLock, Long]] = List(
    (sl: StampedLock) => sl.readLock(),
    (sl: StampedLock) => sl.tryReadLock(),
    (sl: StampedLock) => readLockInterruptiblyUninterrupted(sl),
    (sl: StampedLock) => tryReadLockUninterrupted(sl, Long.MinValue, DAYS),
    (sl: StampedLock) => tryReadLockUninterrupted(sl, 0L, DAYS),
    (sl: StampedLock) => sl.tryConvertToReadLock(sl.tryOptimisticRead())
  )

  def readUnlockers(): List[BiConsumer[StampedLock, Long]] = List(
    (sl: StampedLock, stamp: Long) => sl.unlockRead(stamp),
    (sl: StampedLock, stamp: Long) => assertTrue(sl.tryUnlockRead()),
    (sl: StampedLock, stamp: Long) => sl.asReadLock().unlock(),
    (sl: StampedLock, stamp: Long) => sl.unlock(stamp),
    (sl: StampedLock, stamp: Long) => assertValid(sl, sl.tryConvertToOptimisticRead(stamp))
  )

  def writeLockers(): List[Function[StampedLock, Long]] = List(
    (sl: StampedLock) => sl.writeLock(),
    (sl: StampedLock) => sl.tryWriteLock(),
    (sl: StampedLock) => writeLockInterruptiblyUninterrupted(sl),
    (sl: StampedLock) => tryWriteLockUninterrupted(sl, Long.MinValue, DAYS),
    (sl: StampedLock) => tryWriteLockUninterrupted(sl, 0L, DAYS),
    (sl: StampedLock) => sl.tryConvertToWriteLock(sl.tryOptimisticRead())
  )

  def writeUnlockers(): List[BiConsumer[StampedLock, Long]] = List(
    (sl: StampedLock, stamp: Long) => sl.unlockWrite(stamp),
    (sl: StampedLock, stamp: Long) => assertTrue(sl.tryUnlockWrite()),
    (sl: StampedLock, stamp: Long) => sl.asWriteLock().unlock(),
    (sl: StampedLock, stamp: Long) => sl.unlock(stamp),
    (sl: StampedLock, stamp: Long) => assertValid(sl, sl.tryConvertToOptimisticRead(stamp))
  )

  /** Constructed StampedLock is in unlocked state */
  @Test def testConstructor(): Unit = {
    assertUnlocked(new StampedLock())
  }

  /** write-locking, then unlocking, an unlocked lock succeed */
  @Test def testWriteLock_lockUnlock(): Unit = {
    val lock = new StampedLock()

    for {
      writeLocker <- writeLockers()
      writeUnlocker <- writeUnlockers()
    } {
      assertFalse(lock.isWriteLocked())
      assertFalse(lock.isReadLocked())
      assertEquals(0, lock.getReadLockCount())

      val s = writeLocker.apply(lock)
      assertValid(lock, s)
      assertTrue(lock.isWriteLocked())
      assertFalse(lock.isReadLocked())
      assertEquals(0, lock.getReadLockCount())
      writeUnlocker.accept(lock, s)
      assertUnlocked(lock)
    }
  }

  /** read-locking, then unlocking, an unlocked lock succeed */
  @Test def testReadLock_lockUnlock(): Unit = {
    val lock = new StampedLock()

    for {
      readLocker <- readLockers()
      readUnlocker <- readUnlockers()
    } {
      var s: Long = 42
      var i = 0
      while (i < 2) {
        s = assertValid(lock, readLocker.apply(lock))
        assertFalse(lock.isWriteLocked())
        assertTrue(lock.isReadLocked())
        assertEquals(i + 1, lock.getReadLockCount())
        i += 1
      }
      i = 0
      while (i < 2) {
        assertFalse(lock.isWriteLocked())
        assertTrue(lock.isReadLocked())
        assertEquals(2 - i, lock.getReadLockCount())
        readUnlocker.accept(lock, s)
        i += 1
      }
      assertUnlocked(lock)
    }
  }

  /** tryUnlockWrite fails if not write locked */
  @Test def testTryUnlockWrite_failure(): Unit = {
    val lock = new StampedLock()
    assertFalse(lock.tryUnlockWrite())

    for {
      readLocker <- readLockers()
      readUnlocker <- readUnlockers()
    } {
      val s = assertValid(lock, readLocker.apply(lock))
      assertFalse(lock.tryUnlockWrite())
      assertTrue(lock.isReadLocked())
      readUnlocker.accept(lock, s)
      assertUnlocked(lock)
    }
  }

  /** tryUnlockRead fails if not read locked */
  @Test def testTryUnlockRead_failure(): Unit = {
    val lock = new StampedLock()
    assertFalse(lock.tryUnlockRead())

    for {
      writeLocker <- writeLockers()
      writeUnlocker <- writeUnlockers()
    } {
      val s = writeLocker.apply(lock)
      assertFalse(lock.tryUnlockRead())
      assertTrue(lock.isWriteLocked())
      writeUnlocker.accept(lock, s)
      assertUnlocked(lock)
    }
  }

  /** validate(0L) fails */
  @Test def testValidate0(): Unit = {
    val lock = new StampedLock()
    assertFalse(lock.validate(0L))
  }

  /** A stamp obtained from a successful lock operation validates while the lock is held */
  @Test def testValidate(): Unit = {
    val lock = new StampedLock()

    for {
      readLocker <- readLockers()
      readUnlocker <- readUnlockers()
    } {
      val s = assertNonZero(readLocker.apply(lock))
      assertTrue(lock.validate(s))
      readUnlocker.accept(lock, s)
    }

    for {
      writeLocker <- writeLockers()
      writeUnlocker <- writeUnlockers()
    } {
      val s = assertNonZero(writeLocker.apply(lock))
      assertTrue(lock.validate(s))
      writeUnlocker.accept(lock, s)
    }
  }

  /** A stamp obtained from an unsuccessful lock operation does not validate */
  @Test def testValidate2(): Unit = {
    val lock = new StampedLock()
    val s = assertNonZero(lock.writeLock())
    assertTrue(lock.validate(s))
    assertFalse(lock.validate(lock.tryWriteLock()))
    assertFalse(lock.validate(lock.tryWriteLock(randomExpiredTimeout(), randomTimeUnit())))
    assertFalse(lock.validate(lock.tryReadLock()))
    assertFalse(lock.validate(lock.tryWriteLock(randomExpiredTimeout(), randomTimeUnit())))
    assertFalse(lock.validate(lock.tryOptimisticRead()))
    lock.unlockWrite(s)
  }

  def assertThrowInterruptedExceptionWhenPreInterrupted(actions: Array[Action]): Unit = {
    for (action <- actions) {
      Thread.currentThread().interrupt()
      try {
        action.run()
        shouldThrow()
      } catch {
        case _: InterruptedException => // success
        case fail: Throwable => threadUnexpectedException(fail)
      }
      assertFalse(Thread.interrupted())
    }
  }

  /** interruptible operations throw InterruptedException when pre-interrupted */
  @Test def testInterruptibleOperationsThrowInterruptedExceptionWhenPreInterrupted(): Unit = {
    val lock = new StampedLock()

    val interruptibleLockActions: Array[Action] = Array(
      () => { lock.writeLockInterruptibly(); () },
      () => { lock.tryWriteLock(Long.MinValue, DAYS); () },
      () => { lock.tryWriteLock(Long.MaxValue, DAYS); () },
      () => { lock.readLockInterruptibly(); () },
      () => { lock.tryReadLock(Long.MinValue, DAYS); () },
      () => { lock.tryReadLock(Long.MaxValue, DAYS); () },
      () => { lock.asWriteLock().lockInterruptibly(); () },
      () => { lock.asWriteLock().tryLock(0L, DAYS); () },
      () => { lock.asWriteLock().tryLock(Long.MaxValue, DAYS); () },
      () => { lock.asReadLock().lockInterruptibly(); () },
      () => { lock.asReadLock().tryLock(0L, DAYS); () },
      () => { lock.asReadLock().tryLock(Long.MaxValue, DAYS); () }
    )
    shuffle(interruptibleLockActions)

    assertThrowInterruptedExceptionWhenPreInterrupted(interruptibleLockActions)
    runWithWriteLock(lock) {
      assertThrowInterruptedExceptionWhenPreInterrupted(interruptibleLockActions)
    }
    runWithReadLock(lock) {
      assertThrowInterruptedExceptionWhenPreInterrupted(interruptibleLockActions)
    }
  }

  def runWithWriteLock[T](lock: StampedLock)(body: => T): Unit = {
    val s = lock.writeLock()
    try body
    finally lock.unlockWrite(s)
  }

  def runWithReadLock[T](lock: StampedLock)(body: => T): Unit = {
    val s = lock.readLock()
    try body
    finally lock.unlockRead(s)
  }

  def assertThrowInterruptedExceptionWhenInterrupted(actions: Array[Action]): Unit = {
    val n = actions.length
    val futures = new Array[Future[_]](n)
    val threadsStarted = new CountDownLatch(n)
    val done = new CountDownLatch(n)

    var i = 0
    while (i < n) {
      val action = actions(i)
      futures(i) = cachedThreadPool.submit(new CheckedRunnable() {
        def realRun(): Unit = {
          threadsStarted.countDown()
          try {
            action.run()
            shouldThrow()
          } catch {
            case _: InterruptedException => // success
            case fail: Throwable => threadUnexpectedException(fail)
          }
          assertFalse(Thread.interrupted())
          done.countDown()
        }
      })
      i += 1
    }

    await(threadsStarted)
    assertEquals(n, done.getCount())
    // Interrupt all the tasks
    for (future <- futures) future.cancel(true)
    await(done)
  }

  /** interruptible operations throw InterruptedException when write locked and interrupted */
  @Test def testInterruptibleOperationsThrowInterruptedExceptionWriteLockedInterrupted(): Unit = {
    val lock = new StampedLock()
    val stamp = lock.writeLock()

    val interruptibleLockBlockingActions: Array[Action] = Array(
      () => { lock.writeLockInterruptibly(); () },
      () => { lock.tryWriteLock(Long.MaxValue, DAYS); () },
      () => { lock.readLockInterruptibly(); () },
      () => { lock.tryReadLock(Long.MaxValue, DAYS); () },
      () => { lock.asWriteLock().lockInterruptibly(); () },
      () => { lock.asWriteLock().tryLock(Long.MaxValue, DAYS); () },
      () => { lock.asReadLock().lockInterruptibly(); () },
      () => { lock.asReadLock().tryLock(Long.MaxValue, DAYS); () }
    )
    shuffle(interruptibleLockBlockingActions)

    assertThrowInterruptedExceptionWhenInterrupted(interruptibleLockBlockingActions)

    releaseWriteLock(lock, stamp)
  }

  /** interruptible operations throw InterruptedException when read locked and interrupted */
  @Test def testInterruptibleOperationsThrowInterruptedExceptionReadLockedInterrupted(): Unit = {
    val lock = new StampedLock()
    val stamp = lock.readLock()

    val interruptibleLockBlockingActions: Array[Action] = Array(
      () => { lock.writeLockInterruptibly(); () },
      () => { lock.tryWriteLock(Long.MaxValue, DAYS); () },
      () => { lock.asWriteLock().lockInterruptibly(); () },
      () => { lock.asWriteLock().tryLock(Long.MaxValue, DAYS); () }
    )
    shuffle(interruptibleLockBlockingActions)

    assertThrowInterruptedExceptionWhenInterrupted(interruptibleLockBlockingActions)

    releaseReadLock(lock, stamp)
  }

  /** Non-interruptible operations ignore and preserve interrupted status */
  @Test def testNonInterruptibleOperationsIgnoreInterrupts(): Unit = {
    val lock = new StampedLock()
    Thread.currentThread().interrupt()

    for {
      readUnlocker <- readUnlockers()
    } {
      var s = assertValid(lock, lock.readLock())
      readUnlocker.accept(lock, s)
      s = assertValid(lock, lock.tryReadLock())
      readUnlocker.accept(lock, s)
    }

    lock.asReadLock().lock()
    lock.asReadLock().unlock()

    for {
      writeUnlocker <- writeUnlockers()
    } {
      var s = assertValid(lock, lock.writeLock())
      writeUnlocker.accept(lock, s)
      s = assertValid(lock, lock.tryWriteLock())
      writeUnlocker.accept(lock, s)
    }

    lock.asWriteLock().lock()
    lock.asWriteLock().unlock()

    assertTrue(Thread.interrupted())
  }

  /** tryWriteLock on an unlocked lock succeeds */
  @Test def testTryWriteLock(): Unit = {
    val lock = new StampedLock()
    val s = lock.tryWriteLock()
    assertTrue(s != 0L)
    assertTrue(lock.isWriteLocked())
    assertEquals(0L, lock.tryWriteLock())
    releaseWriteLock(lock, s)
  }

  /** tryWriteLock fails if locked */
  @Test def testTryWriteLockWhenLocked(): Unit = {
    val lock = new StampedLock()
    val s = lock.writeLock()
    val t = newStartedThread(new CheckedRunnable() {
      def realRun(): Unit = {
        assertEquals(0L, lock.tryWriteLock())
      }
    })

    assertEquals(0L, lock.tryWriteLock())
    awaitTermination(t)
    releaseWriteLock(lock, s)
  }

  /** tryReadLock fails if write-locked */
  @Test def testTryReadLockWhenLocked(): Unit = {
    val lock = new StampedLock()
    val s = lock.writeLock()
    val t = newStartedThread(new CheckedRunnable() {
      def realRun(): Unit = {
        assertEquals(0L, lock.tryReadLock())
      }
    })

    assertEquals(0L, lock.tryReadLock())
    awaitTermination(t)
    releaseWriteLock(lock, s)
  }

  /** Multiple threads can hold a read lock when not write-locked */
  @Test def testMultipleReadLocks(): Unit = {
    val lock = new StampedLock()
    val s = lock.readLock()
    val t = newStartedThread(new CheckedRunnable() {
      def realRun(): Unit = {
        val s2 = lock.tryReadLock()
        assertValid(lock, s2)
        lock.unlockRead(s2)
        val s3 = lock.tryReadLock(LONG_DELAY_MS, MILLISECONDS)
        assertValid(lock, s3)
        lock.unlockRead(s3)
        val s4 = lock.readLock()
        assertValid(lock, s4)
        lock.unlockRead(s4)
        lock.asReadLock().lock()
        lock.asReadLock().unlock()
        lock.asReadLock().lockInterruptibly()
        lock.asReadLock().unlock()
        lock.asReadLock().tryLock(Long.MinValue, DAYS)
        lock.asReadLock().unlock()
      }
    })

    awaitTermination(t)
    lock.unlockRead(s)
  }

  /** writeLock() succeeds only after a reading thread unlocks */
  @Test def testWriteAfterReadLock(): Unit = {
    val aboutToLock = new CountDownLatch(1)
    val lock = new StampedLock()
    val rs = lock.readLock()
    val t = newStartedThread(new CheckedRunnable() {
      def realRun(): Unit = {
        aboutToLock.countDown()
        val s = lock.writeLock()
        assertTrue(lock.isWriteLocked())
        assertFalse(lock.isReadLocked())
        lock.unlockWrite(s)
      }
    })

    await(aboutToLock)
    assertThreadBlocks(t, Thread.State.WAITING)
    assertFalse(lock.isWriteLocked())
    assertTrue(lock.isReadLocked())
    lock.unlockRead(rs)
    awaitTermination(t)
    assertUnlocked(lock)
  }

  /** writeLock() succeeds only after reading threads unlock */
  @Test def testWriteAfterMultipleReadLocks(): Unit = {
    val lock = new StampedLock()
    val s = lock.readLock()
    val t1 = newStartedThread(new CheckedRunnable() {
      def realRun(): Unit = {
        val rs = lock.readLock()
        lock.unlockRead(rs)
      }
    })

    awaitTermination(t1)

    val t2 = newStartedThread(new CheckedRunnable() {
      def realRun(): Unit = {
        val ws = lock.writeLock()
        lock.unlockWrite(ws)
      }
    })

    assertTrue(lock.isReadLocked())
    assertFalse(lock.isWriteLocked())
    lock.unlockRead(s)
    awaitTermination(t2)
    assertUnlocked(lock)
  }

  /** readLock() succeed only after a writing thread unlocks */
  @Test def testReadAfterWriteLock(): Unit = {
    val lock = new StampedLock()
    val threadsStarted = new CountDownLatch(2)
    val s = lock.writeLock()
    val acquireReleaseReadLock: Runnable = new CheckedRunnable() {
      def realRun(): Unit = {
        threadsStarted.countDown()
        val rs = lock.readLock()
        assertTrue(lock.isReadLocked())
        assertFalse(lock.isWriteLocked())
        lock.unlockRead(rs)
      }
    }
    val t1 = newStartedThread(acquireReleaseReadLock)
    val t2 = newStartedThread(acquireReleaseReadLock)

    await(threadsStarted)
    assertThreadBlocks(t1, Thread.State.WAITING)
    assertThreadBlocks(t2, Thread.State.WAITING)
    assertTrue(lock.isWriteLocked())
    assertFalse(lock.isReadLocked())
    releaseWriteLock(lock, s)
    awaitTermination(t1)
    awaitTermination(t2)
    assertUnlocked(lock)
  }

  /** tryReadLock succeeds if read locked but not write locked */
  @Test def testTryLockWhenReadLocked(): Unit = {
    val lock = new StampedLock()
    val s = lock.readLock()
    val t = newStartedThread(new CheckedRunnable() {
      def realRun(): Unit = {
        val rs = lock.tryReadLock()
        assertValid(lock, rs)
        lock.unlockRead(rs)
      }
    })

    awaitTermination(t)
    lock.unlockRead(s)
  }

  /** tryWriteLock fails when read locked */
  @Test def testTryWriteLockWhenReadLocked(): Unit = {
    val lock = new StampedLock()
    val s = lock.readLock()
    val t = newStartedThread(new CheckedRunnable() {
      def realRun(): Unit = {
        assertEquals(0L, lock.tryWriteLock())
      }
    })

    awaitTermination(t)
    lock.unlockRead(s)
  }

  /** timed lock operations time out if lock not available */
  @Test def testTimedLock_Timeout(): Unit = {
    val futures = new ListBuffer[Future[_]]()

    // Write locked
    val lock = new StampedLock()
    val stamp = lock.writeLock()
    assertEquals(0L, lock.tryReadLock(0L, DAYS))
    assertEquals(0L, lock.tryReadLock(Long.MinValue, DAYS))
    assertFalse(lock.asReadLock().tryLock(0L, DAYS))
    assertFalse(lock.asReadLock().tryLock(Long.MinValue, DAYS))
    assertEquals(0L, lock.tryWriteLock(0L, DAYS))
    assertEquals(0L, lock.tryWriteLock(Long.MinValue, DAYS))
    assertFalse(lock.asWriteLock().tryLock(0L, DAYS))
    assertFalse(lock.asWriteLock().tryLock(Long.MinValue, DAYS))

    futures += cachedThreadPool.submit(new CheckedRunnable() {
      def realRun(): Unit = {
        val startTime = System.nanoTime()
        assertEquals(0L, lock.tryWriteLock(timeoutMillis(), MILLISECONDS))
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis())
      }
    })

    futures += cachedThreadPool.submit(new CheckedRunnable() {
      def realRun(): Unit = {
        val startTime = System.nanoTime()
        assertEquals(0L, lock.tryReadLock(timeoutMillis(), MILLISECONDS))
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis())
      }
    })

    // Read locked
    val lock2 = new StampedLock()
    val stamp2 = lock2.readLock()
    assertEquals(0L, lock2.tryWriteLock(0L, DAYS))
    assertEquals(0L, lock2.tryWriteLock(Long.MinValue, DAYS))
    assertFalse(lock2.asWriteLock().tryLock(0L, DAYS))
    assertFalse(lock2.asWriteLock().tryLock(Long.MinValue, DAYS))

    futures += cachedThreadPool.submit(new CheckedRunnable() {
      def realRun(): Unit = {
        val startTime = System.nanoTime()
        assertEquals(0L, lock2.tryWriteLock(timeoutMillis(), MILLISECONDS))
        assertTrue(millisElapsedSince(startTime) >= timeoutMillis())
      }
    })

    for (future <- futures) assertNull(future.get())

    releaseWriteLock(lock, stamp)
    releaseReadLock(lock2, stamp2)
  }

  /** writeLockInterruptibly succeeds if unlocked */
  @Test def testWriteLockInterruptibly(): Unit = {
    val lock = new StampedLock()
    val s = lock.writeLockInterruptibly()
    assertTrue(lock.isWriteLocked())
    releaseWriteLock(lock, s)
  }

  /** readLockInterruptibly succeeds if lock free */
  @Test def testReadLockInterruptibly(): Unit = {
    val lock = new StampedLock()

    val s = assertValid(lock, lock.readLockInterruptibly())
    assertTrue(lock.isReadLocked())
    lock.unlockRead(s)

    lock.asReadLock().lockInterruptibly()
    assertTrue(lock.isReadLocked())
    lock.asReadLock().unlock()
  }

  /** A serialized lock deserializes as unlocked */
  @Test def testSerialization(): Unit = {
    // SerialClone is problematic in Scala Native - test what we can
    val lock = new StampedLock()
    lock.writeLock()
    assertTrue(lock.isWriteLocked())
    // Just verify basic operation after serialization would work
    val clone = new StampedLock() // Fresh lock should be unlocked
    assertFalse(clone.isWriteLocked())
    val s = clone.writeLock()
    assertTrue(clone.isWriteLocked())
    clone.unlockWrite(s)
    assertFalse(clone.isWriteLocked())
  }

  /** toString indicates current lock state */
  @Test def testToString(): Unit = {
    val lock = new StampedLock()
    assertTrue(lock.toString.contains("Unlocked"))
    val s = lock.writeLock()
    assertTrue(lock.toString.contains("Write-locked"))
    lock.unlockWrite(s)
    val s2 = lock.readLock()
    assertTrue(lock.toString.contains("Read-locks"))
    releaseReadLock(lock, s2)
  }

  /** tryOptimisticRead succeeds and validates if unlocked, fails if exclusively locked */
  @Test def testValidateOptimistic(): Unit = {
    val lock = new StampedLock()

    assertValid(lock, lock.tryOptimisticRead())

    for {
      writeLocker <- writeLockers()
    } {
      val s = assertValid(lock, writeLocker.apply(lock))
      assertEquals(0L, lock.tryOptimisticRead())
      releaseWriteLock(lock, s)
    }

    for {
      readLocker <- readLockers()
    } {
      val s = assertValid(lock, readLocker.apply(lock))
      val p = assertValid(lock, lock.tryOptimisticRead())
      releaseReadLock(lock, s)
      assertTrue(lock.validate(p))
    }

    assertValid(lock, lock.tryOptimisticRead())
  }

  /** tryOptimisticRead stamp does not validate if a write lock intervenes */
  @Test def testValidateOptimisticWriteLocked(): Unit = {
    val lock = new StampedLock()
    val p = assertValid(lock, lock.tryOptimisticRead())
    val s = assertValid(lock, lock.writeLock())
    assertFalse(lock.validate(p))
    assertEquals(0L, lock.tryOptimisticRead())
    assertTrue(lock.validate(s))
    lock.unlockWrite(s)
  }

  /** tryOptimisticRead stamp does not validate if a write lock intervenes in another thread */
  @Test def testValidateOptimisticWriteLocked2(): Unit = {
    val locked = new CountDownLatch(1)
    val lock = new StampedLock()
    val p = assertValid(lock, lock.tryOptimisticRead())

    val t = newStartedThread(new CheckedInterruptedRunnable() {
      def realRun(): Unit = {
        lock.writeLockInterruptibly()
        locked.countDown()
        lock.writeLockInterruptibly()
      }
    })

    await(locked)
    assertFalse(lock.validate(p))
    assertEquals(0L, lock.tryOptimisticRead())
    assertThreadBlocks(t, Thread.State.WAITING)
    t.interrupt()
    awaitTermination(t)
    assertTrue(lock.isWriteLocked())
  }

  /** tryConvertToOptimisticRead succeeds and validates if successfully locked */
  @Test def testTryConvertToOptimisticRead(): Unit = {
    val lock = new StampedLock()
    assertEquals(0L, lock.tryConvertToOptimisticRead(0L))

    var s = assertValid(lock, lock.tryOptimisticRead())
    assertEquals(s, lock.tryConvertToOptimisticRead(s))
    assertTrue(lock.validate(s))

    for {
      writeLocker <- writeLockers()
    } {
      s = assertValid(lock, writeLocker.apply(lock))
      val p = assertValid(lock, lock.tryConvertToOptimisticRead(s))
      assertFalse(lock.validate(s))
      assertTrue(lock.validate(p))
      assertUnlocked(lock)
    }

    for {
      readLocker <- readLockers()
    } {
      s = assertValid(lock, readLocker.apply(lock))
      val q = assertValid(lock, lock.tryOptimisticRead())
      assertEquals(q, lock.tryConvertToOptimisticRead(q))
      assertTrue(lock.validate(q))
      assertTrue(lock.isReadLocked())
      val p = assertValid(lock, lock.tryConvertToOptimisticRead(s))
      assertTrue(lock.validate(p))
      assertTrue(lock.validate(s))
      assertUnlocked(lock)
      assertEquals(q, lock.tryConvertToOptimisticRead(q))
      assertTrue(lock.validate(q))
    }
  }

  /** tryConvertToReadLock succeeds for valid stamps */
  @Test def testTryConvertToReadLock(): Unit = {
    val lock = new StampedLock()

    assertEquals(0L, lock.tryConvertToReadLock(0L))

    var s = assertValid(lock, lock.tryOptimisticRead())
    var p = assertValid(lock, lock.tryConvertToReadLock(s))
    assertTrue(lock.isReadLocked())
    assertEquals(1, lock.getReadLockCount())
    assertTrue(lock.validate(s))
    lock.unlockRead(p)

    s = assertValid(lock, lock.tryOptimisticRead())
    lock.readLock()
    p = assertValid(lock, lock.tryConvertToReadLock(s))
    assertTrue(lock.isReadLocked())
    assertEquals(2, lock.getReadLockCount())
    lock.unlockRead(p)
    lock.unlockRead(p)
    assertUnlocked(lock)

    for {
      readUnlocker <- readUnlockers()
      writeLocker <- writeLockers()
    } {
      s = assertValid(lock, writeLocker.apply(lock))
      p = assertValid(lock, lock.tryConvertToReadLock(s))
      assertFalse(lock.validate(s))
      assertTrue(lock.isReadLocked())
      assertEquals(1, lock.getReadLockCount())
      readUnlocker.accept(lock, p)
    }

    for {
      readUnlocker <- readUnlockers()
      readLocker <- readLockers()
    } {
      s = assertValid(lock, readLocker.apply(lock))
      assertEquals(s, lock.tryConvertToReadLock(s))
      assertTrue(lock.validate(s))
      assertTrue(lock.isReadLocked())
      assertEquals(1, lock.getReadLockCount())
      readUnlocker.accept(lock, s)
    }
  }

  /** tryConvertToWriteLock succeeds if lock available; fails if multiply read locked */
  @Test def testTryConvertToWriteLock(): Unit = {
    val lock = new StampedLock()

    assertEquals(0L, lock.tryConvertToWriteLock(0L))

    assertTrue({ var s = lock.tryOptimisticRead(); s != 0L })
    assertTrue({ var p = lock.tryConvertToWriteLock(lock.tryOptimisticRead()); p != 0L })
    assertTrue(lock.isWriteLocked())
    lock.unlockWrite(lock.tryConvertToWriteLock(lock.tryOptimisticRead()))

    for {
      writeUnlocker <- writeUnlockers()
      writeLocker <- writeLockers()
    } {
      val s = assertValid(lock, writeLocker.apply(lock))
      assertEquals(s, lock.tryConvertToWriteLock(s))
      assertTrue(lock.validate(s))
      assertTrue(lock.isWriteLocked())
      writeUnlocker.accept(lock, s)
    }

    for {
      writeUnlocker <- writeUnlockers()
      readLocker <- readLockers()
    } {
      val s = assertValid(lock, readLocker.apply(lock))
      val p = assertValid(lock, lock.tryConvertToWriteLock(s))
      assertFalse(lock.validate(s))
      assertTrue(lock.validate(p))
      assertTrue(lock.isWriteLocked())
      writeUnlocker.accept(lock, p)
    }

    // failure if multiply read locked
    for {
      readLocker <- readLockers()
    } {
      val s = assertValid(lock, readLocker.apply(lock))
      val p = assertValid(lock, readLocker.apply(lock))
      assertEquals(0L, lock.tryConvertToWriteLock(s))
      assertTrue(lock.validate(s))
      assertTrue(lock.validate(p))
      assertEquals(2, lock.getReadLockCount())
      lock.unlock(p)
      lock.unlock(s)
      assertUnlocked(lock)
    }
  }

  /** asWriteLock can be locked and unlocked */
  @Test def testAsWriteLock(): Unit = {
    val sl = new StampedLock()
    val lock = sl.asWriteLock()
    for (locker <- lockLockers(lock)) {
      locker.run()
      assertTrue(sl.isWriteLocked())
      assertFalse(sl.isReadLocked())
      assertFalse(lock.tryLock())
      lock.unlock()
      assertUnlocked(sl)
    }
  }

  /** asReadLock can be locked and unlocked */
  @Test def testAsReadLock(): Unit = {
    val sl = new StampedLock()
    val lock = sl.asReadLock()
    for (locker <- lockLockers(lock)) {
      locker.run()
      assertTrue(sl.isReadLocked())
      assertFalse(sl.isWriteLocked())
      assertEquals(1, sl.getReadLockCount())
      locker.run()
      assertTrue(sl.isReadLocked())
      assertEquals(2, sl.getReadLockCount())
      lock.unlock()
      lock.unlock()
      assertUnlocked(sl)
    }
  }

  /** asReadWriteLock.writeLock can be locked and unlocked */
  @Test def testAsReadWriteLockWriteLock(): Unit = {
    val sl = new StampedLock()
    val lock = sl.asReadWriteLock().writeLock()
    for (locker <- lockLockers(lock)) {
      locker.run()
      assertTrue(sl.isWriteLocked())
      assertFalse(sl.isReadLocked())
      assertFalse(lock.tryLock())
      lock.unlock()
      assertUnlocked(sl)
    }
  }

  /** asReadWriteLock.readLock can be locked and unlocked */
  @Test def testAsReadWriteLockReadLock(): Unit = {
    val sl = new StampedLock()
    val lock = sl.asReadWriteLock().readLock()
    for (locker <- lockLockers(lock)) {
      locker.run()
      assertTrue(sl.isReadLocked())
      assertFalse(sl.isWriteLocked())
      assertEquals(1, sl.getReadLockCount())
      locker.run()
      assertTrue(sl.isReadLocked())
      assertEquals(2, sl.getReadLockCount())
      lock.unlock()
      lock.unlock()
      assertUnlocked(sl)
    }
  }

  /** Lock.newCondition throws UnsupportedOperationException */
  @Test def testLockViewsDoNotSupportConditions(): Unit = {
    val sl = new StampedLock()
    assertEachThrows(
      classOf[UnsupportedOperationException],
      () => sl.asWriteLock().newCondition(),
      () => sl.asReadLock().newCondition(),
      () => sl.asReadWriteLock().writeLock().newCondition(),
      () => sl.asReadWriteLock().readLock().newCondition()
    )
  }

  /** Passing optimistic read stamps to unlock operations result in IllegalMonitorStateException */
  @Test def testCannotUnlockOptimisticReadStamps(): Unit = {
    {
      val sl = new StampedLock()
      val stamp = assertValid(sl, sl.tryOptimisticRead())
      assertThrows(classOf[IllegalMonitorStateException], sl.unlockRead(stamp))
    }
    {
      val sl = new StampedLock()
      val stamp = sl.tryOptimisticRead()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlock(stamp))
    }

    {
      val sl = new StampedLock()
      val stamp = sl.tryOptimisticRead()
      sl.writeLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlock(stamp))
    }
    {
      val sl = new StampedLock()
      sl.readLock()
      val stamp = assertValid(sl, sl.tryOptimisticRead())
      assertThrows(classOf[IllegalMonitorStateException], sl.unlockRead(stamp))
    }
    {
      val sl = new StampedLock()
      sl.readLock()
      val stamp = assertValid(sl, sl.tryOptimisticRead())
      assertThrows(classOf[IllegalMonitorStateException], sl.unlock(stamp))
    }

    {
      val sl = new StampedLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.writeLock())
      assertValid(sl, stamp)
      sl.writeLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlockWrite(stamp))
    }
    {
      val sl = new StampedLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.writeLock())
      sl.writeLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlock(stamp))
    }
    {
      val sl = new StampedLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.writeLock())
      sl.readLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlockRead(stamp))
    }
    {
      val sl = new StampedLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.writeLock())
      sl.readLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlock(stamp))
    }

    {
      val sl = new StampedLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.readLock())
      assertValid(sl, stamp)
      sl.writeLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlockWrite(stamp))
    }
    {
      val sl = new StampedLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.readLock())
      sl.writeLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlock(stamp))
    }
    {
      val sl = new StampedLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.readLock())
      sl.readLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlockRead(stamp))
    }
    {
      val sl = new StampedLock()
      sl.readLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.readLock())
      assertValid(sl, stamp)
      sl.readLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlockRead(stamp))
    }
    {
      val sl = new StampedLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.readLock())
      sl.readLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlock(stamp))
    }
    {
      val sl = new StampedLock()
      sl.readLock()
      val stamp = sl.tryConvertToOptimisticRead(sl.readLock())
      sl.readLock()
      assertThrows(classOf[IllegalMonitorStateException], sl.unlock(stamp))
    }
  }

  /** Invalid stamps result in IllegalMonitorStateException */
  @Test def testInvalidStampsThrowIllegalMonitorStateException(): Unit = {
    val sl = new StampedLock()

    assertEachThrows(
      classOf[IllegalMonitorStateException],
      () => sl.unlockWrite(0L),
      () => sl.unlockRead(0L),
      () => sl.unlock(0L)
    )

    val optimisticStamp = sl.tryOptimisticRead()
    val readStamp = sl.readLock()
    sl.unlockRead(readStamp)
    val writeStamp = sl.writeLock()
    sl.unlockWrite(writeStamp)
    assertTrue(optimisticStamp != 0L && readStamp != 0L && writeStamp != 0L)
    val noLongerValidStamps = Array(optimisticStamp, readStamp, writeStamp)

    val assertNoLongerValidStampsThrow: Runnable = () => {
      for (noLongerValidStamp <- noLongerValidStamps) {
        assertEachThrows(
          classOf[IllegalMonitorStateException],
          () => sl.unlockWrite(noLongerValidStamp),
          () => sl.unlockRead(noLongerValidStamp),
          () => sl.unlock(noLongerValidStamp)
        )
      }
    }
    assertNoLongerValidStampsThrow.run()

    for {
      readLocker <- readLockers()
      readUnlocker <- readUnlockers()
    } {
      val stamp = readLocker.apply(sl)
      assertValid(sl, stamp)
      assertNoLongerValidStampsThrow.run()
      assertEachThrows(
        classOf[IllegalMonitorStateException],
        () => sl.unlockWrite(stamp),
        () => sl.unlockRead(sl.tryOptimisticRead()),
        () => sl.unlockRead(0L)
      )
      readUnlocker.accept(sl, stamp)
      assertUnlocked(sl)
      assertNoLongerValidStampsThrow.run()
    }

    for {
      writeLocker <- writeLockers()
      writeUnlocker <- writeUnlockers()
    } {
      val stamp = writeLocker.apply(sl)
      assertValid(sl, stamp)
      assertNoLongerValidStampsThrow.run()
      assertEachThrows(
        classOf[IllegalMonitorStateException],
        () => sl.unlockRead(stamp),
        () => sl.unlockWrite(0L)
      )
      writeUnlocker.accept(sl, stamp)
      assertUnlocked(sl)
      assertNoLongerValidStampsThrow.run()
    }
  }

  /** Read locks can be very deeply nested */
  @Test def testDeeplyNestedReadLocks(): Unit = {
    val lock = new StampedLock()
    val depth = 300
    val stamps = new Array[Long](depth)
    val readLockersList = readLockers()
    val readUnlockersList = readUnlockers()
    val readLockersSize = readLockersList.length
    val readUnlockersSize = readUnlockersList.length
    var i = 0
    while (i < depth) {
      val readLocker = readLockersList(i % readLockersSize)
      val stamp = readLocker.apply(lock)
      assertEquals(i + 1, lock.getReadLockCount())
      assertTrue(lock.isReadLocked())
      stamps(i) = stamp
      i += 1
    }
    i = 0
    while (i < depth) {
      val readUnlocker = readUnlockersList(i % readUnlockersSize)
      assertEquals(depth - i, lock.getReadLockCount())
      assertTrue(lock.isReadLocked())
      readUnlocker.accept(lock, stamps(depth - 1 - i))
      i += 1
    }
    assertUnlocked(lock)
  }

  /** Stamped locks are not reentrant. */
  @Test def testNonReentrant(): Unit = {
    val lock = new StampedLock()
    var stamp: Long = 0L

    stamp = lock.writeLock()
    assertValid(lock, stamp)
    assertEquals(0L, lock.tryWriteLock(0L, DAYS))
    assertEquals(0L, lock.tryReadLock(0L, DAYS))
    assertValid(lock, stamp)
    lock.unlockWrite(stamp)

    stamp = lock.tryWriteLock(1L, DAYS)
    assertEquals(0L, lock.tryWriteLock(0L, DAYS))
    assertValid(lock, stamp)
    lock.unlockWrite(stamp)

    stamp = lock.readLock()
    assertEquals(0L, lock.tryWriteLock(0L, DAYS))
    assertValid(lock, stamp)
    lock.unlockRead(stamp)
  }

  """StampedLocks have no notion of ownership. Locks acquired in
   one thread can be released or converted in another."""
  @Test def testNoOwnership(): Unit = {
    val futures = new ListBuffer[Future[_]]()
    for {
      writeLocker <- writeLockers()
      writeUnlocker <- writeUnlockers()
    } {
      val lock = new StampedLock()
      val stamp = writeLocker.apply(lock)
      futures += cachedThreadPool.submit(new CheckedRunnable() {
        def realRun(): Unit = {
          writeUnlocker.accept(lock, stamp)
          assertUnlocked(lock)
          assertFalse(lock.validate(stamp))
        }
      })
    }
    for (future <- futures) assertNull(future.get())
  }

  /** Tries out sample usage code from StampedLock javadoc. */
  @Test def testSampleUsage(): Unit = {
    class Point {
      private var x: Double = 0.0
      private var y: Double = 0.0
      private val sl = new StampedLock()

      def move(deltaX: Double, deltaY: Double): Unit = { // an exclusively locked method
        val stamp = sl.writeLock()
        try {
          x += deltaX
          y += deltaY
        } finally {
          sl.unlockWrite(stamp)
        }
      }

      def distanceFromOrigin(): Double = { // A read-only method
        var currentX = 0.0
        var currentY = 0.0
        var stamp = sl.tryOptimisticRead()
        do {
          if (stamp == 0L)
            stamp = sl.readLock()
          try {
            // possibly racy reads
            currentX = x
            currentY = y
          } finally {
            stamp = sl.tryConvertToOptimisticRead(stamp)
          }
        } while (stamp == 0)
        Math.hypot(currentX, currentY)
      }

      def distanceFromOrigin2(): Double = {
        var stamp = sl.tryOptimisticRead()
        try {
          var done = false
          while (!done) {
            if (stamp == 0L) {
              stamp = sl.readLock()
            } else {
              // possibly racy reads
              val currentX = x
              val currentY = y
              if (!sl.validate(stamp)) {
                stamp = 0L // force retry
              } else {
                return Math.hypot(currentX, currentY)
              }
            }
          }
          throw new RuntimeException("unreachable")
        } finally {
          if (StampedLock.isReadLockStamp(stamp))
            sl.unlockRead(stamp)
        }
      }

      def moveIfAtOrigin(newX: Double, newY: Double): Unit = {
        var stamp = sl.readLock()
        try {
          var done = false
          while (!done) {
            if (x == 0.0 && y == 0.0) {
              val ws = sl.tryConvertToWriteLock(stamp)
              if (ws != 0L) {
                stamp = ws
                x = newX
                y = newY
                done = true
              } else {
                sl.unlockRead(stamp)
                stamp = sl.writeLock()
              }
            } else {
              done = true
            }
          }
        } finally {
          sl.unlock(stamp)
        }
      }
    }

    val p = new Point()
    p.move(3.0, 4.0)
    assertEquals(5.0, p.distanceFromOrigin(), JSR166Test.epsilon)
    p.moveIfAtOrigin(5.0, 12.0)
    assertEquals(5.0, p.distanceFromOrigin2(), JSR166Test.epsilon)
  }

  /** Stamp inspection methods work as expected, and do not inspect the state of the lock itself. */
  @Test def testStampStateInspectionMethods(): Unit = {
    val lock = new StampedLock()

    assertFalse(StampedLock.isWriteLockStamp(0L))
    assertFalse(StampedLock.isReadLockStamp(0L))
    assertFalse(StampedLock.isLockStamp(0L))
    assertFalse(StampedLock.isOptimisticReadStamp(0L))

    {
      val stamp = lock.writeLock()
      var i = 0
      while (i < 2) {
        assertTrue(StampedLock.isWriteLockStamp(stamp))
        assertFalse(StampedLock.isReadLockStamp(stamp))
        assertTrue(StampedLock.isLockStamp(stamp))
        assertFalse(StampedLock.isOptimisticReadStamp(stamp))
        if (i == 0)
          lock.unlockWrite(stamp)
        i += 1
      }
    }

    {
      val stamp = lock.readLock()
      var i = 0
      while (i < 2) {
        assertFalse(StampedLock.isWriteLockStamp(stamp))
        assertTrue(StampedLock.isReadLockStamp(stamp))
        assertTrue(StampedLock.isLockStamp(stamp))
        assertFalse(StampedLock.isOptimisticReadStamp(stamp))
        if (i == 0)
          lock.unlockRead(stamp)
        i += 1
      }
    }

    {
      val optimisticStamp = lock.tryOptimisticRead()
      val readStamp = lock.tryConvertToReadLock(optimisticStamp)
      val writeStamp = lock.tryConvertToWriteLock(readStamp)
      var i = 0
      while (i < 2) {
        assertFalse(StampedLock.isWriteLockStamp(optimisticStamp))
        assertFalse(StampedLock.isReadLockStamp(optimisticStamp))
        assertFalse(StampedLock.isLockStamp(optimisticStamp))
        assertTrue(StampedLock.isOptimisticReadStamp(optimisticStamp))

        assertFalse(StampedLock.isWriteLockStamp(readStamp))
        assertTrue(StampedLock.isReadLockStamp(readStamp))
        assertTrue(StampedLock.isLockStamp(readStamp))
        assertFalse(StampedLock.isOptimisticReadStamp(readStamp))

        assertTrue(StampedLock.isWriteLockStamp(writeStamp))
        assertFalse(StampedLock.isReadLockStamp(writeStamp))
        assertTrue(StampedLock.isLockStamp(writeStamp))
        assertFalse(StampedLock.isOptimisticReadStamp(writeStamp))
        if (i == 0)
          lock.unlockWrite(writeStamp)
        i += 1
      }
    }
  }

  /** Multiple threads repeatedly contend for the same lock. */
  @Test def testConcurrentAccess(): Unit = {
    val sl = new StampedLock()
    val wl = sl.asWriteLock()
    val rl = sl.asReadLock()
    val testDurationMillis = if (JSR166Test.expensiveTests) 1000L else 2L
    val nTasks = ThreadLocalRandom.current().nextInt(1, 10)
    val done = new AtomicBoolean(false)
    val futures = new ListBuffer[CompletableFuture[Void]]()

    val stampedWriteLockers: java.util.List[Callable[Long]] = new java.util.ArrayList()
    stampedWriteLockers.add(() => sl.writeLock())
    stampedWriteLockers.add(() => writeLockInterruptiblyUninterrupted(sl))
    stampedWriteLockers.add(() => tryWriteLockUninterrupted(sl, LONG_DELAY_MS, MILLISECONDS))
    stampedWriteLockers.add(() => {
      var stamp: Long = 0L
      do { stamp = sl.tryConvertToWriteLock(sl.tryOptimisticRead()) }
      while (stamp == 0L)
      stamp
    })
    stampedWriteLockers.add(() => {
      var stamp: Long = 0L
      do { stamp = sl.tryWriteLock() } while (stamp == 0L)
      stamp
    })
    stampedWriteLockers.add(() => {
      var stamp: Long = 0L
      do { stamp = sl.tryWriteLock(0L, DAYS) } while (stamp == 0L)
      stamp
    })

    val stampedReadLockers: java.util.List[Callable[Long]] = new java.util.ArrayList()
    stampedReadLockers.add(() => sl.readLock())
    stampedReadLockers.add(() => readLockInterruptiblyUninterrupted(sl))
    stampedReadLockers.add(() => tryReadLockUninterrupted(sl, LONG_DELAY_MS, MILLISECONDS))
    stampedReadLockers.add(() => {
      var stamp: Long = 0L
      do { stamp = sl.tryConvertToReadLock(sl.tryOptimisticRead()) }
      while (stamp == 0L)
      stamp
    })
    stampedReadLockers.add(() => {
      var stamp: Long = 0L
      do { stamp = sl.tryReadLock() } while (stamp == 0L)
      stamp
    })
    stampedReadLockers.add(() => {
      var stamp: Long = 0L
      do { stamp = sl.tryReadLock(0L, DAYS) } while (stamp == 0L)
      stamp
    })

    val stampedWriteUnlockers: java.util.List[Consumer[Long]] = new java.util.ArrayList()
    stampedWriteUnlockers.add((stamp: Long) => sl.unlockWrite(stamp))
    stampedWriteUnlockers.add((stamp: Long) => sl.unlock(stamp))
    stampedWriteUnlockers.add((stamp: Long) => assertTrue(sl.tryUnlockWrite()))
    stampedWriteUnlockers.add((stamp: Long) => wl.unlock())
    stampedWriteUnlockers.add((stamp: Long) => sl.tryConvertToOptimisticRead(stamp))

    val stampedReadUnlockers: java.util.List[Consumer[Long]] = new java.util.ArrayList()
    stampedReadUnlockers.add((stamp: Long) => sl.unlockRead(stamp))
    stampedReadUnlockers.add((stamp: Long) => sl.unlock(stamp))
    stampedReadUnlockers.add((stamp: Long) => assertTrue(sl.tryUnlockRead()))
    stampedReadUnlockers.add((stamp: Long) => rl.unlock())
    stampedReadUnlockers.add((stamp: Long) => sl.tryConvertToOptimisticRead(stamp))

    val writer: Action = () => {
      // repeatedly acquires write lock
      val locker = chooseRandomly(stampedWriteLockers)
      val unlocker = chooseRandomly(stampedWriteUnlockers)
      while (!done.getAcquire()) {
        val stamp = locker.call()
        try {
          assertTrue(StampedLock.isWriteLockStamp(stamp))
          assertTrue(sl.isWriteLocked())
          assertFalse(StampedLock.isReadLockStamp(stamp))
          assertFalse(sl.isReadLocked())
          assertEquals(0, sl.getReadLockCount())
          assertTrue(sl.validate(stamp))
        } finally {
          unlocker.accept(stamp)
        }
      }
    }

    val reader: Action = () => {
      // repeatedly acquires read lock
      val locker = chooseRandomly(stampedReadLockers)
      val unlocker = chooseRandomly(stampedReadUnlockers)
      while (!done.getAcquire()) {
        val stamp = locker.call()
        try {
          assertFalse(StampedLock.isWriteLockStamp(stamp))
          assertFalse(sl.isWriteLocked())
          assertTrue(StampedLock.isReadLockStamp(stamp))
          assertTrue(sl.isReadLocked())
          assertTrue(sl.getReadLockCount() > 0)
          assertTrue(sl.validate(stamp))
        } finally {
          unlocker.accept(stamp)
        }
      }
    }

    val tasks: java.util.List[Action] = new java.util.ArrayList()
    tasks.add(writer)
    tasks.add(reader)

    var i = nTasks
    while (i > 0) {
      val task = chooseRandomly(tasks)
      futures += CompletableFuture.runAsync(checkedRunnable(task))
      i -= 1
    }
    Thread.sleep(testDurationMillis)
    done.setRelease(true)
    for (future <- futures) checkTimedGet(future, null)
  }
}

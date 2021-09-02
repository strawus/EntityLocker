import java.lang.Thread.currentThread
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newSingleThreadScheduledExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantLock

class EntityLocker<K> (private val escalationThreshold: Int) {
  private val locks = ConcurrentHashMap<K, WaitersAwareReentrantLock>()
  private val globalLock = ReentrantLock()
  private val lockedCount = ThreadLocal.withInitial { 0 }
  private val timeoutScheduler = newSingleThreadScheduledExecutor()
  private val heldKeys = ConcurrentHashMap<Thread, MutableSet<K>>().withDefault { HashSet() }

  fun <V> withLock(key: K, timeout: Long, unit: TimeUnit, protected: () -> V): V {
    withTimeout(timeout, unit) {
      lockInterruptibly(key)
      return try {
        protected()
      } finally {
        unlock(key)
      }
    }
  }

  private fun lockInterruptibly(key: K) {
    val lock = locks.computeIfAbsent(key) { WaitersAwareReentrantLock() }
    if (lock.isLocked) detectDeadlock(key)

    val currentThreadFirstEnter = !lock.isHeldByCurrentThread

    lock.lockInterruptibly()

    heldKeys.getValue(currentThread()).add(key)

    if (currentThreadFirstEnter) tryEscalate()
  }

  private fun tryEscalate() {
    lockedCount.get().inc()
    if (lockedCount.get() >= escalationThreshold) {
      globalLock.lockInterruptibly()
    }
  }

  private fun unlock(key: K) {
    locks[key]?.let { lock ->
      if (globalLock.isHeldByCurrentThread) globalLock.unlock()
      lockedCount.get().dec()
      heldKeys.getValue(currentThread()).remove(key)
      lock.unlock()
    }
  }

  private fun detectDeadlock(key: K) {
    val heldKeys = heldKeys.getValue(currentThread())
    if (key in heldKeys) return
    heldKeys.forEach { detectDeadlock(key, it) }
  }

  private fun detectDeadlock(baseKey: K, heldKey: K) {
    locks[heldKey]?.waiters()?.forEach { waitingThread ->
      val heldKeys = heldKeys.getValue(waitingThread)
      if (baseKey in heldKeys) throw DeadlockException()
      heldKeys.forEach { detectDeadlock(baseKey, it) }
    }
  }

  private inline fun <T> withTimeout(timeout: Long, unit: TimeUnit, block: () -> T): T {
    val currentThread = currentThread()
    val interrupter = timeoutScheduler.schedule({ currentThread.interrupt() }, timeout, unit)

    return try {
      block().also { interrupter.cancel(false) }
    } catch (e: InterruptedException) {
      throw TimeoutException()
    }
  }

  private class WaitersAwareReentrantLock: ReentrantLock() {
    fun waiters(): Collection<Thread> = super.getQueuedThreads()
  }
}

class DeadlockException: Exception()
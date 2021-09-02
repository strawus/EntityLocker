import java.lang.Thread.currentThread
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newSingleThreadScheduledExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantLock

class EntityLocker<K> (private val escalationThreshold: Int) {
  private val locks = ConcurrentHashMap<K, ReentrantLock>()
  private val globalLock = ReentrantLock()
  private val lockedCount = ThreadLocal.withInitial { 0 }
  private val timeoutScheduler = newSingleThreadScheduledExecutor()
  private val holdingThreads = ConcurrentHashMap<K, Thread>()
  private val waitingKeys = HashMap<Thread, MutableSet<K>>().withDefault { HashSet() }

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

  private fun lockInterruptibly(key: K) = synchronized(this) {
    val lock = locks.computeIfAbsent(key) { ReentrantLock() }
    if (lock.isLocked) detectDeadlock(key)

    waitingKeys.getValue(currentThread()).add(key)

    val currentThreadFirstEnter = !lock.isHeldByCurrentThread

    lock.lockInterruptibly()

    waitingKeys.getValue(currentThread()).remove(key)
    holdingThreads[key] = currentThread()

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
      holdingThreads.remove(key)
      lock.unlock()
    }
  }

  private fun detectDeadlock(key: K) {
    holdingThreads[key]?.let { holdingThread ->
      if (holdingThread == currentThread()) return

      waitingKeys[holdingThread]?.forEach { waitingKey ->
        if (holdingThreads[waitingKey] == currentThread()) throw DeadlockException()
        detectDeadlock(waitingKey)
      }
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
}

class DeadlockException: Exception()
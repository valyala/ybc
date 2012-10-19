"""Hash lock simulator.

Simulates concurrent random access to multiple locks from multiple CPUs.
Prints out concurrency level for each (cpus_count, locks_count) pair.

The model is built using the following assumptions:
- Each CPU tries acquiring a random lock at a time in the inifinite loop.
- If the lock is already acquired by another CPU, then the given CPU waits
  until the lock is released.
- CPU performs real work only after the lock is successfully acquired.
"""
from collections import deque
import random

# The maximum numer of CPUs to simulate
MAX_CPUS_COUNT = 64

# The maximum number of locks to simulate
MAX_LOCKS_COUNT = 256

# The number of iterations to simulate. More iterations increase
# the resulting accuracy.
ITERATIONS_COUNT = 1000 * 20


def GetConcurrency(iterations_count, cpus_count, locks_count):
  """Returns concurrency level for the given number of CPUs and locks.

  Args:
    iterations_count: The number of iterations to perform. More iterations
        increase the accuracy of the result.
    cpus_count: The number of concurrent CPUs.
    locks_count: The number of locks.

  Returns:
    concurrency level in the range (0.0 ... 1.0].
    1.0 means perfect concurrency, i.e. all the cpus_count CPUs may be busy
    with real work. They spend zero time waiting for contended locks.
    0.5 means CPUs spend half of their time waiting for contended locks.
    0.0 means CPUs don't do any real work. Instead, they spend all the time
    waiting for contented locks.
  """
  work = 0
  wait_queues = [deque() for i in range(locks_count)]
  cpu_ids = [i for i in range(cpus_count)]
  for i in xrange(iterations_count):
    for cpu_id in cpu_ids:
      lock_id = random.randint(0, locks_count - 1)
      wait_queues[lock_id].append(cpu_id)

    cpu_ids = []
    for q in wait_queues:
      if len(q):
        cpu_id = q.popleft()
        cpu_ids.append(cpu_id)
    work += len(cpu_ids)

  return float(work) / iterations_count / cpus_count


cpus_count = 1
while cpus_count <= MAX_CPUS_COUNT:
  locks_count = 1
  while locks_count <= MAX_LOCKS_COUNT:
    concurrency = GetConcurrency(ITERATIONS_COUNT, cpus_count, locks_count)
    print("cpus_count=%d, locks_count=%d, concurrency=%.2f%%" % (
        cpus_count, locks_count, concurrency * 100))
    locks_count *= 2
  cpus_count *= 2

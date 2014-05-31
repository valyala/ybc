""" Estimates eviction rates for a cache model depending on cache fill ratio.

The model is built using the following rules.
- Items are hashed into buckets using a hash function with uniform output
  distribution.
- Each bucket can contain up to SLOTS_PER_BUCKET items (slots).
- If the given bucket is full (i.e. it already contains SLOTS_PER_BUCKET
  items), then all item additions into this bucket lead to another item's
  eviction from the bucket.
"""

import random

# The maximum number of slots per bucket.
SLOTS_PER_BUCKET = 16

def GetEvictionRate(buckets, slots_to_add):
  """ Returns eviction rate when filling additional slots_to_add in buckets.

  Simulates random items addition into the given list of buckets.
  Calculates an average eviction rate while filling slots_to_add new slots
  in the given list of buckets. Each bucket in the list can contain
  up to SLOTS_PER_BUCKET filled slots. An item addition leads to eviction
  if the given bucket already contains SLOTS_PER_BUCKET filled slots
  (i.e. it is full).

  Args:
    buckets: A list of buckets. Buckets may contain already filled slots.
    slots_to_add: The number of new slots, which must be filled while estimating
        eviction rate.

  Returns:
    A floating point number n in the range [0..1), where 0 means 'no evictions',
    while n -> 1 means 100% evictions.
  """

  evictions_count = 0
  requests_count = 0
  slots_added = 0
  buckets_count = len(buckets)

  # Make sure buckets contain at least slots_to_add empty slots.
  # Otherwise the loop below will never break.
  assert sum(buckets) <= buckets_count * SLOTS_PER_BUCKET - slots_to_add

  while slots_added < slots_to_add:
    requests_count += 1
    bucket_num = int(random.random() * buckets_count)
    if buckets[bucket_num] < SLOTS_PER_BUCKET:
      buckets[bucket_num] += 1
      slots_added += 1
    else:
      evictions_count += 1

  return float(evictions_count) / requests_count


def PrintEvictionRates(steps_count, buckets_per_step):
  """ Prints eviction rates for steps_count fill ratios for the cache model.

  Args:
    steps_count: The number of cache fill ratio steps. Fill ratios are uniformly
        distributed between 0% and 100%.
    buckets_per_step: The number of buckets, which must be filled during each
        step. Higher values result in more precise eviction rates' estimations.
  """
  buckets_count = steps_count * buckets_per_step
  buckets = [0 for i in range(buckets_count)]
  slots_count = buckets_count * SLOTS_PER_BUCKET
  slots_per_step = buckets_per_step * SLOTS_PER_BUCKET

  for i in range(steps_count):
    eviction_rate = GetEvictionRate(buckets, slots_per_step)
    fill_ratio = float(i + 0.5) / steps_count
    print 'fill_ratio=%.4f%%, eviction_rate=%.4f%%' % (
        fill_ratio * 100, eviction_rate * 100)


PrintEvictionRates(25, 10000)

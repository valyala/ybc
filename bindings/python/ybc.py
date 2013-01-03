import ctypes
import os

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_ybc = ctypes.cdll.LoadLibrary(_CURRENT_DIR + "/libybc-release.so")

class Error(Exception): pass

class OpenFailedError(Error): pass
class NoSpaceError(Error): pass
class ItemTooLargeError(Error): pass
class CacheMissError(Error): pass


class Config(object):
  _BUF_SIZE = _ybc.ybc_config_get_size()

  def __init__(self):
    config_buf = ctypes.create_string_buffer(self._BUF_SIZE)
    _ybc.ybc_config_init(config_buf)
    self._buf = config_buf

  def __del__(self):
    if hasattr(self, '_buf'):
      _ybc.ybc_config_destroy(self._buf)

  def set_max_items_count(self, max_items_count):
    max_items_count = ctypes.c_size_t(max_items_count)
    _ybc.ybc_config_set_max_items_count(self._buf, max_items_count)

  def set_data_file_size(self, file_size):
    file_size = ctypes.c_size_t(file_size)
    _ybc.ybc_config_set_data_file_size(self._buf, file_size)

  def set_index_file(self, index_file):
    index_file = str(index_file)
    _ybc.ybc_config_set_index_file(self._buf, index_file)

  def set_data_file(self, data_file):
    data_file = str(data_file)
    _ybc.ybc_config_set_data_file(self._buf, data_file)

  def set_hot_items_count(self, hot_items_count):
    hot_items_count = ctypes.c_size_t(hot_items_count)
    _ybc.ybc_config_set_hot_items_count(self._buf, hot_items_count)

  def set_hot_data_size(self, hot_data_size):
    hot_data_size = ctypes.c_size_t(hot_data_size)
    _ybc.ybc_config_set_hot_data_size(self._buf, hot_data_size)

  def set_de_hashtable_size(self, de_hashtable_size):
    de_hashtable_size = ctypes.c_size_t(de_hashtable_size)
    _ybc.ybc_config_set_de_hashtable_size(self._buf, de_hashtable_size)

  def set_sync_interval(self, sync_interval):
    """Sets data sync interval.

    Args:
      sync_interval: sync interval in milliseconds.
    """
    sync_interval = ctypes.c_uint64(sync_interval)
    _ybc.ybc_config_set_sync_interval(self._buf, sync_interval)

  def open_cache(self, force):
    return _Cache(self._buf, force)

  def open_tiny_cache(self, max_item_size, force):
    _ybc.ybc_config_disable_overwrite_protection(self._buf)
    return _TinyCache(self._buf, max_item_size, force)

  def remove_cache(self):
    _ybc.ybc_remove(self._buf)


class _Key(ctypes.Structure):
  _fields_ = (
      ("ptr", ctypes.c_void_p),
      ("size", ctypes.c_size_t),
  )

  @staticmethod
  def create(s):
    v = _Key()
    v.ptr = ctypes.cast(s, ctypes.c_void_p)
    v.size = len(s)
    return v


class _Value(ctypes.Structure):
  _fields_ = (
      ("ptr", ctypes.c_void_p),
      ("size", ctypes.c_size_t),
      ("ttl", ctypes.c_uint64),
  )

  @staticmethod
  def create(s, ttl):
    v = _Value()
    v.ptr = ctypes.cast(s, ctypes.c_void_p)
    v.size = len(s)
    v.ttl = ttl
    return v


class _Item(object):
  _BUF_SIZE = _ybc.ybc_item_get_size()


class _TinyCache(object):
  def __init__(self, config_buf, max_item_size, force):
    self._cache = _Cache(config_buf, force)
    self._max_item_size = max_item_size

  def clear(self):
    self._cache.clear()

  def set(self, key, value, ttl=(1<<62)):
    if len(value) > self._max_item_size:
      raise ItemTooLargeError

    key = _Key.create(key)
    value = _Value.create(value, ttl)
    if not _ybc.ybc_tiny_set(self._cache._buf, ctypes.byref(key), ctypes.byref(value)):
      raise NoSpaceError

  def get(self, key):
    key = _Key.create(key)
    value = _Value()
    buf = ctypes.create_string_buffer(self._max_item_size)
    value.ptr = ctypes.cast(buf, ctypes.c_void_p)
    value.size = self._max_item_size
    if _ybc.ybc_tiny_get(self._cache._buf, ctypes.byref(key), ctypes.byref(value)) != 1:
      raise CacheMissError
    return buf.raw[:value.size]

  def remove(self, key):
    return self._cache.remove(key)


class _Cache(object):
  _BUF_SIZE = _ybc.ybc_get_size()

  def __init__(self, config_buf, force):
    force = int(force)
    cache_buf = ctypes.create_string_buffer(self._BUF_SIZE)
    if not _ybc.ybc_open(cache_buf, config_buf, force):
      raise OpenFailedError
    self._buf = cache_buf

  def __del__(self):
    if hasattr(self, '_buf'):
      _ybc.ybc_close(self._buf)

  def clear(self):
    _ybc.ybc_clear(self._buf)

  def set(self, key, value, ttl=(1<<62)):
    key = _Key.create(key)
    value = _Value.create(value, ttl)
    if not _ybc.ybc_item_set(self._buf, ctypes.byref(key), ctypes.byref(value)):
      raise NoSpaceError

  def get(self, key):
    key = _Key.create(key)
    item_buf = ctypes.create_string_buffer(_Item._BUF_SIZE)
    if not _ybc.ybc_item_get(self._buf, item_buf, ctypes.byref(key)):
      raise CacheMissError
    value = _Value()
    _ybc.ybc_item_get_value(item_buf, ctypes.byref(value))
    value = ctypes.create_string_buffer(value.ptr, value.size).raw
    _ybc.ybc_item_release(item_buf)
    return value

  def get_de(self, key, grace_ttl):
    key = _Key.create(key)
    grace_ttl = ctypes.c_uint64(grace_ttl)
    item_buf = ctypes.create_string_buffer(_Item._BUF_SIZE)
    if not _ybc.ybc_item_get_de(self._buf, item_buf, ctypes.byref(key),
        grace_ttl):
      raise CacheMissError
    value = _Value()
    _ybc.ybc_item_get_value(item_buf, ctypes.byref(value))
    value = ctypes.create_string_buffer(value.ptr, value.size).raw
    _ybc.ybc_item_release(item_buf)
    return value

  def remove(self, key):
    key = _Key.create(key)
    return (_ybc.ybc_item_remove(self._buf, ctypes.byref(key)) == 1)


def f():
  c = Config()
  c.set_max_items_count(10000)
  c.set_data_file_size(100*1000)
  c.set_index_file("foobar.index")
  c.set_data_file("foobar.data")
  c.set_hot_items_count(100)
  c.set_hot_data_size(1000)
  c.set_de_hashtable_size(100)
  c.set_sync_interval(10 * 1000)
  cache = c.open_tiny_cache(20, True)
  cache.clear()

  for i in range(1000 * 1000):
    cache.set("key_%d" % i, "value_%d" % i)

  cache.set("key", "value")
  v = cache.get("key")
  print "get(): v=[%s], len=%d" % (v, len(v))
#  v = cache.get_de("key", 1000)
#  print "get_de(): v=[%s], len=%d" % (v, len(v))
  if not cache.remove("key"):
    print "cannot remove existing item"

#  for i in range(10):
#    try:
#      print "get_de(%d)" % i
#      cache.get_de("key", 100)
#    except CacheMissError:
#      pass

  del cache

  c.remove_cache()

f()
print 'done'

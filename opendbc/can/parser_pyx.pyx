# distutils: language = c++
# cython: c_string_encoding=ascii, language_level=3
from cython.operator cimport dereference as deref, preincrement as preinc
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport uint32_t
from libcpp.map cimport map
from libcpp.set cimport set

from .common cimport CANParser as cpp_CANParser
from .common cimport dbc_lookup, DBC, CanData, CanFrame, SignalValue

import numbers
from collections import defaultdict

cdef class CANParser:
  cdef:
    cpp_CANParser *can
    const DBC *dbc

  cdef readonly:
    dict vl
    dict vl_all
    dict ts_nanos
    string dbc_name

  def __init__(self, dbc_name, messages, bus=0):
    self.dbc_name = dbc_name
    self.dbc = dbc_lookup(dbc_name)
    if not self.dbc:
      raise RuntimeError(f"Can't find DBC: {dbc_name}")

    # Convert message names into addresses and check existence in DBC
    cdef vector[pair[uint32_t, int]] message_v
    cdef set[uint32_t] addresses

    for i in range(len(messages)):
      c = messages[i]
      try:
        m = self.dbc.addr_to_msg.at(c[0]) if isinstance(c[0], numbers.Number) else self.dbc.name_to_msg.at(c[0])
        addresses.insert(m.address)
        message_v.push_back((m.address, c[1]))
      except IndexError:
        raise RuntimeError(f"could not find message {repr(c[0])} in DBC {self.dbc_name}")

    self.can = new cpp_CANParser(bus, dbc_name, message_v)

    # Populate value dictionaries
    self._update(addresses)
    # map name to address
    for address in addresses:
      name = self.dbc.addr_to_msg.at(address).name.decode("utf8")
      self.vl[name] = self.vl[address]
      self.vl_all[name] = self.vl_all[address]
      self.ts_nanos[name] = self.ts_nanos[address]

  def __dealloc__(self):
    if self.can:
      del self.can

  def update_strings(self, strings, sendcan=False):
    # input format:
    # [nanos, [[address, data, src], ...]]
    # [[nanos, [[address, data, src], ...], ...]]

    cdef CanFrame* frame
    cdef CanData* can_data
    cdef vector[CanData] can_data_array

    try:
      if len(strings) and not isinstance(strings[0], (list, tuple)):
        strings = [strings]

      can_data_array.reserve(len(strings))
      for s in strings:
        can_data = &(can_data_array.emplace_back())
        can_data.nanos = s[0]
        can_data.frames.reserve(len(s[1]))
        for f in s[1]:
          frame = &(can_data.frames.emplace_back())
          frame.address = f[0]
          frame.dat = f[1]
          frame.src = f[2]
    except TypeError:
      raise RuntimeError("invalid parameter")

    update_addresses = self.can.update(can_data_array, sendcan)
    self._update(update_addresses)
    return update_addresses

  cdef _update(self, set[uint32_t] &addresses):
    cdef map[string, SignalValue].iterator val_it
    for address in addresses:
      vl = self.vl.setdefault(address, {})
      vl_all = self.vl_all.setdefault(address, {})
      ts_nanos = self.ts_nanos.setdefault(address, {})
      state = &(self.can.message_states.at(address).values)
      val_it = state.begin()
      while val_it != state.end():
        name = <unicode>deref(val_it).first
        value = &deref(val_it).second
        vl[name] = value.value
        vl_all[name] = value.all_values
        ts_nanos[name] = value.ts_nanos
        preinc(val_it)

  @property
  def can_valid(self):
    return self.can.can_valid

  @property
  def bus_timeout(self):
    return self.can.bus_timeout


cdef class CANDefine():
  cdef:
    const DBC *dbc

  cdef public:
    dict dv
    string dbc_name

  def __init__(self, dbc_name):
    self.dbc_name = dbc_name
    self.dbc = dbc_lookup(dbc_name)
    if not self.dbc:
      raise RuntimeError(f"Can't find DBC: '{dbc_name}'")

    dv = defaultdict(dict)

    for i in range(self.dbc[0].vals.size()):
      val = self.dbc[0].vals[i]

      sgname = val.name.decode("utf8")
      def_val = val.def_val.decode("utf8")
      address = val.address
      try:
        m = self.dbc.addr_to_msg.at(address)
      except IndexError:
        raise KeyError(address)
      msgname = m.name.decode("utf-8")

      # separate definition/value pairs
      def_val = def_val.split()
      values = [int(v) for v in def_val[::2]]
      defs = def_val[1::2]

      # two ways to lookup: address or msg name
      dv[address][sgname] = dict(zip(values, defs))
      dv[msgname][sgname] = dv[address][sgname]

    self.dv = dict(dv)

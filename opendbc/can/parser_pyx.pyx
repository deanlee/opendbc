# distutils: language = c++
# cython: c_string_encoding=ascii, language_level=3

from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport uint32_t, uint8_t

from .common cimport CANParser as cpp_CANParser
from .common cimport dbc_lookup, Msg, DBC, CanData

import numbers
import numpy as np
cimport numpy as np
from collections import defaultdict


cdef class CANParser:
  cdef:
    cpp_CANParser *can
    const DBC *dbc
    set addresses

  cdef readonly:
    dict vl
    dict vl_all
    dict ts_nanos
    string dbc_name
    uint32_t bus

  def __init__(self, dbc_name, messages, bus=0):
    self.dbc_name = dbc_name
    self.bus = bus
    self.dbc = dbc_lookup(dbc_name)
    if not self.dbc:
      raise RuntimeError(f"Can't find DBC: {dbc_name}")

    self.vl = {}
    self.vl_all = {}
    self.ts_nanos = {}
    self.addresses = set()

    # Convert message names into addresses and check existence in DBC
    cdef vector[pair[uint32_t, int]] message_v
    for i in range(len(messages)):
      c = messages[i]
      try:
        m = self.dbc.addr_to_msg.at(c[0]) if isinstance(c[0], numbers.Number) else self.dbc.name_to_msg.at(c[0])
      except IndexError:
        raise RuntimeError(f"could not find message {repr(c[0])} in DBC {self.dbc_name}")

      address = m.address
      message_v.push_back((address, c[1]))
      self.addresses.add(address)

      name = m.name.decode("utf8")
      signal_names = [sig.name.decode("utf-8") for sig in (<Msg*>m).sigs]

      self.vl[address] = {name: 0.0 for name in signal_names}
      self.vl[name] = self.vl[address]
      self.vl_all[address] = defaultdict(list)
      self.vl_all[name] = self.vl_all[address]
      self.ts_nanos[address] = {name: 0.0 for name in signal_names}
      self.ts_nanos[name] = self.ts_nanos[address]

    self.can = new cpp_CANParser(bus, dbc_name, message_v)

  def __dealloc__(self):
    if self.can:
      del self.can

  def update_strings(self, np.ndarray[uint8_t, ndim=1, mode="c"] byte_array, sendcan=False):
    # input format:
    # [nanos, [[address, data, src], ...]]
    # [[nanos, [[address, data, src], ...], ...]]
    for address in self.addresses:
      self.vl_all[address].clear()

    cdef size_t size = byte_array.shape[0]
    if (size == 0) return {}

    cdef uint8_t[::1] arr_memview = byte_array
    cdef set updated_addrs = self.can.update(&arr_memview[0], size)

    for addr in updated_addrs:
      vl = self.vl[addr]
      vl_all = self.vl_all[addr]
      ts_nanos = self.ts_nanos[addr]

      state = self.can.getMessageState(addr)
      for i in range(state.parse_sigs.size()):
        name = <unicode>state.parse_sigs[i].name
        vl[name] = state.vals[i]
        vl_all[name] = state.all_vals[i]
        ts_nanos[name] = state.last_seen_nanos

    return updated_addrs

  def update_from_list(self, can_list, sendcan=False):
    if len(can_list) and not isinstance(can_list[0], (list, tuple)):
      can_list = [can_list]

    flat_data = np.empty((0,), dtype=np.uint8)  # Initialize an empty 1D numpy array
    for s in can_list:
      nanos = int(s[0])
      for address, dat, src in s[1]:
        current_entry = np.empty(sizeof(CanData), dtype=np.uint8)
        current_entry[0:8] = np.frombuffer(nanos.to_bytes(8, 'little'), dtype=np.uint8)  # nanos (uint64_t)
        current_entry[8:12] = np.frombuffer(src.to_bytes(4, 'little'), dtype=np.uint8)  # src (uint32_t)
        current_entry[12:16] = np.frombuffer(address.to_bytes(4, 'little'), dtype=np.uint8)  # address (uint32_t)
        current_entry[16] = len(dat)  # len (uint8_t)
        dat = (dat + bytes(64))[:64]
        current_entry[17:81] = np.frombuffer(dat, dtype=np.uint8)  # dat (uint8_t array, 64 bytes)
        current_entry[81:] = 0

        flat_data = np.concatenate((flat_data, current_entry))

    return self.update_strings(flat_data, sendcan)

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

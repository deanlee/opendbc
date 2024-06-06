# distutils: language = c++
# cython: c_string_encoding=ascii, language_level=3

from libcpp.pair cimport pair
from libcpp.map cimport map
from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport uint32_t

from .common cimport CANParser as cpp_CANParser
from .common cimport dbc_lookup, DBC, Msg

import numbers
from collections import defaultdict
from collections.abc import Mapping


class ValueDict(Mapping):
  def __init__(self, signal_names, get_value_func):
    self.signal_names = signal_names
    self.get_value_func = get_value_func

  def __getitem__(self, key):
    return self.get_value_func(key)

  def __iter__(self):
    return iter(self.signal_names)

  def __len__(self):
    return len(self.signal_names)

  def __repr__(self):
    return repr(dict(self.items()))


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

    self.vl = {}
    self.vl_all = {}
    self.ts_nanos = {}
    msg_name_to_address = {}
    cdef map[uint32_t, Msg] address_to_msg

    for i in range(self.dbc[0].msgs.size()):
      msg = self.dbc[0].msgs[i]
      name = msg.name.decode("utf8")
      msg_name_to_address[name] = msg.address
      address_to_msg[msg.address] = msg

    # Convert message names into addresses and check existence in DBC
    cdef vector[pair[uint32_t, int]] message_v
    for i in range(len(messages)):
      c = messages[i]
      address = c[0] if isinstance(c[0], numbers.Number) else msg_name_to_address.get(c[0])
      if address is None or address_to_msg.count(address) == 0:
        raise RuntimeError(f"could not find message {repr(c[0])} in DBC {self.dbc_name}")
      message_v.push_back((address, c[1]))

    self.can = new cpp_CANParser(bus, dbc_name, message_v)

    for address, v in message_v:
      msg = address_to_msg[address]
      name = msg.name.decode("utf8")
      names = [sig.name.decode("utf8") for sig in msg.sigs]

      self.vl[address] = ValueDict(names, lambda name, addr=address: self.can.getValue(addr, name).value)
      self.vl_all[address] = ValueDict(names, lambda name, addr=address: self.can.getValue(addr, name).all_values)
      self.ts_nanos[address] = ValueDict(names, lambda name, addr=address: self.can.getValue(addr, name).ts_nanos)

      self.vl[name] = self.vl[address]
      self.vl_all[name] = self.vl_all[address]
      self.ts_nanos[name] = self.ts_nanos[address]

  def __dealloc__(self):
    if self.can:
      del self.can

  def update_strings(self, strings, sendcan=False):
    return self.can.update_strings(strings, sendcan)

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

    address_to_msg_name = {}

    for i in range(self.dbc[0].msgs.size()):
      msg = self.dbc[0].msgs[i]
      name = msg.name.decode("utf8")
      address = msg.address
      address_to_msg_name[address] = name

    dv = defaultdict(dict)

    for i in range(self.dbc[0].vals.size()):
      val = self.dbc[0].vals[i]

      sgname = val.name.decode("utf8")
      def_val = val.def_val.decode("utf8")
      address = val.address
      msgname = address_to_msg_name[address]

      # separate definition/value pairs
      def_val = def_val.split()
      values = [int(v) for v in def_val[::2]]
      defs = def_val[1::2]

      # two ways to lookup: address or msg name
      dv[address][sgname] = dict(zip(values, defs))
      dv[msgname][sgname] = dv[address][sgname]

    self.dv = dict(dv)

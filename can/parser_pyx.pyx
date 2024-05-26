# distutils: language = c++
# cython: c_string_encoding=ascii, language_level=3

from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport uint32_t

from .common cimport CANParser as cpp_CANParser
from .common cimport dbc_lookup, SignalValue, DBC

import numbers
from collections import defaultdict


cdef class CANParser:
  cdef:
    cpp_CANParser *can
    const DBC *dbc
    dict msg_name_to_address
    dict address_to_msg_name

  cdef readonly:
    string dbc_name

  def vl(self, msg_name_or_address, signal_name):
    return self.get_signal_value(msg_name_or_address, signal_name).value

  def vl_all(self, msg_name_or_address, signal_name):
    return self.get_signal_value(msg_name_or_address, signal_name).all_values

  def ts_nanos(self, msg_name_or_address, signal_name):
    return self.get_signal_value(msg_name_or_address, signal_name).ts_nanos

  cdef const SignalValue *get_signal_value(self, msg_name_or_address, signal_name):
    cdef uint32_t address = (msg_name_or_address if isinstance(msg_name_or_address, numbers.Number)
                             else self.msg_name_to_address.get(msg_name_or_address))
    if address not in self.address_to_msg_name:
      raise RuntimeError(f"could not find message {repr(msg_name_or_address)} in DBC {self.dbc_name}")
    cdef const SignalValue *v = &(self.can.getSignalValue(address, signal_name))
    return v

  def __init__(self, dbc_name, messages, bus=0):
    self.dbc_name = dbc_name
    self.dbc = dbc_lookup(dbc_name)
    if not self.dbc:
      raise RuntimeError(f"Can't find DBC: {dbc_name}")

    self.msg_name_to_address = {}
    self.address_to_msg_name = {}

    for i in range(self.dbc[0].msgs.size()):
      msg = self.dbc[0].msgs[i]
      name = msg.name.decode("utf8")

      self.msg_name_to_address[name] = msg.address
      self.address_to_msg_name[msg.address] = name

    # Convert message names into addresses and check existence in DBC
    cdef vector[pair[uint32_t, int]] message_v
    for i in range(len(messages)):
      c = messages[i]
      address = c[0] if isinstance(c[0], numbers.Number) else self.msg_name_to_address.get(c[0])
      if address not in self.address_to_msg_name:
        raise RuntimeError(f"could not find message {repr(c[0])} in DBC {self.dbc_name}")
      message_v.push_back((address, c[1]))

    self.can = new cpp_CANParser(bus, dbc_name, message_v)

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

# distutils: language = c++
# cython: c_string_encoding=ascii, language_level=3

from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector
from libc.stdint cimport uint32_t

from .common cimport CANParser as cpp_CANParser
from .common cimport dbc_lookup, Msg, DBC, CanData, MessageState

import numbers
from collections import defaultdict
#from collections.abc import Mapping

# readonly dict
cdef class ReadOnlyDict:
  cdef MessageState *state
  cdef int value_index
  cdef list _signal_names

  def __init__(self):
    pass

  def __iter__(self):
      # Return an iterator over the keys
      return iter(self._signal_names)

  def __len__(self):
      # Return the number of items in the dictionary
      return len(self._signal_names)

  def __repr__(self):
      # Return a string representation of the dictionary
      return f"ReadOnlyDict"

  cdef create(MessageState *state, index):
    value_dict = ReadOnlyDict()
    value_dict.state = state
    value_dict.value_index = index
    value_dict._signal_names = [sig.name.decode("utf-8") for sig in state.parse_sigs]
    return value_dict


cdef class ValueDict(ReadOnlyDict):
  def __getitem__(self, key):
    return self.state.vals[self.index]

cdef class AllValueDict(ReadOnlyDict):
  def __getitem__(self, key):
    return self.state.all_vals[self.index]


cdef class TimeValueDict(ReadOnlyDict):
  def __getitem__(self, key):
    return self.state.last_seen_nanos

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


    self.can = new cpp_CANParser(bus, dbc_name, message_v)

    # Populate dictionaries with ValueDict
    for address, _ in message_v:
      m = self.dbc.addr_to_msg.at(address)
      name = m.name.decode("utf8")
      state = MessageState.create(self.can.messageState(address))
      ValueDict value
      value.create(state, 1)



  def __dealloc__(self):
    if self.can:
      del self.can

  def update_strings(self, strings, sendcan=False):
    # input format:
    # [nanos, [[address, data, src], ...]]
    # [[nanos, [[address, data, src], ...], ...]]
    for address in self.addresses:
      self.vl_all[address].clear()

    cdef vector[CanData] can_data_array

    try:
      if len(strings) and not isinstance(strings[0], (list, tuple)):
        strings = [strings]

      can_data_array.reserve(len(strings))
      for s in strings:
        can_data = &(can_data_array.emplace_back())
        can_data.nanos = s[0]
        can_data.frames.reserve(len(s[1]))
        for address, dat, src in s[1]:
          source_bus = <uint32_t>src
          if source_bus == self.bus:
            frame = &(can_data.frames.emplace_back())
            frame.address = address
            frame.dat = dat
            frame.src = source_bus
    except TypeError:
      raise RuntimeError("invalid parameter")

    return self.can.update(can_data_array)

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

#include <cassert>
#include <algorithm>
#include <cmath>

#include "opendbc/can/common.h"


void set_value(std::vector<uint8_t> &msg, const Signal &sig, int64_t ival) {
  int i = sig.lsb / 8;
  int bits = sig.size;
  if (sig.size < 64) {
    ival &= ((1ULL << sig.size) - 1);
  }

  while (i >= 0 && i < msg.size() && bits > 0) {
    int shift = (int)(sig.lsb / 8) == i ? sig.lsb % 8 : 0;
    int size = std::min(bits, 8 - shift);

    msg[i] &= ~(((1ULL << size) - 1) << shift);
    msg[i] |= (ival & ((1ULL << size) - 1)) << shift;

    bits -= size;
    ival >>= size;
    i = sig.is_little_endian ? i+1 : i-1;
  }
}

CANPacker::CANPacker(const std::string& dbc_name) {
  dbc = dbc_lookup(dbc_name);
  assert(dbc);

  for (const auto& msg : dbc->msgs) {
    message_lookup[msg.address] = msg;
  }
  init_crc_lookup_tables();
}

std::vector<uint8_t> CANPacker::pack(uint32_t address, const std::map<std::string, double> &values) {
  const auto &msg = message_lookup.at(address);
  // set all values for all given signal/value pairs
  std::vector<uint8_t> ret(msg.size, 0);
  bool counter_set = false;
  const Signal *counter_sig = nullptr;
  const Signal *checksum_sig = nullptr;

  for (const auto &sig : msg.sigs) {
    auto value_it = values.find(sig.name);
    double v = (value_it != values.end()) ? value_it->second : 0.0;
    int64_t ival = (int64_t)(round((v - sig.offset) / sig.factor));
    if (ival < 0) {
      ival = (1ULL << sig.size) + ival;
    }
    if (ival != 0) {
      set_value(ret, sig, ival);
    }

    if (sig.name == "CHECKSUM") {
      checksum_sig = &sig;
    } else if (sig.name == "COUNTER" && value_it != values.end()) {
      counter_sig = &sig;
      counter_set = true;
      counters[address] = v;
    }
  }

  // set message counter
  if (counter_sig && !counter_set) {
    auto &cnt = counters[address];
    set_value(ret, *counter_sig, cnt);
    cnt = (cnt + 1) % (1 << counter_sig->size);
  }

  // set message checksum
  if (checksum_sig && checksum_sig->calc_checksum != nullptr) {
    unsigned int checksum = checksum_sig->calc_checksum(address, *checksum_sig, ret);
    set_value(ret, *checksum_sig, checksum);
  }

  return ret;
}

// This function has a definition in common.h and is used in PlotJuggler
Msg* CANPacker::lookup_message(uint32_t address) {
  return &message_lookup[address];
}

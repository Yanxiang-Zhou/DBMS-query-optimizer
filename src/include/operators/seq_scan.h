#pragma once

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>
#include <tuple>
#include "common/macros.h"
#include "heap/heap_file.h"
#include "log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "common/macros.h"
#include "buffer/buffer_manager.h"

namespace buzzdb {
namespace operators {

enum class PredicateType {
    EQ,  // a == b
    NE,  // a != b
    LT,  // a < b
    LE,  // a <= b
    GT,  // a > b
    GE   // a >= b
  };


class SeqScan {
 private:
    uint16_t _table_id;
    std::vector<int> _tuple;
    HeapSegment* _heap_segment;
    BufferManager* _buffer_manager;
    uint64_t _curr_segment, _num_pages, _curr_slot, _num_fields;

 public:
  SeqScan(uint16_t table_id, uint64_t num_pages, uint64_t num_fields);

  ~SeqScan();

  /// Initializes the operator.
  void open();
  
  /// Tries to generate the next tuple. Return true when a new tuple is
  /// available.
  bool has_next();
  
  // resets the next pointer to the start of the table
  void reset();

  /// Destroys the operator.
  void close();

  /// This returns the vector of the generated tuple. When
  /// `has_next()` returns true, the vector will contain the values for the
  /// next tuple. 
  std::vector<int> get_tuple();
};

}  // namespace operators
}  // namespace buzzdb

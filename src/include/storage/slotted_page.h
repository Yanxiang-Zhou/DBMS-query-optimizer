#pragma once

#include <bitset>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

namespace buzzdb {

struct TID {
  /// Constructor
  explicit TID(uint64_t raw_value);
  /// Constructor
  TID(uint64_t page, uint16_t slot);

  /// The TID value
  /// The TID could, for instance, look like the following:
  /// - 48 bit page id
  /// - 16 bit slot id
  uint64_t value;
};

std::ostream &operator<<(std::ostream &os, TID const &t);

struct SlottedPage {
  struct Header {
    // Constructor
    explicit Header(char *_buffer_frame, uint32_t page_size);

    /// overall page id
    uint64_t overall_page_id;
    /// location of the page in memory
    char *buffer_frame;
    /// Number of currently used slots
    uint16_t slot_count;
    /// To speed up the search for a free slot
    uint16_t first_free_slot;
    /// Lower end of the data
    uint32_t data_start;
    /// Space that would be available after compactification
    uint32_t free_space;
  };

  struct Slot {
    /// Constructor
    Slot() = default;
    /// The slot value
    /// c.f. chapter 3 page 13
    uint64_t value;
  };

  /// Constructor.
  /// @param[in] buffer_frame The location of the page
  /// @param[in] page_size    The size of the page.
  explicit SlottedPage(char *buffer_frame, uint32_t page_size);

  /// Compact the page.
  /// @param[in] page_size    The size of a buffer frame.
  void compactify(uint32_t page_size);

  /// The header.
  /// Note that the slotted page itself should reside on the buffer frame!
  /// DO NOT allocate heap objects for a slotted page but instead
  /// reinterpret_cast BufferFrame.get_data()! This is also the reason why the
  /// constructor and compactify require the actual page size as argument. (The
  /// slotted page itself does not know how large it is)
  Header header;

  /// Slot array
  // Slot* slots;

  Slot getSlot(uint16_t slotId);

  TID addSlot(uint32_t size);

  void setSlot(uint16_t slotId, uint64_t value);
};

std::ostream &operator<<(std::ostream &os, SlottedPage::Slot const &m);

std::ostream &operator<<(std::ostream &os, SlottedPage::Header const &h);

std::ostream &operator<<(std::ostream &os, SlottedPage const &p);

}  // namespace buzzdb

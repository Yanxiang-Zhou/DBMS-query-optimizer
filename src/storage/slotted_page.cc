
#include <algorithm>
#include <bitset>
#include <cassert>
#include <cstring>
#include <vector>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/slotted_page.h"

using SlottedPage = buzzdb::SlottedPage;
using TID = buzzdb::TID;

TID::TID(uint64_t raw_value) { value = raw_value; }

TID::TID(uint64_t page, uint16_t slot) { value = (page << 16) | slot; }

std::ostream &buzzdb::operator<<(std::ostream &os, TID const &t) {
  uint64_t overall_page_id = t.value >> 16;
  uint64_t page_id = BufferManager::get_segment_page_id(overall_page_id);
  uint16_t slot = t.value & ((1ull << 16) - 1);

  os << "TID: page_id: " << page_id << " -- ";
  os << "slot: " << slot << " \n";
  return os;
}

SlottedPage::Header::Header(char *_buffer_frame, uint32_t page_size) {
  buffer_frame = _buffer_frame;
  first_free_slot = 0;
  data_start = page_size;
  slot_count = 0;
  free_space = page_size - sizeof(header);
  slot_count = 0;
  overall_page_id = -1;
}

SlottedPage::SlottedPage(char *buffer_frame, uint32_t page_size)
    : header(buffer_frame, page_size) {}

std::ostream &buzzdb::operator<<(std::ostream &os, SlottedPage::Slot const &m) {
  uint64_t t = m.value >> 56;
  uint64_t s = m.value << 8 >> 56;
  uint64_t length = m.value << 40 >> 40;
  uint64_t offset = m.value << 16 >> 40;

  os << "[ T: " << t << " ";
  os << "S: " << s << " ";
  os << "O: " << offset << " ";
  os << "L: " << length << " ]";
  os << "\n";

  return os;
}

std::ostream &buzzdb::operator<<(std::ostream &os,
                                 SlottedPage::Header const &h) {
  os << "first_free_slot  : " << h.first_free_slot << "\n";
  os << "data_start       : " << h.data_start << "\n";
  os << "free_space       : " << h.free_space << "\n";
  os << "slot_count       : " << h.slot_count << "\n";

  return os;
}

std::ostream &buzzdb::operator<<(std::ostream &os, SlottedPage const &p) {
  uint16_t segment_id = BufferManager::get_segment_id(p.header.overall_page_id);
  uint64_t page_id =
      BufferManager::get_segment_page_id(p.header.overall_page_id);

  os << "------------------------------------------------\n";
  os << "Slotted Page:: segment " << segment_id << " :: page " << page_id
     << " \n";

  os << "Header: \n";
  os << p.header;

  os << "Slot List: ";
  os << " (" << p.header.slot_count << " slots)\n";

  auto slots = reinterpret_cast<buzzdb::SlottedPage::Slot *>(
      p.header.buffer_frame + sizeof(p.header));
  for (uint16_t slot_itr = 0; slot_itr < p.header.slot_count; slot_itr++) {
    os << slot_itr << " :: " << slots[slot_itr];
  }

  os << "------------------------------------------------\n";
  return os;
}

void SlottedPage::compactify(UNUSED_ATTRIBUTE uint32_t page_size) {}

buzzdb::SlottedPage::Slot SlottedPage::getSlot(uint16_t slotId) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  // std::cout << slots.size() << " "<< slotId <<  std::endl;
  return slots[slotId];
}

void SlottedPage::setSlot(uint16_t slotId, uint64_t value) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  slots[slotId].value = value;
}

TID SlottedPage::addSlot(uint32_t size) {
  uint64_t t = ~0;
  uint64_t s = 0;
  uint64_t offset = header.data_start - size;
  uint64_t length = size;

  // update data_start
  header.data_start = offset;

  if (size > header.free_space) {
    std::cout << "No space in page to add slot \n";
    std::cout << *this;
    std::cout << "free space: " << header.free_space << "\n";
    std::cout << "requested size: " << size << "\n";
    exit(0);
  }

  uint64_t slotValue = 0;
  slotValue += t << 56;
  slotValue += s << 56 >> 8;
  slotValue += (offset << 40 >> 16);
  slotValue += (length << 40 >> 40);

  Slot newSlot;
  newSlot.value = slotValue;

  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));

  // Add slot at end
  if (header.first_free_slot == header.slot_count) {
    slots[header.slot_count].value = slotValue;
    header.slot_count++;
  } else {
    // Update existing slot
    slots[header.first_free_slot].value = slotValue;
  }

  // Update free space
  uint32_t slot_space = header.slot_count * sizeof(Slot);
  header.free_space = header.data_start - slot_space - sizeof(header);

  TID new_tid = TID(header.overall_page_id, header.first_free_slot);

  bool found_empty_slot = false;
  for (uint16_t slot_itr = 0; slot_itr < header.slot_count; slot_itr++) {
    Slot l = slots[slot_itr];
    if (l.value == 0) {
      found_empty_slot = true;
      header.first_free_slot = slot_itr;
      break;
    }
  }

  if (!found_empty_slot) {
    header.first_free_slot = header.slot_count;
  }

  return new_tid;
}

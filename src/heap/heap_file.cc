
#include <iostream>

#include "heap/heap_file.h"
#include "common/macros.h"

namespace buzzdb {

HeapPage::Header::Header(char *_buffer_frame, uint32_t page_size) {
  buffer_frame = _buffer_frame;
  last_dirtied_transaction_id = INVALID_TXN_ID;
  first_free_slot = 0;
  data_start = page_size;
  slot_count = 0;
  free_space = page_size - sizeof(header);
  slot_count = 0;
  overall_page_id = -1;
}

HeapPage::HeapPage(char *buffer_frame, uint32_t page_size)
    : header(buffer_frame, page_size) {}

std::ostream &operator<<(std::ostream &os, HeapPage::Slot const &m) {
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

std::ostream &operator<<(std::ostream &os,
                                 HeapPage::Header const &h) {
  os << "first_free_slot  : " << h.first_free_slot << "\n";
  os << "data_start       : " << h.data_start << "\n";
  os << "free_space       : " << h.free_space << "\n";
  os << "slot_count       : " << h.slot_count << "\n";

  return os;
}

std::ostream &operator<<(std::ostream &os, HeapPage const &p) {
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

  auto slots = reinterpret_cast<buzzdb::HeapPage::Slot *>(
      p.header.buffer_frame + sizeof(p.header));
  for (uint16_t slot_itr = 0; slot_itr < p.header.slot_count; slot_itr++) {
    os << slot_itr << " :: " << slots[slot_itr];
  }

  os << "------------------------------------------------\n";
  return os;
}

HeapPage::Slot HeapPage::getSlot(uint16_t slotId) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  return slots[slotId];
}

void HeapPage::setSlot(uint16_t slotId, uint64_t value) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  slots[slotId].value = value;
}

TID HeapPage::addSlot(uint32_t size) {
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

HeapSegment::HeapSegment(uint16_t segment_id, LogManager &log_manager,
		BufferManager& buffer_manager)
    : segment_id_(segment_id),
	  log_manager_(log_manager),
	  buffer_manager_(buffer_manager),
	  page_count_(0){
}

TID HeapSegment::allocate(uint32_t record_size) {

	// Go over all pages in heap segment
	// std::cout << "Go over all pages in heap segment \n";

	for (size_t segment_page_itr = 0;
			segment_page_itr < page_count_;
			segment_page_itr++) {

		uint64_t page_id =
				BufferManager::get_overall_page_id(segment_id_, segment_page_itr);

		BufferFrame &frame = buffer_manager_.fix_page(page_id, true);

		auto* page = reinterpret_cast<SlottedPage*>(frame.get_data());

		if(record_size > page->header.free_space){
			continue;
		}

		TID tid = page->addSlot(record_size);
		buffer_manager_.unfix_page(frame, true);
		return tid;
	}

	// Did not find a free slot
	// std::cout << "Did not find a free slot \n";

	uint64_t page_id =
	      BufferManager::get_overall_page_id(segment_id_, page_count_);

	// Bump up page count for next allocation
	page_count_++;

	BufferFrame& frame = buffer_manager_.fix_page(page_id, true);

	auto* page = new (frame.get_data())
			SlottedPage(frame.get_data(), buffer_manager_.get_page_size());

	page->header.overall_page_id = page_id;

	TID tid = page->addSlot(record_size);
	buffer_manager_.unfix_page(frame, true);

	return tid;
}

uint32_t HeapSegment::read(TID tid, std::byte* record, uint32_t capacity) const {
  uint64_t page_id = tid.value >> 16;
  uint64_t overall_page_id =
      BufferManager::get_overall_page_id(segment_id_, page_id);
  uint16_t slot_id = tid.value & ((1ull << 16) - 1);

  BufferFrame& frame = buffer_manager_.fix_page(overall_page_id, false);
  auto* page = reinterpret_cast<SlottedPage*>(frame.get_data());

//  std::cout << *page;

  buzzdb::SlottedPage::Slot slot = page->getSlot(slot_id);

  uint64_t value = slot.value;
  uint32_t length = value << 40 >> 40;
  uint32_t offset = value << 16 >> 40;

  if (capacity <= length) {
    memcpy(record, &frame.get_data()[offset], capacity);
  } else {
    std::cout << "Capacity exceeds length \n";
    std::cout << "Length: " << length << "\n";
    exit(0);
  }

  buffer_manager_.unfix_page(frame, false);

  return length;
}

uint32_t HeapSegment::write(TID tid, std::byte* record, uint32_t record_size, UNUSED_ATTRIBUTE uint64_t txn_id) {

  uint64_t page_id = tid.value >> 16;
  uint64_t overall_page_id =
      BufferManager::get_overall_page_id(segment_id_, page_id);
  uint16_t slot_id = tid.value & ((1ull << 16) - 1);

  BufferFrame& frame = buffer_manager_.fix_page(overall_page_id, true);
  auto* page = reinterpret_cast<SlottedPage*>(frame.get_data());

  buzzdb::SlottedPage::Slot slot = page->getSlot(slot_id);
  uint64_t value = slot.value;

  uint32_t offset = value << 16 >> 40;
  
  // save before record
  std::vector<char> before_record;
	before_record.resize(record_size);
  memcpy(before_record.data(), &frame.get_data()[offset], record_size);

  // update
  memcpy(&frame.get_data()[offset], record, record_size);

  buffer_manager_.unfix_page(frame, true);

  // Add an update record
  log_manager_.log_update(txn_id, overall_page_id, record_size, offset, reinterpret_cast<std::byte *> (before_record.data()), record);

  return 0;
}

std::ostream &operator<<(std::ostream &os, HeapSegment const &s) {

	for (size_t segment_page_itr = 0;
			segment_page_itr < s.page_count_;
			segment_page_itr++) {

		uint64_t page_id =
				BufferManager::get_overall_page_id(s.segment_id_,
						segment_page_itr);

		BufferFrame &frame = s.buffer_manager_.fix_page(page_id, true);

		auto* page = reinterpret_cast<SlottedPage*>(frame.get_data());

		os << *page;

		s.buffer_manager_.unfix_page(frame, true);
	}

  return os;
}


}  // namespace buzzdb

#pragma once

#include <string>
#include <vector>
#include <atomic>
#include <cstddef>

#include "buffer/buffer_manager.h"
#include "log/log_manager.h"
#include "storage/slotted_page.h"  // for TID
#include "common/macros.h"

namespace buzzdb {

struct HeapPage {

	struct Header {
		// Constructor
		explicit Header(char *_buffer_frame, uint32_t page_size);

		/// overall page id
		uint64_t overall_page_id;
		/// last dirtied transaction id
		uint64_t last_dirtied_transaction_id;
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
		uint64_t value;
	};

	/// Constructor.
	/// @param[in] buffer_frame The location of the page
	/// @param[in] page_size    The size of the page.
	explicit HeapPage(char *buffer_frame, uint32_t page_size);

	/// The header.
	/// Note that the heap page itself should reside on the buffer frame!
	Header header;

	Slot getSlot(uint16_t slotId);

	TID addSlot(uint32_t size);

	void setSlot(uint16_t slotId, uint64_t value);

};

std::ostream &operator<<(std::ostream &os, HeapPage::Slot const &m);

std::ostream &operator<<(std::ostream &os, HeapPage::Header const &h);

std::ostream &operator<<(std::ostream &os, HeapPage const &p);

class HeapSegment {

public:
	/// Constructor.
	/// @param[in] segment_id       Id of the segment.
	/// @param[in] log_manager   The log manager that should be used by the
	/// transaction manager to store all log records
	/// @param[in] buffer_manager   The buffer manager that should be used by the
	/// segment.
	HeapSegment(uint16_t segment_id, LogManager &log_manager,
			BufferManager &buffer_manager);

	/// Allocate a new record.
	/// Returns a TID that stores the page as well as the slot of the allocated
	/// record. The allocate method should use the free-space inventory to find a
	/// suitable page quickly.
	/// @param[in] size         The size that should be allocated.
	TID allocate(uint32_t record_size);

	/// Read the data of the record into a buffer.
	/// @param[in] tid          The TID that identifies the record.
	/// @param[in] record       The buffer that is read into.
	/// @param[in] capacity     The capacity of the buffer that is read into.
	uint32_t read(TID tid, std::byte *record, uint32_t capacity) const;

	/// Write a record.
	/// @param[in] tid          The TID that identifies the record.
	/// @param[in] record       The buffer that is written.
	/// @param[in] record_size  The capacity of the buffer that is written.
	/// @param[in] txn_id		The txn_id for the transaction
	uint32_t write(TID tid, std::byte* record, uint32_t record_size, uint64_t txn_id = INVALID_TXN_ID);

	/// The segment id
	uint16_t segment_id_;

	/// Log manager
	LogManager &log_manager_;

	/// The buffer manager
	BufferManager &buffer_manager_;

	/// Number of pages in segment
	uint64_t page_count_;
};

std::ostream &operator<<(std::ostream &os, HeapSegment const &s);

}  // namespace buzzdb

#include <cassert>
#include <iostream>
#include <string>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"
#include "storage/slotted_page.h"

/*
This is a dummy implementation of a buffer manager. It does not do any locking.
 */

namespace buzzdb {

char* BufferFrame::get_data() {
	return data.data();
}


BufferManager::BufferManager(size_t page_size, size_t page_count) {
	capacity_ = page_count;
	page_counter_ = 0;
	page_size_ = page_size;

	pool_.resize(capacity_);
	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		pool_[frame_id].reset(new BufferFrame());
		pool_[frame_id]->data.resize(page_size_);
		pool_[frame_id]->page_id = INVALID_PAGE_ID;
		pool_[frame_id]->frame_id = frame_id;
	}

}

BufferManager::~BufferManager() {
	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->dirty == true) {
			write_frame(frame_id);
		}
	}
}

BufferFrame& BufferManager::fix_page(uint64_t page_id, bool /*exclusive*/) {

//	std::cout << "Fix page: " << page_id << "\n";

	if (page_id == INVALID_PAGE_ID) {
		std::cout << "INVALID FIX REQUEST \n";
		exit(-1);
	}

	/// Check if page is in buffer
	uint64_t page_frame_id = get_frame_id_of_page(page_id);
	if (page_frame_id != INVALID_FRAME_ID) {
		return *pool_[page_frame_id];
	}

//	std::cout << "Create page: " << page_id << "\n";

	// Create a new page
	uint64_t free_frame_id = page_counter_++;

	if(page_counter_ >= capacity_){
		std::cout << "Out of space \n";
		std::cout << page_counter_ << " " << capacity_ << "\n";
		exit(-1);
	}

	pool_[free_frame_id]->page_id = page_id;
	pool_[free_frame_id]->dirty = false;

	read_frame(free_frame_id);

	return *pool_[free_frame_id];
}

void BufferManager::read_frame(uint64_t frame_id) {

	auto segment_id = get_segment_id(pool_[frame_id]->page_id);
	auto file_handle =
			File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
	size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
	
	file_handle->read_block(start, page_size_, pool_[frame_id]->data.data());
}

void BufferManager::write_frame(uint64_t frame_id) {

	auto segment_id = get_segment_id(pool_[frame_id]->page_id);
	auto file_handle =
			File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
	size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;

	file_handle->write_block(pool_[frame_id]->data.data(), start, page_size_);
}

void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {

	if (!page.dirty) {
		page.dirty = is_dirty;
	}

}

void  BufferManager::flush_page(uint64_t page_id){

	// std::cout << "FLUSH: " << page_id << "\n";

	/// Check if page is in buffer
	uint64_t page_frame_id = get_frame_id_of_page(page_id);
	if (page_frame_id != INVALID_FRAME_ID) {
		if (pool_[page_frame_id]->dirty == true) {
			write_frame(page_frame_id);
		}
	}

}

void  BufferManager::discard_page(uint64_t page_id){

	/// Check if page is in buffer
	uint64_t page_frame_id = get_frame_id_of_page(page_id);
	if (page_frame_id != INVALID_FRAME_ID) {
		pool_[page_frame_id].reset(new BufferFrame());
		pool_[page_frame_id]->page_id = INVALID_PAGE_ID;
		pool_[page_frame_id]->dirty = false;
		pool_[page_frame_id]->data.resize(page_size_);
	}

}

void  BufferManager::flush_all_pages(){

//	std::cout << "FLUSH ALL PAGES \n";

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->dirty == true) {
			write_frame(frame_id);
		}
	}

}

void  BufferManager::discard_all_pages(){

//	std::cout << "DISCARD ALL PAGES \n";

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		pool_[frame_id].reset(new BufferFrame());
		pool_[frame_id]->page_id = INVALID_PAGE_ID;
		pool_[frame_id]->dirty = false;
		pool_[frame_id]->data.resize(page_size_);
	}

}

uint64_t BufferManager::get_frame_id_of_page(uint64_t page_id){

	uint64_t page_frame_id = INVALID_FRAME_ID;

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->page_id == page_id) {
			page_frame_id = frame_id;
			break;
		}
	}

	return page_frame_id;
}


std::vector<uint64_t> BufferManager::get_fifo_list() const {
	return {};
}


std::vector<uint64_t> BufferManager::get_lru_list() const {
	return {};
}

}  // namespace buzzdb

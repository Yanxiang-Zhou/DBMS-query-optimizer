
#include "operators/seq_scan.h"

#include <cassert>
#include <functional>
#include <string>

#include "common/macros.h"

#define UNUSED(p) ((void)(p))
namespace buzzdb {
namespace operators {

SeqScan::SeqScan(uint16_t table_id, uint64_t num_pages, uint64_t num_fields){
  _table_id = table_id;
  _num_pages = num_pages;
  _num_fields = num_fields;
  _buffer_manager = new BufferManager (buzzdb::BUFFER_PAGE_SIZE, buzzdb::BUFFER_PAGE_COUNT);
  const char* LOG_FILE = buzzdb::LOG_FILE_PATH.c_str();
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	_heap_segment = new HeapSegment (_table_id, log_manager, *_buffer_manager);
}

void SeqScan::open(){
  // read from catalog
  _curr_segment = 0;
  _curr_slot = 0;
}
void SeqScan::reset(){
  _curr_segment = 0;
  _curr_slot = 0;
}

SeqScan::~SeqScan(){
}

void SeqScan::close(){
  delete _buffer_manager;
  delete _heap_segment;
}

bool SeqScan::has_next(){
  auto tuple_size = sizeof(int)*_num_fields; //  field1 | field2
  while(_curr_segment < _num_pages){
			uint64_t page_id =
					BufferManager::get_overall_page_id(
							_heap_segment->segment_id_, _curr_segment);

			BufferFrame &frame = _buffer_manager->fix_page(page_id, true);

			auto* page = reinterpret_cast<SlottedPage*>(frame.get_data());
			page->header.buffer_frame = reinterpret_cast<char*>(page);
			auto overall_page_id = page->header.overall_page_id;
			auto slot_count = page->header.first_free_slot;
      while(_curr_slot < slot_count){
				TID tid = TID(overall_page_id, _curr_slot);

				// Check slot
				std::vector<char> buf;
				buf.resize(tuple_size);
				int field;

				_heap_segment->read(tid, reinterpret_cast<std::byte *>(buf.data()),
						tuple_size);
        _tuple.clear();
        for(size_t i=0; i<_num_fields; i++){
          memcpy(&field, buf.data() + i*sizeof(int), sizeof(int));
          _tuple.push_back(field);
        }
				
        _curr_slot++;

        _buffer_manager->unfix_page(frame, true);
        return true;
      }
      _buffer_manager->unfix_page(frame, true);
      _curr_segment++;
      _curr_slot = 0;
    }
  return false;
}
	
std::vector<int> SeqScan::get_tuple(){
  return _tuple;
  }
}  // namespace operators
}  // namespace buzzdb

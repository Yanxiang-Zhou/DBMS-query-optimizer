#include "utils.h"
using  buzzdb::operators::PredicateType;
using buzzdb::BufferManager;
using buzzdb::LogManager;
using buzzdb::HeapSegment;
using buzzdb::TID;
using buzzdb::BufferFrame;
using buzzdb::SlottedPage;
using buzzdb::File;

	auto catalog_file = buzzdb::File::open_file(buzzdb::CATALOG.c_str(), buzzdb::File::WRITE);

    uint64_t TestUtils::populate_table(uint64_t table_id, uint32_t num_tuples, uint32_t num_cols, uint32_t max_rand){
		BufferManager buffer_manager(buzzdb::BUFFER_PAGE_SIZE, buzzdb::BUFFER_PAGE_COUNT);
		const char* LOG_FILE = buzzdb::LOG_FILE_PATH.c_str();
		auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
		LogManager log_manager(logfile.get());
		HeapSegment heap_segment(table_id, log_manager, buffer_manager);

		/* initialize random seed: */
		// srand(100);
		std::random_device device;
		std::mt19937 generator(device());
		std::vector<uint32_t> tuples = generate_random(max_rand, num_tuples*num_cols, generator);
		auto tuple_size = sizeof(uint32_t)*num_cols; // field1 | field2 | 
		std::size_t i = 0;
		while(i < tuples.size()){
			// Allocate slot
			auto tid = heap_segment.allocate(tuple_size);
			// Write buffer
			std::vector<char> buf;
			buf.resize(tuple_size);

			// Populate tuple
			for(size_t j=0; j<num_cols;j++){
				int field = tuples[i++];
				memcpy(buf.data() + j*sizeof(uint32_t), &field, sizeof(uint32_t));
			}
			heap_segment.write(tid, reinterpret_cast<std::byte *>(buf.data()),
					tuple_size);
		}
		buffer_manager.flush_all_pages();
		char data[2*sizeof(uint64_t)]; 
		memcpy(&data, &table_id, sizeof(uint64_t));
		memcpy(&data[sizeof(uint64_t)], &heap_segment.page_count_, sizeof(uint64_t));
		catalog_file->write_block(data, 0, 2*sizeof(uint64_t));
		
		return heap_segment.page_count_;
	}
	
	std::vector<uint32_t> TestUtils::generate_random(int N, int k, std::mt19937& gen)
	{
    	std::uniform_int_distribution<> dis(1, N-1);
    	std::vector<uint32_t> elems;

    	while (elems.size() < (uint64_t)k) {
        	elems.push_back(dis(gen));
    	}
		return elems;
	}


	std::vector<double> TestUtils::get_diff(std::vector<double>& sequence) {
		std::vector<double> ret(sequence.size()-1); 
		for (size_t i = 0; i < sequence.size() - 1; ++i)
			ret[i] = sequence[i + 1] - sequence[i];
		return ret;
	}

	bool TestUtils::check_constant(std::vector<double> sequence){
		bool ret = false;
		//compute average
		double sum = 0.0;
		for(size_t i = 0; i < sequence.size(); ++i)
			sum += sequence[i];
		double av = sum/(sequence.size() + .0);
		//compute standard deviation
		double sqsum = 0;
		for(size_t i = 0; i < sequence.size(); ++i)
			sqsum += (sequence[i] - av)*(sequence[i] - av);
		double std = std::sqrt(sqsum/(sequence.size() + .0));
		ret = std < 1.0 ? true : false;
		return ret;
	}

	bool TestUtils::check_linear(std::vector<double> sequence){
		return check_constant(get_diff(sequence));
	}
	bool TestUtils::check_quadratic(std::vector<double> sequence){
		return check_linear(get_diff(sequence));
	}
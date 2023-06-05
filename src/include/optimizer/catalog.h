#pragma once
#include <cstddef>
#include <cstdint>
#include "operators/operators.h"
#include "table_stats.h"
#include <map>
#include <vector>

using buzzdb::operators::PredicateType;
using buzzdb::table_stats::TableStats;
namespace buzzdb {
namespace catalog {
    class Catalog{
        private:
            std::map<uint16_t, uint64_t> pages;
        public:
            Catalog();
            uint64_t get_num_heap_pages(uint16_t table_id) {return pages[table_id];}
            void get_num_heap_pages(uint16_t table_id, uint64_t num_pages) { pages[table_id] = num_pages; }
    };
}
}
#pragma once
#include <cstddef>
#include <cstdint>
#include "operators/seq_scan.h"
#include <vector>

using buzzdb::operators::PredicateType;

namespace buzzdb {
namespace table_stats {

class IntHistogram {
    public:
        IntHistogram() = default;
        
        IntHistogram(int64_t buckets, int64_t min_val, int64_t max_val);
        
        double estimate_selectivity(PredicateType op, int64_t v);
        void add_value(int64_t val);

        double span;
        int64_t min_v;
        int64_t max_v;
        int ntups;
        int NumB;
        std::vector<int> umap;
};



class TableStats{
    public:
        TableStats() = default;
        TableStats(int64_t table_id, int64_t io_cost_per_page, 
                uint64_t num_pages, uint64_t num_fields);
        double estimate_selectivity(int64_t field, PredicateType op, int64_t constant);
        double estimate_scan_cost();
        uint64_t estimate_table_cardinality(double selectivity_factor);
        
    private:
        /**
         * Number of bins for the histogram. Feel free to increase this value over
         * 100, though our tests assume that you have at least 100 bins in your
         * histograms.
         */
        std::vector<int64_t> min_value;
        std::vector<int64_t> max_value;
        int num_tups = 0;
        int64_t io_cost;
        int64_t nump;
        int64_t numf;
        std::unordered_map<int, IntHistogram> histmap;
        int NUM_HIST_BINS = 100;
};


}  // namespace table_stats
}  // namespace buzzdb
#include <algorithm>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "operators/seq_scan.h"
#include "heap/heap_file.h"
#include "log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "common/macros.h"
#include "buffer/buffer_manager.h"
#include <ctime>
#include <iostream>
#include <random>

class TestUtils{
    public:
        	std::vector<uint32_t> generate_random(int N, int k, std::mt19937& gen);
            uint64_t populate_table(uint64_t table_id, uint32_t num_tuples, uint32_t num_cols, uint32_t max_rand);
            bool check_constant(std::vector<double> stats);
            bool check_linear(std::vector<double> stats);
            bool check_quadratic(std::vector<double> stats);
    private:
        std::vector<double> get_diff(std::vector<double>& sequence);
};
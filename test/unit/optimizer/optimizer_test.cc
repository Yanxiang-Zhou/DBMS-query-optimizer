
#include <gtest/gtest.h>
#include <algorithm>
#include <random>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "optimizer/table_stats.h"
#include "optimizer/join_optimizer.h"
#include "operators/seq_scan.h"
#include "heap/heap_file.h"
#include "log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "common/macros.h"
#include "buffer/buffer_manager.h"
#include <ctime>
#include <iostream>
#include <random>
#include "utils.h"
#include <future>

#define TEST_TIMEOUT_BEGIN   std::promise<bool> promisedFinished; \
                              auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
 
#define TEST_TIMEOUT_FAIL_END(X)  finished.set_value(true); \
                                   }, std::ref(promisedFinished)).detach(); \
                                   EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout);


namespace {
    
using  buzzdb::operators::PredicateType;
using  buzzdb::table_stats::IntHistogram;
using  buzzdb::table_stats::TableStats;
using buzzdb::optimizer::JoinOptimizer;
using buzzdb::optimizer::LogicalJoinNode;
using buzzdb::BufferManager;
using buzzdb::LogManager;
using buzzdb::HeapSegment;
using buzzdb::TID;
using buzzdb::BufferFrame;
using buzzdb::SlottedPage;
using buzzdb::File;

	uint64_t table_id = 101, table_id2 = 102;
	std::string table_name1 = "t1", table_name2 = "t2";
	uint64_t IO_COST = 100;
	uint64_t num_pages, num_pages2;
	uint64_t num_fields = 4;
	std::map<std::string, TableStats> stats_map;

	
    std::vector<double> get_table_scan_costs(std::vector<int> io_costs, std::vector<int> page_nums){
        std::vector<double> ret(io_costs.size(), 0);
		for(size_t i=0; i<io_costs.size(); i++){
			uint64_t n_pages = TestUtils().populate_table(i, 510*page_nums[i], 2, 32);
            ret[i] = TableStats(i, io_costs[i], n_pages, 2).estimate_scan_cost();
        }
        return ret;
    }
	
	std::vector<double> get_join_costs(JoinOptimizer jo, LogicalJoinNode js,
            std::vector<int> card1s, std::vector<int> card2s, 
			std::vector<double> cost1s, std::vector<double> cost2s) {
        std::vector<double> ret(card1s.size(), 0.0);
        for (size_t i = 0; i < card1s.size(); ++i) {
            ret[i] = jo.estimate_join_cost(js, card1s[i], card2s[i], cost1s[i],
                    cost2s[i], stats_map);
            // assert that the join cost is no less than the total cost of
            // scanning two tables
            EXPECT_TRUE(ret[i] > cost1s[i] + cost2s[i]);
        }
        return ret;
    }
    
	
	void check_join_estimate_costs(JoinOptimizer jo,LogicalJoinNode join_node) {
        std::vector<int> card1s(20), card2s(20);  
		std::vector<double> cost1s(20), cost2s(20);

        // card1s linear others constant
        for (size_t i = 0; i < card1s.size(); ++i) {
            card1s[i] = 3 * i + 1;
            card2s[i] = 5;
            cost1s[i] = cost2s[i] = 5.0;
        }
        auto stats = get_join_costs(jo, join_node, card1s, card2s,
                cost1s, cost2s);
        bool ret = TestUtils().check_linear(stats);
        EXPECT_TRUE (ret);

        // card2s linear others constant
        for (size_t i = 0; i < card1s.size(); ++i) {
            card1s[i] = 4;
            card2s[i] = 3 * i + 1;
            cost1s[i] = cost2s[i] = 5.0;
        }
        stats = get_join_costs(jo, join_node, card1s, card2s, cost1s,
                cost2s);
        ret = TestUtils().check_linear(stats);
        EXPECT_TRUE (ret);

        // cost1s linear others constant
        for (size_t i = 0; i < card1s.size(); ++i) {
            card1s[i] = card2s[i] = 7;
            cost1s[i] = 5.0 * (i + 1);
            cost2s[i] = 3.0;
        }
        stats = get_join_costs(jo, join_node, card1s, card2s, cost1s,
                cost2s);
        ret = TestUtils().check_linear(stats);
        EXPECT_TRUE (ret);

        // cost2s linear others constant
        for (size_t i = 0; i < card1s.size(); ++i) {
            card1s[i] = card2s[i] = 9;
            cost1s[i] = 5.0;
            cost2s[i] = 3.0 * (i + 1);
        }
        stats = get_join_costs(jo, join_node, card1s, card2s, cost1s,
                cost2s);
        ret = TestUtils().check_linear(stats);
        EXPECT_TRUE (ret);

        // everything linear
        for (size_t i = 0; i < card1s.size(); ++i) {
            card1s[i] = 2 * (i + 1);
            card2s[i] = 9 * i + 1;
            cost1s[i] = 5.0 * i + 2;
            cost2s[i] = 3.0 * i + 1;
        }
        stats = get_join_costs(jo, join_node, card1s, card2s, cost1s,
                cost2s);
        ret = TestUtils().check_quadratic(stats);
        EXPECT_TRUE (ret);
    }
	/**
	 * Test to confirm that the IntHistogram implementation is constant-space
	 * (or, at least, reasonably small space; O(log(n)) might still work if
	 * your constants are good).
	 */
    TEST(HistogramTest, OrderOfGrowthTest){
        IntHistogram hist(10000, 0, 100);

        for(int c=0; c<33554432; c++){
            hist.add_value((c*23)%101);
        }

        double selectivity = 0.0;
        for(int c=0; c<101; c++){
            selectivity += hist.estimate_selectivity(PredicateType::EQ, c); 
        }

        // all the selectivity should add upto 1.0
        EXPECT_TRUE(selectivity > 0.99);
    }

    /**
	 * Test with a minimum and a maximum that are both negative numbers.
	 */
    TEST(HistogramTest, NegativeRangeTest){
        IntHistogram hist(10, -60, -10);
        
        for (int c = -60; c <= -10; c++) {
			hist.add_value(c);
		}

		// Even with just 10 bins and 50 values,
		// the selectivity for this particular value should be at most 0.2.
        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::EQ, -33) < 0.3);

        // And it really shouldn't be 0.
		// Though, it could easily be as low as 0.02, seeing as that's
		// the fraction of elements that actually are equal to -33.
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::EQ, -33) > 0.001);
    }

    /**
	 * Make sure that equality binning does something reasonable.
	 */
    TEST(HistogramTest, EqualsTest){
        IntHistogram hist(10, 1, 10);
        
        hist.add_value(3);
		hist.add_value(3);
		hist.add_value(3);
		

		// This really should return "1.0"; but,
		// be conservative in case of alternate implementations
        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::EQ, 3) > 0.9);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::EQ, 8) < 0.001);

        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::NE, 3) < 0.001);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::NE, 8) > 0.01);
    }

    /**
	 * Make sure that GREATER_THAN, LESS_THAN binning does something reasonable.
	 */
    TEST(HistogramTest, LtTest){
        IntHistogram hist(10, 1, 10);
        
        hist.add_value(3);
		hist.add_value(3);
		hist.add_value(3);
		hist.add_value(1);
        hist.add_value(10);

		
        // be conservative in case of alternate implementations
        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GT, -1) > 0.999);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GT, 2) > 0.6);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GT, 4) < 0.4);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GT, 12) < 0.001);

		// be conservative in case of alternate implementations
        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LT, -1) < 0.001);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LT, 2) < 0.4);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LT, 4) > 0.6);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LT, 12) > 0.999);
    }

    /**
	 * Make sure that GREATER_THAN_OR_EQ, LESS_THAN_OR_EQ binning does something reasonable.
	 */
    TEST(HistogramTest, GETest){
        IntHistogram hist(10, 1, 10);
        
        hist.add_value(3);
		hist.add_value(3);
		hist.add_value(3);
		hist.add_value(1);
        hist.add_value(10);

		// be conservative in case of alternate implementations
        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GE, -1) > 0.999);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GE, 2) > 0.6);
        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GE, 3) > 0.45);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GE, 4) < 0.5);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::GE, 12) < 0.001);
        
        // be conservative in case of alternate implementations
        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LE, -1) < 0.001);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LE, 2) < 0.4);
        EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LE, 3) > 0.45);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LE, 4) > 0.6);
		EXPECT_TRUE(hist.estimate_selectivity(PredicateType::LE, 12) > 0.999);
    }
    

    /**
	 * This test checks that the estimate_scan_cost is: 
	 *   +linear in numPages when IO_COST is constant
	 *   +linear in IO_COST when numPages is constant
	 *   +quadratic when IO_COST and numPages increase linearly.
	 */
    TEST(TableStatsTest, EstimateScanCostTest){

		std::vector<int> io_costs(10, 0);
		std::vector<int> page_nums(10, 0);

		// io_costs constant, page_nums linear
		for(size_t i=0; i<io_costs.size(); i++){
			io_costs[i]=1;
			page_nums[i]=3*(i+1);
		}
		auto stats = get_table_scan_costs(io_costs, page_nums);
		auto ret = TestUtils().check_constant(stats);
		EXPECT_FALSE(ret);
		ret = TestUtils().check_linear(stats);
		EXPECT_TRUE(ret);

		// page_nums constant, io_costs linear
		for(size_t i=0; i<io_costs.size(); i++){
			io_costs[i]=8*(i+1);
			page_nums[i]=1;
		}
		stats = get_table_scan_costs(io_costs, page_nums);
		ret = TestUtils().check_constant(stats);
		EXPECT_FALSE(ret);
		ret = TestUtils().check_linear(stats);
		EXPECT_TRUE(ret);
		
		// page_nums linear, io_costs linear
		for(size_t i=0; i<io_costs.size(); i++){
			io_costs[i]=3*(i+1);
			page_nums[i]=i+1;
		}
		stats = get_table_scan_costs(io_costs, page_nums);
		ret = TestUtils().check_constant(stats);
		EXPECT_FALSE(ret);
		ret = TestUtils().check_linear(stats);
		EXPECT_FALSE(ret);	
		ret = TestUtils().check_quadratic(stats);
		EXPECT_TRUE(ret);
				
		TestUtils().populate_table(5, 1000, 2, 1000000);

	}
	

	/**
	 * Verify the table-cardinality estimates based on a selectivity estimate
	 */

	TEST(TableStatsTest, EstimateTableCardinalityTest) {
		TableStats stats = TableStats(table_id, IO_COST, num_pages, num_fields);
		// Try a random selectivity
		EXPECT_EQ(3060, stats.estimate_table_cardinality(0.3));
		
		// Make sure we get all rows with 100% selectivity, and none with 0%
		EXPECT_EQ(10200, stats.estimate_table_cardinality(1.0));
		EXPECT_EQ(0, stats.estimate_table_cardinality(0.0));
	}


	TEST(TableStatsTest, EstimateSelectivityTest) {
		// tuples between 0 and 32
		int max_val = 32;	
		int min_val = 0;
		int above_max = max_val + 10;
		int mid_val = (min_val + max_val)/2;
		int below_min = min_val - 10;
		
		// uint64_t num_pages = 
		TableStats stats = TableStats(table_id, IO_COST, num_pages, num_fields);
		for (size_t col = 0; col < num_fields; col++) {
			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::EQ, above_max), 0.001);			
			ASSERT_NEAR(1.0/32.0, stats.estimate_selectivity(col, PredicateType::EQ, mid_val), 0.015);
			ASSERT_NEAR(0, stats.estimate_selectivity(col, PredicateType::EQ, below_min), 0.001);

			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::NE, above_max), 0.001);
			ASSERT_NEAR(31.0/32.0, stats.estimate_selectivity(col, PredicateType::NE, mid_val), 0.015);
			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::NE, below_min), 0.015);

			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::GT, above_max), 0.001);
			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::GT, max_val), 0.001);
			ASSERT_NEAR(0.5, stats.estimate_selectivity(col, PredicateType::GT, mid_val), 0.1);
			ASSERT_NEAR(31.0/32.0, stats.estimate_selectivity(col, PredicateType::GT, min_val), 0.05);
			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::GT, below_min), 0.001);
			
			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::LT, above_max), 0.001);
			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::LT, max_val), 0.015);
			ASSERT_NEAR(0.5, stats.estimate_selectivity(col, PredicateType::LT, mid_val), 0.1);
			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::LT, min_val), 0.001);
			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::LT, below_min), 0.001);
			
			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::GE, above_max), 0.001);
			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::GE, max_val), 0.015);
			ASSERT_NEAR(0.5, stats.estimate_selectivity(col, PredicateType::GE, mid_val), 0.1);
			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::GE, min_val), 0.015);
			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::GE, below_min), 0.001);
			
			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::LE, above_max), 0.001);
			ASSERT_NEAR(1.0, stats.estimate_selectivity(col, PredicateType::LE, max_val), 0.015);
			ASSERT_NEAR(0.5, stats.estimate_selectivity(col, PredicateType::LE, mid_val), 0.1);
			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::LE, min_val), 0.05);
			ASSERT_NEAR(0.0, stats.estimate_selectivity(col, PredicateType::LE, below_min), 0.001);
		}
	}

	/**
     * Verify that the estimated join costs from estimate_join_cost() are
     * reasonable we check various order requirements for the output of
     * estimate_join_cost.
     */
	TEST(JoinOptimizerTest, EstimateJoinCostTest){
		JoinOptimizer jo;

		// 1 join 2
		auto equal_join_node = LogicalJoinNode(table_name1, table_name2, 1, 2, PredicateType::EQ);
		check_join_estimate_costs(jo, equal_join_node);

		// 2 join 1
		equal_join_node = LogicalJoinNode(table_name1, table_name2, 2, 1, PredicateType::EQ);
		check_join_estimate_costs(jo, equal_join_node);

		// 1 join 1
		equal_join_node = LogicalJoinNode(table_name1, table_name2, 1, 1, PredicateType::EQ);
		check_join_estimate_costs(jo, equal_join_node);

		// 2 join 2
		equal_join_node = LogicalJoinNode(table_name1, table_name2, 2, 2, PredicateType::EQ);
		check_join_estimate_costs(jo, equal_join_node);
	}

	/**
     * Verify that the join cardinalities produced by estimate_join_cardinality()
     * are reasonable
     */
	TEST(JoinOptimizerTest, EstimateJoinCardinality) {
		JoinOptimizer jo;
		auto card1 = stats_map[table_name1].estimate_table_cardinality(0.8);
		auto card2 = stats_map[table_name2].estimate_table_cardinality(0.2);
		auto join_node = LogicalJoinNode(table_name1, table_name2, 1, 2, PredicateType::EQ);
		
        double cardinality = jo.estimate_join_cardinality(join_node, card1, card2, true, false, stats_map);
		
		EXPECT_TRUE(cardinality == 8160 || cardinality == 200);

		cardinality = jo.estimate_join_cardinality(join_node, card1, card2, false, true, stats_map);
        EXPECT_TRUE(cardinality == 8160 || cardinality == 200);
    }

	/**
     * Determine whether the orderJoins implementation is doing a reasonable job
     * of ordering joins, and not taking an unreasonable amount of time to do so
     */
    TEST(JoinOptimizerTest, OrderJoinsTest) {
		/**  Query: SELECT * FROM emp,dept,hobbies,hobby 
			WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND e.c3 < 1000;
			There are 4 tables: emp,dept,hobbies,hobby (ids: 201,202,203,204)
			Columns of each table are named as: c0,c1,c2,c3... (0,1,2,3, ...)
			You can access the stats corresponding to each of the column by indexing.
			Eg: stats['dept'][1] = stats for dept's column c1
		*/

        std::vector<LogicalJoinNode> result;
        std::vector<LogicalJoinNode> nodes;
        std::map<std::string, TableStats> stats;
        std::map<std::string, double> filter_selectivities;

        // Create all of the tables

		// emp 
		// 6 columns and 100000 tuples
		auto emp_pages = TestUtils().populate_table(201, 50000, 6, 32);
		stats["emp"] = TableStats(201, IO_COST, emp_pages, 6);

		// dept 
		// 3 columns and 1000 tuples
		auto dept_pages = TestUtils().populate_table(202, 1000, 3, 32);
		stats["dept"] = TableStats(202, IO_COST, dept_pages, 3);

		// hobby 
		// 6 columns and 1000 tuples
		auto hobby_pages = TestUtils().populate_table(203, 1000, 6, 32);
		stats["hobby"] = TableStats(203, IO_COST, hobby_pages, 6);


		// hobbies 
		// 2 columns and 200000 tuples
		auto hobbies_pages = TestUtils().populate_table(204, 100000, 2, 32);
		stats["hobbies"] = TableStats(204, IO_COST, hobbies_pages, 2);


        // Note that your code shouldn't re-compute selectivities.
        // If you get statistics numbers, even if they're wrong (which they are
        // here
        // because the data is random), you should use the numbers that you are
        // given.
        // Re-computing them at runtime is generally too expensive for complex
        // queries.
        filter_selectivities["emp"] = 0.1;
        filter_selectivities["dept"] = 1.0;
        filter_selectivities["hobby"] = 1.0;
        filter_selectivities["hobbies"]= 1.0;

        // Note that there's no particular guarantee that the LogicalJoinNode's
        // will be in
        // the same order as they were written in the query.
        // They just have to be in an order that uses the same operators and
        // semantically means the same thing.
        nodes.push_back(LogicalJoinNode("hobbies", "hobby", 1, 0,
                PredicateType::EQ));
        nodes.push_back(LogicalJoinNode("emp", "dept", 1, 0,
                PredicateType::EQ));
        nodes.push_back(LogicalJoinNode("emp", "hobbies", 2, 0,
                PredicateType::EQ));

		JoinOptimizer j(nodes);

        // Set the last boolean here to 'true' in order to have orderJoins()
        // print out its logic
        result = j.order_joins(stats, filter_selectivities);

        // There are only three join nodes; if you're only re-ordering the join
        // nodes,
        // you shouldn't end up with more than you started with
        EXPECT_EQ(result.size(), nodes.size());

        // There were a number of ways to do the query, reasonably
        // well;
        // we're just doing a heuristics-based optimizer, so, only ignore the
        // really
        // bad case where "hobbies" is the outermost node in the left-deep tree.
        EXPECT_FALSE(result[0].left_table == "hobbies");

        // Also check for some of the other silly cases, like forcing a cross
        // join by
        // "hobbies" only being at the two extremes, or "hobbies" being the
        // outermost table.
        EXPECT_FALSE(result[2].right_table == "hobbies"
                && (result[0].left_table == "hobbies" || result[0].right_table == "hobbies"));
    }


    /**
     * Test a much-larger join ordering, to confirm that it executes in a
     * reasonable amount of time
     */
	TEST(JoinOptimizerTest, BigOrderJoinsTest){
		TEST_TIMEOUT_BEGIN
        
			std::vector<LogicalJoinNode> result;
			std::vector<LogicalJoinNode> nodes;
			std::map<std::string, TableStats> stats;
			std::map<std::string, double> filter_selectivities;

			// Create a large set of tables, and add tuples to the tables
			// 300: big_table
			// 301, 302 ..: a,b,c
			// // c0 is the primary key
			std::vector<uint64_t> table_ids; 
			for(size_t i=0; i<15; i++) {
				table_ids.push_back(300+i);
			}
			// 100000
			auto num_pages = TestUtils().populate_table(300, 100000, 2, 32);
			stats["big_table"] = TableStats(300, IO_COST, num_pages, 2);
			filter_selectivities["big_table"] = 1.0;
			for(size_t i=0; i<11; i++){
				auto num_pages = TestUtils().populate_table(table_ids[i+1], 100, 2, 32);
				auto table_name = std::string(1, (char)('a'+i)); 
				stats[table_name] = TableStats(table_ids[i+1], IO_COST, num_pages, 2);
				filter_selectivities[table_name] = 1.0;
			}
			

			// Add the nodes to a collection for a query plan
			nodes.push_back(LogicalJoinNode("a", "b", 1, 1, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("b", "c", 0, 0, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("c", "d", 1, 1, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("d", "e", 0, 0, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("e", "f", 1, 1, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("f", "g", 0, 0, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("g", "h", 1, 1, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("h", "i", 0, 0, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("i", "j", 1, 1, PredicateType::EQ));
			// nodes.push_back(LogicalJoinNode("k", "l", 1, 1, PredicateType::EQ));
			// nodes.push_back(LogicalJoinNode("l", "m", 0, 0, PredicateType::EQ));
			// nodes.push_back(LogicalJoinNode("m", "n", 1, 1, PredicateType::EQ));
			nodes.push_back(LogicalJoinNode("j", "big_table", 2, 2, PredicateType::EQ));

			// Make sure we don't give the nodes to the optimizer in a nice order
			auto rng = std::default_random_engine {};
			std::shuffle(std::begin(nodes), std::end(nodes), rng);
			/* Query: SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n 
				WHERE bigTable.c2 = j.c2 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 
				AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 
				AND h.c0 = i.c0 AND i.c1 = j.c1;
			**/

			JoinOptimizer j(nodes);

			result = j.order_joins(stats, filter_selectivities);

			// If you're only re-ordering the join nodes,
			// you shouldn't end up with more than you started with
			EXPECT_EQ(result.size(), nodes.size());

			// Make sure that "bigTable" is the outermost table in the join
	        EXPECT_EQ(result[(result.size() - 1)].right_table, "big_table");

		TEST_TIMEOUT_FAIL_END(600000)
    }

	/**
     * Test a join ordering with an inequality, to make sure the inequality gets
     * put as the outermost join
     */
    TEST(JoinOptimizerTest, NonequalityOrderJoinsTest){

        std::vector<LogicalJoinNode> result;
        std::vector<LogicalJoinNode> nodes;
        std::map<std::string, TableStats> stats;
        std::map<std::string, double> filter_selectivities;

        // Create a large set of tables, and add tuples to the tables
		// 300, 301 ..: a,b,c
		// c0 is the primary key
		std::vector<uint64_t> table_ids; 
		for(size_t i=0; i<9; i++) {
			table_ids.push_back(300+i);
		}

		for(size_t i=0; i<9; i++){
			auto num_pages = TestUtils().populate_table(table_ids[i], 100, 2, 32);
			auto table_name = std::string(1, (char)('a'+i)); 
			stats[table_name] = TableStats(table_ids[i], IO_COST, num_pages, 2);
			filter_selectivities[table_name] = 1.0;
		}

		// Query: SELECT COUNT(a.c0) FROM a, b, c, d,e,f,g,h,i 
		// WHERE a.c1 < b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 
		// AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0;

        // Add the nodes to a collection for a query plan
		nodes.push_back(LogicalJoinNode("a", "b", 1, 1, PredicateType::LE));
        nodes.push_back(LogicalJoinNode("b", "c", 0, 0, PredicateType::EQ));
        nodes.push_back(LogicalJoinNode("c", "d", 1, 1, PredicateType::EQ));
        nodes.push_back(LogicalJoinNode("d", "e", 0, 0, PredicateType::EQ));
        nodes.push_back(LogicalJoinNode("e", "f", 1, 1, PredicateType::EQ));
        nodes.push_back(LogicalJoinNode("f", "g", 0, 0, PredicateType::EQ));
        nodes.push_back(LogicalJoinNode("g", "h", 1, 1, PredicateType::EQ));
        nodes.push_back(LogicalJoinNode("h", "i", 0, 0, PredicateType::EQ));
        
		JoinOptimizer j(nodes);
		result = j.order_joins(stats, filter_selectivities);

        // If you're only re-ordering the join nodes,
        // you shouldn't end up with more than you started with
        EXPECT_EQ(result.size(), nodes.size());

        // Make sure that "a" is the outermost table in the join
        EXPECT_TRUE(result[result.size() - 1].right_table == "a"
                || result[result.size() - 1].left_table == "a");
    }
}  // namespace


int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  num_pages = TestUtils().populate_table(table_id, 10200, num_fields, 32);
  stats_map[table_name1] = TableStats(table_id, IO_COST, num_pages, num_fields);
  num_pages2 = TestUtils().populate_table(table_id2, 1000, num_fields, 32);
  stats_map[table_name2] = TableStats(table_id2, IO_COST, num_pages, num_fields);
//   testing::GTEST_FLAG(filter) = "JoinOptimizerTest.BigOrderJoinsTest";
  return RUN_ALL_TESTS();
}


#pragma once
#include <cstddef>
#include <cstdint>
#include "operators/seq_scan.h"
#include "table_stats.h"
#include <map>
#include <vector>
#include <set>
#include <algorithm>
#include <tuple> 

using buzzdb::operators::PredicateType;
using buzzdb::table_stats::TableStats;
namespace buzzdb {
namespace optimizer {

    class LogicalJoinNode{
        public:
        std::string left_table;
        std::string right_table;

        uint64_t left_field;
        uint64_t right_field;

        PredicateType op; 
        bool operator< (const LogicalJoinNode& jn) const{
            return std::tie(left_table, right_table, left_field, right_field) < 
                    std::tie(jn.left_table, jn.right_table, jn.left_field, jn.right_field);
        }
        bool operator==(const LogicalJoinNode &jn) const { 
            return std::tie(left_table, right_table, left_field, right_field) ==
                    std::tie(jn.left_table, jn.right_table, jn.left_field, jn.right_field);
        }

        LogicalJoinNode() = default;
        LogicalJoinNode(std::string left_table, std::string right_table, 
                        uint64_t left_field, uint64_t right_field, 
                        PredicateType op): 
                        left_table(left_table), right_table(right_table), 
                        left_field(left_field), right_field(right_field), 
                        op(op) {}
        ~LogicalJoinNode(){}

        LogicalJoinNode swap_inner_outer(){
            return LogicalJoinNode(right_table, left_table, right_field, left_field, op);
        }

    };

    

    struct CostCard {
        /** The cost of the optimal subplan */
        double cost;
        /** The cardinality of the optimal subplan */
        int card;
        /** The optimal subplan */
        std::vector<LogicalJoinNode> plan;

    };



    class PlanCache {
        
        std::map<std::set<LogicalJoinNode>,std::vector<LogicalJoinNode>> best_orders;
        std::map<std::set<LogicalJoinNode>,double> best_costs;
        std::map<std::set<LogicalJoinNode>,int> best_cardinalities; 
        
    public:
        /** Add a new cost, cardinality and ordering for a particular join set.  Does not verify that the
            new cost is less than any previously added cost -- simply adds or replaces an existing plan for the
            specified join set
            @param s the set of joins for which a new ordering (plan) is being added
            @param cost the estimated cost of the specified plan
            @param card the estimatied cardinality of the specified plan
            @param order the ordering of the joins in the plan
        */
    
        void add_plan(std::set<LogicalJoinNode>& s, double cost, int card, std::vector<LogicalJoinNode>& order) {
            best_orders[s]= order;                        
            best_costs[s]= cost;
            best_cardinalities[s]=card;
        }
        
        /** Find the best join order in the cache for the specified plan 
            @param s the set of joins to look up the best order for
            @return the best order for s in the cache
        */
        std::vector<LogicalJoinNode> get_order(std::set<LogicalJoinNode>& s) {
            if(best_orders.find(s) != best_orders.end())
                return best_orders[s];
            else
                return std::vector<LogicalJoinNode>();
        }
        
        /** Find the cost of the best join order in the cache for the specified plan 
            @param s the set of joins to look up the best cost for
            @return the cost of the best order for s in the cache
        */
        double get_cost(std::set<LogicalJoinNode>& s) {
            return best_costs[s];
        }
        
        /** Find the cardinality of the best join order in the cache for the specified plan 
            @param s the set of joins to look up the best cardinality for
            @return the cardinality of the best order for s in the cache
        */
        int get_card(std::set<LogicalJoinNode>& s) {
            return best_cardinalities[s];
        }
};



class JoinOptimizer{
        public:
            JoinOptimizer(std::vector<LogicalJoinNode> joins);
            JoinOptimizer(){};
            ~JoinOptimizer(){};
            double estimate_join_cost(LogicalJoinNode j, 
                                        uint64_t card1, uint64_t card2, double cost1, double cost2, 
                                        std::map<std::string, TableStats>& stats);
            int estimate_join_cardinality(LogicalJoinNode j, uint64_t card1, uint64_t card2, 
                                            bool t1pkey, bool t2pkey, 
                                            std::map<std::string, TableStats>& stats);
            
            std::vector<LogicalJoinNode> order_joins(UNUSED_ATTRIBUTE std::map<std::string, TableStats> stats,
                                                        UNUSED_ATTRIBUTE std::map<std::string, double> filter_selectivities);
        
        private:
            std::vector<LogicalJoinNode> _joins;
            bool compute_cost_and_card_of_subplan(
                std::map<std::string, TableStats> stats,
                std::map<std::string, double> filter_selectivities,
                LogicalJoinNode join_to_remove, std::set<LogicalJoinNode> join_set,
                double best_cost_so_far, PlanCache pc, CostCard&);
            bool has_Pkey(std::vector<LogicalJoinNode> joinlist);
            bool does_join(std::vector<LogicalJoinNode> joinlist, std::string table_name);
            std::set<std::set<LogicalJoinNode>> enumerate_subsets(std::vector<LogicalJoinNode> v, int size);
    };
}   // namespace optimizer
}  // namespace buzzdb

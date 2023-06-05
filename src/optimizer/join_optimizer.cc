#include "optimizer/join_optimizer.h"
#include "float.h"
#include <chrono>
using namespace std;
using namespace std::chrono;
namespace buzzdb {
namespace optimizer {

    JoinOptimizer::JoinOptimizer(std::vector<LogicalJoinNode> joins){
        _joins = joins;
    }

    /**
     * Estimate the cost of a join.
     * 
     * The cost of the join should be calculated based on any join algorithm 
     *  It should be a function of the amount of data that must be read over 
     * the course of the query, as well as the number of CPU opertions performed by your join. 
     * Assume thatthe cost of a single predicate application is roughly 1.
     * 
     * 
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed.
     * @param card1
     *            Estimated cardinality of the left-hand side of the query
     * @param card2
     *            Estimated cardinality of the right-hand side of the query
     * @param cost1
     *            Estimated cost of one full scan of the table on the left-hand
     *            side of the query
     * @param cost2
     *            Estimated cost of one full scan of the table on the right-hand
     *            side of the query
     * @param stats
     *            The table stats, referenced by table names
     * @return An estimate of the cost of this query, in terms of cost1 and
     *         cost2
     */
    double JoinOptimizer::estimate_join_cost(UNUSED_ATTRIBUTE LogicalJoinNode j, 
                                             UNUSED_ATTRIBUTE uint64_t card1, 
                                             UNUSED_ATTRIBUTE uint64_t card2, 
                                             UNUSED_ATTRIBUTE double cost1, 
                                             UNUSED_ATTRIBUTE double cost2, 
                                             UNUSED_ATTRIBUTE std::map<std::string, TableStats>& stats){
            // Insert your code here.
            /*
             *    Nested Loop Join Cost
             *   joincost(t1 join t2) = scancost(t1) + ntups(t1) x scancost(t2) //IO cost
             *                          + ntups(t1) x ntups(t2)  //CPU cost
             */
            // HINT: You can use j and stats if you want to try other join algorithm
        	double res = cost1 + card1 * cost2 + card1 * card2;
            return res;
    }

    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.
     * 
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed.
     * @param card1
     *            Cardinality of the left-hand table in the join
     * @param card2
     *            Cardinality of the right-hand table in the join
     * @param t1pkey
     *            Is the left-hand table a primary-key table?
     * @param t2pkey
     *            Is the right-hand table a primary-key table?
     * @param stats
     *            The table stats, referenced by table names
     * @return The cardinality of the join
     */
    int JoinOptimizer::estimate_join_cardinality(UNUSED_ATTRIBUTE LogicalJoinNode j, UNUSED_ATTRIBUTE uint64_t card1, UNUSED_ATTRIBUTE uint64_t card2, UNUSED_ATTRIBUTE bool t1pkey, UNUSED_ATTRIBUTE bool t2pkey, UNUSED_ATTRIBUTE std::map<std::string, TableStats>& stats){
        
        if (j.op == PredicateType::EQ) {
            if (t1pkey) {
                return (int) card2;
            } else if (t2pkey) {
                return (int) card1;
            } else if (card1 >= card2) {
                return (int) card1;
            } else {
                return (int) card2;
            }
        } else {
            return (int) (0.3*card1*card2);
        }

    }


    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * docs for hints on how this should be implemented.
     * 
     * @param stats
     *            Statistics for each table involved in the join, referenced by
     *            base table names
     * @param filter_selectivities
     *            Selectivities of the filter predicates on each table in the
     *            join, referenced by table name
     * @return A vector<LogicalJoinNode> that stores joins in the left-deep
     *         order in which they should be executed.
     */
    std::vector<LogicalJoinNode> JoinOptimizer::order_joins(
                UNUSED_ATTRIBUTE std::map<std::string, TableStats> stats,
                UNUSED_ATTRIBUTE std::map<std::string, double> filter_selectivities) {
            
            CostCard bcc = {};
            PlanCache pc;
            int size = _joins.size();
            for (int i = 1; i <= size; i++){
                std::set<std::set<LogicalJoinNode>> subsets = enumerate_subsets(_joins, i);
                for (auto set : subsets){
                    double bestres = std::numeric_limits<double>::max();
                    bcc = {};
                    for (auto ljnode : set){
                        CostCard cc = {};
                        bool flag = compute_cost_and_card_of_subplan(stats, filter_selectivities, ljnode, set, bestres, pc, cc);
                        if (flag){
                            bestres = cc.cost;
                            bcc = cc;
                        }
                        
                    }

                    if (bestres != std::numeric_limits<double>::max()){
                        pc.add_plan(set, bcc.cost, bcc.card, bcc.plan);
                    }
                }
            }
            
            return bcc.plan;
    }


    // helper methods

    /**
     * Helper method to enumerate all of the subsets of a given size of a
     * specified vector.
     * 
     * @param v
     *            The vector whose subsets are desired
     * @param size
     *            The size of the subsets of interest
     * @return a set of all subsets of the specified size
     */

    std::set<std::set<LogicalJoinNode>> JoinOptimizer::enumerate_subsets(std::vector<LogicalJoinNode> v, int size) {
        std::set<std::set<LogicalJoinNode>> res;
        std::vector<bool> bitset(v.size() - size, 0);
        bitset.resize(v.size(), 1);
        std::set<LogicalJoinNode> subset;
        do {
            for (std::size_t i = 0; i != v.size(); ++i) {
                if (bitset[i]) {
                    subset.insert(v[i]);
                }
            }
            res.insert(subset);
            subset.clear();
        } while (std::next_permutation(bitset.begin(), bitset.end()));
    return res;
    }
    /**
     * This is a helper method that computes the cost and cardinality of joining
     * join_to_remove to join_set (join_set should contain join_to_remove), given that
     * all of the subsets of size join_set.size() - 1 have already been computed
     * and stored in PlanCache pc.
     * 
     * @param stats
     *            table stats for all of the tables, referenced by table names
     * @param filter_selectivities
     *            the selectivities of the filters over each of the tables
     * @param join_to_remove
     *            the join to remove from join_set
     * @param join_set
     *            the set of joins being considered
     * @param best_cost_so_far
     *            the best way to join join_set so far (minimum of previous
     *            invocations of computeCostAndCardOfSubplan for this joinSet,
     *            from returned CostCard)
     * @param pc
     *            the PlanCache for this join; should have subplans for all
     *            plans of size joinSet.size()-1
     * @param cc A CostCard objects desribing the cost, cardinality,
     *         optimal subplan. This is populated by the function.
     * @return true if we successfully found a costcard, false otherwise
     */
    
    bool JoinOptimizer::compute_cost_and_card_of_subplan(
            std::map<std::string, TableStats> stats,
            std::map<std::string, double> filter_selectivities,
            LogicalJoinNode join_to_remove, std::set<LogicalJoinNode> join_set,
            double best_cost_so_far, PlanCache pc, CostCard& cc) {

        LogicalJoinNode j = join_to_remove;

        std::vector<LogicalJoinNode> prev_best;

        std::string table1_name = j.left_table;
        std::string table2_name = j.right_table;
        uint64_t left_col = j.left_field;
        uint64_t right_col = j.right_field;
        auto s = join_set;
        s.erase(j);

        double t1_cost, t2_cost;
        uint64_t t1_card, t2_card;
        bool left_Pkey, right_Pkey;

        if (s.empty()) { // base case -- both are base relations
            prev_best =  std::vector<LogicalJoinNode>();
            t1_cost = stats[table1_name].estimate_scan_cost();
            t1_card = stats[table1_name].estimate_table_cardinality(filter_selectivities[table1_name]);
            left_Pkey = (left_col == 0);

            t2_cost = stats[table2_name].estimate_scan_cost();
            t2_card = stats[table2_name].estimate_table_cardinality(filter_selectivities[table2_name]);
            right_Pkey = (right_col == 0);
        } 
        else {
            // s is not empty -- figure best way to join j to s
            prev_best = pc.get_order(s);

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prev_best.size() == 0) {
                return false;
            }

            double prev_best_cost = pc.get_cost(s);
            int best_card = pc.get_card(s);

            // estimate cost of right subtree
            if (does_join(prev_best, table1_name)) { // j.t1 is in prev_best
                t1_cost = prev_best_cost; // left side just has cost of whatever
                                       // left
                // subtree is
                t1_card = best_card;
                left_Pkey = has_Pkey(prev_best);

                t2_cost = stats[table2_name].estimate_scan_cost();
                t2_card = stats[table2_name].estimate_table_cardinality(filter_selectivities[table2_name]);
                right_Pkey = (right_col == 0);
            } else if (does_join(prev_best, table2_name)) { // j.t2 is in prevbest
                                                        // (both
                // shouldn't be)
                t2_cost = prev_best_cost; // left side just has cost of whatever
                                       // left
                // subtree is
                t2_card = best_card;
                right_Pkey = has_Pkey(prev_best);
                t1_cost = stats[table1_name].estimate_scan_cost();
                t1_card = stats[table1_name].estimate_table_cardinality(filter_selectivities[table1_name]);
                left_Pkey = (left_col == 0);

            } else {
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prev_best (cross product)
                return false;
            }
        }

        // case where prevbest is left
        double cost1 = estimate_join_cost(j, t1_card, t2_card, t1_cost, t2_cost, stats);

        LogicalJoinNode j2 = j.swap_inner_outer();
        double cost2 = estimate_join_cost(j2, t2_card, t1_card, t2_cost, t1_cost, stats);
        if (cost2 < cost1) {
            bool tmp;
            j = j2;
            cost1 = cost2;
            tmp = right_Pkey;
            right_Pkey = left_Pkey;
            left_Pkey = tmp;
        }
        if (cost1 >= best_cost_so_far)
            return false;

        
        cc.card = estimate_join_cardinality(j, t1_card, t2_card, left_Pkey,
                right_Pkey, stats);
        cc.cost = cost1;
        cc.plan = (std::vector<LogicalJoinNode>) prev_best;
        cc.plan.push_back(j); // prevbest is left -- add  join to end
        return true;
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    bool JoinOptimizer::does_join(std::vector<LogicalJoinNode> joinlist, std::string table_name) {
        for (LogicalJoinNode j : joinlist) {
            if ((j.left_table == table_name) || (j.right_table == table_name))
                return true;
        }
        return false;
    }


    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    bool JoinOptimizer::has_Pkey(std::vector<LogicalJoinNode> joinlist) {
        for (auto j : joinlist) {
            if ((j.left_field == 0) || (j.right_field == 0))
                return true;
        }
        return false;

    }
}// namespace optimizer
}  // namespace buzzdb
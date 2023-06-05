
#include "optimizer/table_stats.h"
#include <math.h>

namespace buzzdb {
namespace table_stats {


/**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "add_value()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min_val The minimum integer value that will ever be passed to this class for histogramming
     * @param max_val The maximum integer value that will ever be passed to this class for histogramming
     */
    IntHistogram::IntHistogram(UNUSED_ATTRIBUTE int64_t buckets, 
                               UNUSED_ATTRIBUTE int64_t min_val, 
                               UNUSED_ATTRIBUTE int64_t max_val){
                                span = std::max(1.0, (double) ((1.0 + max_val - min_val) / buckets));
                                min_v = min_val;
                                max_v = max_val;
                                NumB = buckets;
                                ntups = 0;
                                umap = std::vector<int> (NumB, 0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param val Value to add to the histogram
     */
    void IntHistogram::add_value(UNUSED_ATTRIBUTE int64_t val){
        if (val < min_v || val > max_v){
            return;
        }
        int b_idx = std::min((int) ((val - min_v) / span), NumB - 1);

        umap[b_idx] += 1;

        ntups += 1;
    }      
    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    double IntHistogram::estimate_selectivity(UNUSED_ATTRIBUTE PredicateType op, 
                                              UNUSED_ATTRIBUTE int64_t v){
        double sel;

        if (op == PredicateType::EQ){  

            if (v < min_v || v > max_v){
                return 0.0;
            }

            int b_idx = std::min((int) ((v - min_v) / span), NumB - 1);
        
            int h = umap[b_idx];       
            sel = (h / span) / ntups;

            return sel;
        }
        else if (op == PredicateType::NE){
            sel = 1.0 - estimate_selectivity(PredicateType::EQ, v);

            return sel;
        }
        else if(op == PredicateType::GT){
            if(v <= min_v){
                return 1.0;
            }

            if(v >= max_v){
                return 0.0;
            }

            int b_idx = std::min((int) ((v - min_v) / span), NumB - 1);
        
            int h = umap[b_idx];

            sel = (h / ntups) * ((min_v + (b_idx +1) * span - v) / span);
            int cnt = 0;
            for (int i = b_idx + 1; i < NumB; i++){           
                cnt += umap[i];
            }
            
            return sel + 1.0 * cnt / ntups;
        }
        else if(op == PredicateType::GE){
            if(v < min_v){
                return 1.0;
            }

            if(v > max_v){
                return 0.0;
            }

            return estimate_selectivity(PredicateType::GT, v - 1);
        }
        else if (op == PredicateType::LT){
            if(v <= min_v){
                return 0.0;
            }

            if(v >= max_v){
                return 1.0;
            }

            return 1.0 - estimate_selectivity(PredicateType::GE, v);
        }
        else if(op == PredicateType::LE){
            if(v < min_v){
                return 0.0;
            }

            if(v > max_v){
                return 1.0;
            }

            return 1.0 - estimate_selectivity(PredicateType::GT, v);
        }
        
        return -1.0;
    }
        

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param table_id
     *            The table over which to compute statistics
     * @param io_cost_per_page
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     * @param num_pages
     *            The number of disk pages spanned by the table 
     * @param num_fields
     *            The number of columns in the table 
     */
    TableStats::TableStats(UNUSED_ATTRIBUTE int64_t table_id, UNUSED_ATTRIBUTE int64_t io_cost_per_page, 
                    UNUSED_ATTRIBUTE uint64_t num_pages, UNUSED_ATTRIBUTE uint64_t num_fields){

    /*
        // some code goes here
        Hint: use seqscan operator (operators/seq_scan.h) to scan over the table
        and build stats. You should try to do this reasonably efficiently, but you don't
        necessarily have to (for example) do everything in a single scan of the table.
    */
        buzzdb::operators::SeqScan scan(table_id, num_pages, num_fields);
        scan.open();
        io_cost = io_cost_per_page;
        nump = num_pages;
        numf = num_fields;
        for (int k = 0; k < numf; k++){
            max_value.push_back((-1) * std::numeric_limits<int>::max());
            min_value.push_back(std::numeric_limits<int>::max());
        }
        
        std::vector<int> tup(numf);

        while (scan.has_next()) {
            tup = scan.get_tuple();
            for(int i = 0; i < numf; i++){
                if (tup[i] > max_value[i]){
                    max_value[i] = tup[i];
                }

                if (tup[i] < min_value[i]){
                    min_value[i] = tup[i];
                }
                
            }

            num_tups += 1;
            
        }

        scan.reset();
        tup = std::vector<int>(numf);

        for(int i = 0; i < numf; i++){
            IntHistogram hist(NUM_HIST_BINS, min_value[i], max_value[i]);
            histmap[i] = hist;    
        }
        

        while (scan.has_next()) {
            tup = scan.get_tuple();
            for(int i = 0; i < numf; i++){
                histmap[i].add_value((int)tup[i]);
            }
            
        }

        scan.close();

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is io_cost_per_page. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */

    double TableStats::estimate_scan_cost(){
        double res = static_cast<double> (io_cost * nump);
        return res * 2;
    }


    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivity_factor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivity_factor
     */
    uint64_t TableStats::estimate_table_cardinality(UNUSED_ATTRIBUTE double selectivity_factor){
        auto t = static_cast<int>(selectivity_factor * num_tups);
        return t;
    }


    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    double TableStats::estimate_selectivity(UNUSED_ATTRIBUTE int64_t field, 
                                            UNUSED_ATTRIBUTE PredicateType op, 
                                            UNUSED_ATTRIBUTE int64_t constant){
        int idx = static_cast<int>(field);
        double res = histmap[idx].estimate_selectivity(op, constant);
        return res;
    }

}  // namespace table_stats
}  // namespace buzzdb
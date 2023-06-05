#pragma once

#include <atomic>
#include <map>
#include <vector>

#include "buffer/buffer_manager.h"
#include "log/log_manager.h"
#include "common/macros.h"

namespace buzzdb {

class Transaction{
public:
    uint64_t txn_id_ = INVALID_TXN_ID;
    bool started_ = false;

    /// pages modified by the transaction
    std::vector<uint64_t> modified_pages_;

    /// Constructor
    Transaction();
    Transaction(uint64_t txn_id, bool started);

    uint64_t get_txn_id(){ 	return txn_id_; }
};

class TransactionManager {

public:
    /// Constructor.
	/// @param[in] log_manager   The log manager that should be used by the
	/// transaction manager to store all log records
	/// @param[in] buffer_manager The buffer manager that should be used by the
	/// transaction manager to manage all the pages
	TransactionManager(LogManager &log_manager, BufferManager &buffer_manager);

    /// Destructor.
    ~TransactionManager();

    /// Start the transaction
    uint64_t start_txn();

    /// Commit the transaction
    void commit_txn(uint64_t txn_id);

    /// Abort the transaction
    void abort_txn(uint64_t txn_id);

    /// add modified page to the transaction 
    void add_modified_page(uint64_t txn_id, uint64_t page_id);

    /// reset the state
    /// this is used to simulate crash
    void reset(LogManager &log_manager);
private:

    /// Log manager
    LogManager &log_manager_;

    /// Buffer manager
    BufferManager &buffer_manager_;

    /// To obtain a new txn id for each transaction
    std::atomic<uint64_t> transaction_counter_;

    /// List of transactions
    std::map<uint64_t, Transaction> transaction_table_;

};

}  // namespace buzzdb

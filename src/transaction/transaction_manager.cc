
#include <iostream>

#include "transaction/transaction_manager.h"
#include "common/macros.h"

namespace buzzdb {

Transaction::Transaction():
				txn_id_(INVALID_TXN_ID),
				started_(false){
}

Transaction::Transaction(uint64_t txn_id, bool started):
				txn_id_(txn_id),
				started_(started){
}

TransactionManager::TransactionManager(LogManager &log_manager,
		BufferManager &buffer_manager):
				log_manager_(log_manager),
				buffer_manager_(buffer_manager),
				transaction_counter_(0){
}

TransactionManager::~TransactionManager(){
}

void TransactionManager::reset(LogManager &log_manager){
	log_manager_ = log_manager;
	buffer_manager_.discard_all_pages();
	transaction_counter_ = 0;
	transaction_table_.clear();
}

/// Start the transaction
uint64_t TransactionManager::start_txn(){

	uint64_t txn_id = ++transaction_counter_;

	/// Create a transaction
	Transaction txn(txn_id, true);
	transaction_table_[txn_id] = txn;

	/// Add a txn begin log record
	log_manager_.log_txn_begin(txn_id);

	return txn_id;
}

/// Commit the transaction
void TransactionManager::commit_txn(uint64_t txn_id){

	if(transaction_table_.count(txn_id) == 0){
		std::cout << "Txn does not exist \n";
		exit(-1);
	}

	auto& txn = transaction_table_[txn_id];

	if (txn.started_) {

		// flush all the dirty pages associated with this transaction out
		for(auto page_id : txn.modified_pages_){
			buffer_manager_.flush_page(page_id);
		}

		log_manager_.log_commit(txn_id);

	    txn.started_ = false;
	}

}

/// Abort the transaction
void TransactionManager::abort_txn(UNUSED_ATTRIBUTE uint64_t txn_id){

	if(transaction_table_.count(txn_id) == 0){
		std::cout << "Txn does not exist \n";
		exit(-1);
	}

	auto& txn = transaction_table_[txn_id];

	if (txn.started_) {

		// discard all the dirty pages associated with this transaction
		for(auto page_id : txn.modified_pages_){
			buffer_manager_.discard_page(page_id);
		}

		log_manager_.log_abort(txn_id, buffer_manager_);

	    txn.started_ = false;
	}

}

void TransactionManager::add_modified_page(uint64_t txn_id, uint64_t page_id){
	auto &txn = transaction_table_[txn_id];
	txn.modified_pages_.push_back(page_id);
}

}  // namespace buzzdb

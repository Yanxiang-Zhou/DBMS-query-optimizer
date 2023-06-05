#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <unordered_map>
#include <vector>
#include <memory>
#include <atomic>

namespace buzzdb {

class BufferFrame {
private:
    friend class BufferManager;

    uint64_t frame_id;
    uint64_t page_id;
    std::vector<char> data;

	bool dirty;

public:
    /// Returns a pointer to this page's data.
    char* get_data();
    
};


class buffer_full_error
: public std::exception {
public:
    const char* what() const noexcept override {
        return "buffer is full";
    }
};


class BufferManager {

public:
    BufferManager() = default;
    /// Constructor.
    /// @param[in] page_size  Size in bytes that all pages will have.
    /// @param[in] page_count Maximum number of pages that should reside in
    //                        memory at the same time.
    BufferManager(size_t page_size, size_t page_count);

    /// Destructor. Writes all dirty pages to disk.
    ~BufferManager();

    /// Returns size of a page
    size_t get_page_size() { return page_size_; }

    /// Returns a reference to a `BufferFrame` object for a given page id. When
    /// the page is not loaded into memory, it is read from disk. Otherwise the
    /// loaded page is used.
    /// When the page cannot be loaded because the buffer is full, throws the
    /// exception `buffer_full_error`.
    /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
    /// `unfix_page()`.
    /// @param[in] page_id   Page id of the page that should be loaded.
    /// @param[in] exclusive If `exclusive` is true, the page is locked
    ///                      exclusively. Otherwise it is locked
    ///                      non-exclusively (shared).
    BufferFrame& fix_page(uint64_t page_id, bool exclusive);

    /// Takes a `BufferFrame` reference that was returned by an earlier call to
    /// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
    /// written back to disk eventually.
    void unfix_page(BufferFrame& page, bool is_dirty);

    /// Returns the page ids of all pages (fixed and unfixed) that are in the
    /// FIFO list in FIFO order.
    /// Is not thread-safe.
    std::vector<uint64_t> get_fifo_list() const;

    /// Returns the page ids of all pages (fixed and unfixed) that are in the
    /// LRU list in LRU order.
    /// Is not thread-safe.
    std::vector<uint64_t> get_lru_list() const;

    /// Returns the segment id for a given page id which is contained in the 16
    /// most significant bits of the page id.
    static constexpr uint16_t get_segment_id(uint64_t page_id) {
        return page_id >> 48;
    }

    /// Returns the page id within its segment for a given page id. This
    /// corresponds to the 48 least significant bits of the page id.
    static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
        return page_id & ((1ull << 48) - 1);
    }

    /// Returns the overall page id associated with a segment id and
    /// a given segment page id.
    static uint64_t get_overall_page_id(uint16_t segment_id,
                                        uint64_t segment_page_id) {
        return (static_cast<uint64_t>(segment_id) << 48) | segment_page_id;
    }

    void  flush_page(uint64_t page_id);

    void  discard_page(uint64_t page_id);

    void  flush_all_pages();

    void  discard_all_pages();

    /// Returns the frame id of the frame containing the page if it is
    /// present in the buffer
    /// Otherwise, returns INVALID_FRAME_ID
    uint64_t get_frame_id_of_page(uint64_t page_id);

private:
    size_t capacity_;

	size_t page_size_;

    std::vector<std::unique_ptr<BufferFrame>> pool_;

    uint64_t page_counter_ = 0;

    void read_frame(uint64_t frame_id);

    void write_frame(uint64_t frame_id);

};


}  // namespace buzzdb

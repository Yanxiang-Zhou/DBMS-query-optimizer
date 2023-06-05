#pragma once

#include <vector>

#include "storage/file.h"

namespace buzzdb {

class TestFile : public File {
 private:
  Mode mode;
  std::vector<char> file_content;

 public:
  explicit TestFile(Mode mode = WRITE) : mode(mode) {}

  explicit TestFile(std::vector<char>&& file_content, Mode mode = READ)
      : mode(mode), file_content(std::move(file_content)) {}

  TestFile(const TestFile&) = default;
  TestFile(TestFile&&) = default;

  ~TestFile() override = default;

  TestFile& operator=(const TestFile&) = default;
  TestFile& operator=(TestFile&&) = default;

  std::vector<char>& get_content() { return file_content; }

  Mode get_mode() const override;

  size_t size() const override;

  void resize(size_t new_size) override;

  void read_block(size_t offset, size_t size, char* block);

  void write_block(const char* block, size_t offset, size_t size);
};

}  // namespace buzzdb

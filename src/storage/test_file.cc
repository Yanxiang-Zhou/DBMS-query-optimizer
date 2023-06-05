
#include <fcntl.h>
#include <stdlib.h>  // NOLINT
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <memory>
#include <system_error>

#include "storage/test_file.h"

namespace buzzdb {

class TestFileError : public std::exception {
 private:
  const char* message;

 public:
  explicit TestFileError(const char* message) : message(message) {}

  ~TestFileError() override = default;

  const char* what() const noexcept override { return message; }
};

File::Mode TestFile::get_mode() const { return mode; }

size_t TestFile::size() const { return file_content.size(); }

void TestFile::resize(size_t new_size) {
  if (mode == READ) {
    throw TestFileError{"trying to resize a read only file"};
  }
  file_content.resize(new_size);
}

void TestFile::read_block(size_t offset, size_t size, char* block) {
  if (offset + size > file_content.size()) {
    throw TestFileError{"trying to read past end of file"};
  }
  std::memcpy(block, file_content.data() + offset, size);
}

void TestFile::write_block(const char* block, size_t offset, size_t size) {
  if (mode == READ) {
    throw TestFileError{"trying to write to a read only file"};
  }
  if (offset + size > file_content.size()) {
    throw TestFileError{"trying to write past end of file"};
  }
  std::memcpy(file_content.data() + offset, block, size);
}

}  // namespace buzzdb

#ifndef WRITABLEFILE_H
#define WRITABLEFILE_H

#include <fcntl.h>
#include "slice.h"
#include "../src/status.h"

constexpr const size_t kWritableFileBufferSize = 65536;

static std::string Dirname(const std::string& filename) {
	std::string::size_type separator_pos = filename.rfind('/');
	if (separator_pos == std::string::npos) {
		return { "." };
	}

	assert(filename.find('/', separator_pos + 1) == std::string::npos);

	return filename.substr(0, separator_pos);
}

class WritableFile {
private:
	Status FlushBuffer() {
		Status status = WriteUnbuffered(buf, pos);
		pos = 0;
		return status;
	}

	Status WriteUnbuffered(const char* data, size_t size) {
		while (size > 0) {
			ssize_t write_result = ::write(_fd, data, size);
			if (write_result < 0) {
				if (errno == EINTR) {
					continue;  // Retry
				}
				return Status::IOError("WriteUnbuffered");
			}
			data += write_result;
			size -= write_result;
		}
		return Status::OK();
	}

	static Status SyncFd(int fd, const std::string& fd_path) {
		bool sync_success = ::fdatasync(fd) == 0;

		if (sync_success) {
			return Status::OK();
		}
		return Status::IOError(fd_path);
	}

	char buf[kWritableFileBufferSize];
	size_t pos;
	int _fd;

	const std::string filename;
	const std::string dirname;
public:
	WritableFile(std::string filename, int fd)
		: pos(0),
		  _fd(fd),
		  filename(std::move(filename)),
		  dirname(Dirname(filename)) {
	}
	WritableFile(const WritableFile&) = delete;
	WritableFile& operator=(const WritableFile&) = delete;

	~WritableFile() {
		if (_fd >= 0) {
			Close();
		}
	}

	Status Append(const Slice& data) {
		size_t write_size = data.size();
		const char* write_data = data.data();

		size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos);
		std::memcpy(buf + pos, write_data, copy_size);
		write_data += copy_size;
		write_size -= copy_size;
		pos += copy_size;
		if (write_size == 0) {
			return Status::OK();
		}

		// Can't fit in buffer, so need to do at least one write.
		Status status = FlushBuffer();
		if (!status.ok()) {
			return status;
		}

		// Small writes go to buffer, large writes are written directly.
		if (write_size < kWritableFileBufferSize) {
			std::memcpy(buf, write_data, write_size);
			pos = write_size;
			return Status::OK();
		}
		return WriteUnbuffered(write_data, write_size);
	}
	Status Close() {
		Status status = FlushBuffer();
		const int close_result = ::close(_fd);
		if (close_result < 0 && status.ok()) {
			status = Status::IOError(filename);
		}
		_fd = -1;
		return status;
	}
	Status Flush() {
		return FlushBuffer();
	}
	Status Sync() {
		Status status = FlushBuffer();
		if (!status.ok()) {
			return status;
		}

		return SyncFd(_fd, filename);
	}
};

Status NewWritableFile(const std::string& filename, WritableFile** result) {
	int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
	if (fd < 0) {
		*result = nullptr;
	}

	// 创建一个 PosixWritableFile 对象
	*result = new WritableFile(filename, fd);
	return Status::OK();
}

Status NewAppendableFile(const std::string& filename,
	WritableFile** result) {
	int fd = ::open(filename.c_str(),
		O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
	if (fd < 0) {
		*result = nullptr;
		return Status::IOError(filename);
	}

	*result = new WritableFile(filename, fd);
	return Status::OK();
}

#endif //WRITABLEFILE_H

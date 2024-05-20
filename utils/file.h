#ifndef WRITABLEFILE_H
#define WRITABLEFILE_H

#include <fcntl.h>
#include "slice.h"
#include "../src/include/status.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>

#ifndef __Fuchsia__

#include <sys/resource.h>

#endif

#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

namespace LSMKV {
    constexpr const size_t kWritableFileBufferSize = 65536;
    constexpr const size_t PAGE_SIZE = 4096;
    constexpr const int kOpenBaseFlags = O_CLOEXEC;

    static inline std::string Dirname(const std::string &filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
            return {"."};
        }

        assert(filename.find('/', separator_pos + 1) == std::string::npos);

        return filename.substr(0, separator_pos);
    }

    class MemoryReadableFile {
    public:
        MemoryReadableFile(std::string filename, char *mmap_base, size_t length, size_t offset)
                : mmap_base_(mmap_base),
                  length_(length),
                  offset_(offset),
                  filename_(std::move(filename)) {
        }

        ~MemoryReadableFile() {
            ::munmap(static_cast<void *>(mmap_base_), length_);
        }

        size_t Size() const {
            return length_;
        }

        size_t GetOffset() const {
            return offset_;
        }

        bool Read(uint64_t offset, size_t n, Slice *result,
                  char *scratch) const {
            *result = Slice(mmap_base_ + offset, n);
            return true;
        }

    private:
        char *const mmap_base_;
        const size_t length_;
        const size_t offset_;
        const std::string filename_;
    };

    class SequentialFile {
    public:
        SequentialFile(std::string filename, int fd)
                : fd_(fd), filename_(std::move(filename)) {
        }

        ~SequentialFile() {
            close(fd_);
        }

        Status ReadAll(Slice *result, char *scratch) {
            struct stat statbuf;
            Status status;
            stat(filename_.c_str(), &statbuf);
            size_t length = statbuf.st_size;
            if (length == 0) {
                return Status::IOError("empty");
            }
            ::ssize_t read_size = ::read(fd_, scratch, length);
            if (read_size < 0) {  // Read error.
                status = Status::IOError(filename_);
            }
            *result = Slice(scratch, read_size);
            return Status::OK();
        }

        Status Read(size_t n, Slice *result, char *scratch) {
            Status status;
            while (true) {
                ::ssize_t read_size = ::read(fd_, scratch, n);
                if (read_size < 0) {  // Read error.
                    if (errno == EINTR) {
                        continue;  // Retry
                    }
                    status = Status::IOError(filename_);
                    break;
                }
                *result = Slice(scratch, read_size);
                break;
            }
            return status;
        }

        Status MoveTo(uint64_t n) {
            if (::lseek(fd_, n, SEEK_SET) == static_cast<off_t>(-1)) {
                return Status::IOError(filename_);
            }
            return Status::OK();
        }


        Status Skip(uint64_t n) {
            if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
                return Status::IOError(filename_);
            }
            return Status::OK();
        }

    private:
        const int fd_;
        const std::string filename_;
    };

    static inline Status NewSequentialFile(const std::string &filename,
                                           SequentialFile **result) {
        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            *result = nullptr;
            return Status::IOError(filename);
        }

        *result = new SequentialFile(filename, fd);
        return Status::OK();
    }

    class RandomReadableFile {
    public:
        RandomReadableFile(std::string filename, int fd)
                : fd_(fd),
                  filename_(std::move(filename)) {
        }

        ~RandomReadableFile() {
            ::close(fd_);
        }

        Status Read(uint64_t offset, size_t n, Slice *result,
                    char *scratch) const {
            int fd = fd_;
            fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
            if (fd < 0) {
                return Status::IOError("Failed to open file");
            }

            Status status;
            ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
            *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
            if (read_size < 0) {
                // An error: return a non-ok status.
                status = Status::IOError("Failed to read from file");
            }
            assert(fd != fd_);
            ::close(fd);
            return status;
        }

    private:
        const int fd_;
        const std::string filename_;
    };

    class WritableFile {
    private:
        Status FlushBuffer() {
            Status status = WriteUnbuffered(buf, pos);
            pos = 0;
            return status;
        }

        static Status SyncFd(int fd, const std::string &fd_path) {
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
        char *WriteToBuffer(size_t size) {
            if (size > kWritableFileBufferSize - pos) {
                FlushBuffer();
            }
            pos += size;
            return buf + pos - size;
        }

        Status WriteUnbuffered(const Slice &s) {
            auto size = s.size();
            auto data = s.data();
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

        Status WriteUnbuffered(const char *data, size_t size) {
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


        WritableFile(std::string filename, int fd)
                : pos(0),
                  _fd(fd),
                  filename(std::move(filename)),
                  dirname(Dirname(filename)) {
        }

        WritableFile(const WritableFile &) = delete;

        WritableFile &operator=(const WritableFile &) = delete;

        ~WritableFile() {
            if (_fd >= 0) {
                Close();
            }
        }

        Status Append(const Slice &data) {
            size_t write_size = data.size();
            const char *write_data = data.data();

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


    class WritableNoBufFile {
    private:
        static Status SyncFd(int fd, const std::string &fd_path) {
            bool sync_success = ::fdatasync(fd) == 0;

            if (sync_success) {
                return Status::OK();
            }
            return Status::IOError(fd_path);
        }

        size_t pos;
        int _fd;

        const std::string filename;
        const std::string dirname;
    public:
        Status WriteUnbuffered(const Slice &s) {
            auto size = s.size();
            auto data = s.data();
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

        Status WriteUnbuffered(const char *data, size_t size) {
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


        WritableNoBufFile(std::string filename, int fd)
                : pos(0),
                  _fd(fd),
                  filename(std::move(filename)),
                  dirname(Dirname(filename)) {
        }

        WritableNoBufFile(const WritableNoBufFile &) = delete;

        WritableNoBufFile &operator=(const WritableNoBufFile &) = delete;

        ~WritableNoBufFile() = default;

    };

    static inline bool FileExists(const std::string &filename) {
        return ::access(filename.c_str(), F_OK) == 0;
    }

    static inline Status NewRandomReadableFile(const std::string &filename, RandomReadableFile **result) {
        *result = nullptr;
        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            return Status::IOError(filename);
        }

        *result = new RandomReadableFile(filename, fd);
        return Status::OK();
    }

    static inline off64_t GetFileSize(const std::string &filePath) {
        struct stat64 stat_buf{};
        int rc = stat64(filePath.c_str(), &stat_buf);
        if (rc != 0) {
            return -1;
        }
        return stat_buf.st_size;
    }

    static inline bool
    NewRandomMemoryReadFile(const std::string &filename, size_t size, size_t offset, MemoryReadableFile **result) {
        *result = nullptr;

        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            return false;
        }

        auto file_size = GetFileSize(filename);

        assert(file_size >= 0);
        size_t real_size;

        if (file_size > size) {
            auto new_offset = file_size - size + PAGE_SIZE;
            offset = std::min(offset, new_offset);
            offset = (offset) / PAGE_SIZE * PAGE_SIZE;
            real_size = size;

        } else {
            offset = 0;
            real_size = file_size;
        }


        void *mmap_base =
                ::mmap(/*addr=*/nullptr, size, PROT_READ, MAP_SHARED, fd, offset);
        if (mmap_base != MAP_FAILED) [[likely]] {
            *result = new MemoryReadableFile(filename,
                                             reinterpret_cast<char *>(mmap_base),
                                             real_size, offset);
            ::close(fd);
            return true;
        }

        return false;
    }

    static inline Status NewWritableNoBufFile(const std::string &filename, WritableNoBufFile **result) {
        int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
        }

        *result = new WritableNoBufFile(filename, fd);
        return Status::OK();
    }

    static inline Status NewWritableFile(const std::string &filename, WritableFile **result) {
        int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
        }

        *result = new WritableFile(filename, fd);
        return Status::OK();
    }

    static inline Status NewAppendableFile(const std::string &filename,
                                           WritableFile **result) {
        int fd = ::open(filename.c_str(),
                        O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
            return Status::IOError(filename);
        }

        *result = new WritableFile(filename, fd);
        return Status::OK();
    }

    static inline Status NewWriteAtStartFile(const std::string &filename,
                                             WritableNoBufFile **result) {
        int fd = ::open(filename.c_str(),
                        O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        ::lseek(fd, 0, SEEK_SET);
        if (fd < 0) {
            *result = nullptr;
            return Status::IOError(filename);
        }

        *result = new WritableNoBufFile(filename, fd);
        return Status::OK();
    }
}

#endif //WRITABLEFILE_H

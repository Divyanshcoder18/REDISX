#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <string>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <chrono>

using namespace std;

// Error handler: prints system errors and exits
inline void die(const string& message) {
    cerr << message << " failed: " << strerror(errno) << "\n";
    exit(1);
}

// Configures a file descriptor/socket to non-blocking mode
inline void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        die("fcntl F_GETFL");
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        die("fcntl F_SETFL O_NONBLOCK");
    }
}

// Returns the current Unix timestamp in milliseconds
inline uint64_t current_time_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

#endif // UTILS_H

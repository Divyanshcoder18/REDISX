#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <cstdlib>
#include <vector>
#include <poll.h>
#include <fcntl.h>

using namespace std;

// Helper function to handle errors cleanly
void die(const string& message) {
    cerr << message << " failed: " << strerror(errno) << "\n";
    exit(1);
}

// Utility to set a file descriptor to non-blocking mode
void set_nonblocking(int fd) {
    // 1. Get current configuration flags of the socket
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        die("fcntl F_GETFL");
    }
    // 2. Add the O_NONBLOCK flag to make it non-blocking
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        die("fcntl F_SETFL O_NONBLOCK");
    }
}

int main() {
    // ==========================================
    // STEP 1: CREATE THE MAIN SERVER SOCKET
    // ==========================================
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        die("Socket creation");
    }

    // Set the main listening socket to non-blocking
    set_nonblocking(server_fd);

    // Allow instant restart of the server (SO_REUSEADDR)
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        die("Setting socket options");
    }

    // ==========================================
    // STEP 2: BIND SOCKET TO PORT 6379
    // ==========================================
    struct sockaddr_in address{}; 
    address.sin_family = AF_INET;         // IPv4 Address family
    address.sin_addr.s_addr = INADDR_ANY; // Listen on all network interfaces
    address.sin_port = htons(6379);      // Port 6379 in Network Byte Order

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        die("Binding socket");
    }

    cout << "Socket successfully bound to Port 6379!\n";

    // ==========================================
    // STEP 3: START LISTENING FOR CLIENTS
    // ==========================================
    if (listen(server_fd, 10) < 0) {
        die("Listen");
    }
    cout << "Server is listening on port 6379...\n";

    // ==========================================
    // STEP 4: SETUP THE EVENT WATCH LIST (poll)
    // ==========================================
    // We use a vector to store all the sockets we want to monitor.
    vector<struct pollfd> fds;

    // Register our main server socket first.
    struct pollfd server_pfd{};
    server_pfd.fd = server_fd;
    server_pfd.events = POLLIN; // Monitor for incoming connection events
    fds.push_back(server_pfd);

    // ==========================================
    // STEP 5: THE EVENT LOOP
    // ==========================================
    while (true) {
        // poll() blocks the program until there is active network traffic
        int num_events = poll(fds.data(), fds.size(), -1);
        if (num_events < 0) {
            if (errno == EINTR) continue; // If interrupted, try again
            die("poll");
        }

        // ------------------------------------------
        // CASE A: NEW CLIENT IS CONNECTING
        // ------------------------------------------
        // fds[0] is the main listening socket. If its POLLIN light is on:
        if (fds[0].revents & POLLIN) {
            struct sockaddr_in client_addr{}; //// store the client info  for ex ip add and port 
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
            
            if (client_fd >= 0) {
                // Set the new client socket to non-blocking
                set_nonblocking(client_fd);

                // Add the new client socket to our watch list (fds)
                struct pollfd client_pfd{};
                client_pfd.fd = client_fd;
                client_pfd.events = POLLIN; // Monitor for readable messages
                fds.push_back(client_pfd);
                cout << "Client connected! Client FD = " << client_fd << "\n";
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                cerr << "Accept failed: " << strerror(errno) << "\n";
            }
        }

        // ------------------------------------------
        // CASE B: EXISTING CLIENT SENT DATA OR LEFT
        // ------------------------------------------
        // We start at index 1 because index 0 is our main server socket
        for (size_t i = 1; i < fds.size(); ) {
            bool socket_closed = false;

            // Check if this client socket is active (light is on)
            if (fds[i].revents & POLLIN) {
                char buffer[1024];
                int bytes_read = read(fds[i].fd, buffer, sizeof(buffer) - 1);
                
                if (bytes_read < 0) {
                    // Check if it's a real error (not just "no data right now")
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        cerr << "Read failed on Client FD = " << fds[i].fd << ": " << strerror(errno) << "\n";
                        close(fds[i].fd);
                        fds.erase(fds.begin() + i); // Remove from watch list
                        socket_closed = true;
                    }
                } 
                else if (bytes_read == 0) {
                    // Client closed the connection
                    cout << "Client disconnected. Client FD = " << fds[i].fd << "\n";
                    close(fds[i].fd);
                    fds.erase(fds.begin() + i); // Remove from watch list
                    socket_closed = true;
                } 
                else {
                    // We received actual data! Null-terminate and echo it back.
                    buffer[bytes_read] = '\0';
                    cout << "Received: " << buffer;
                    if (write(fds[i].fd, buffer, bytes_read) < 0) {
                        cerr << "Write failed on Client FD = " << fds[i].fd << ": " << strerror(errno) << "\n";
                    }
                }
            }

            // Only increment 'i' if we did NOT erase the socket at index 'i'
            if (!socket_closed) {
                i++;
            }
        }
    }

    close(server_fd);
    return 0;
}

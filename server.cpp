#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <cstdlib> // For exit()

using namespace std;

// Simple helper function to handle fatal errors cleanly
void die(const string& message) {
    cerr << message << " failed: " << strerror(errno) << "\n";
    exit(1);
}

int main() {
    // 1. Create socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        die("Socket creation");
    }

    // 2. Set socket options (Allow instant restarts)
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        die("Setting socket options");
    }

    // 3. Setup address structure (zero-initialized automatically using {}) // binding to the ip and port no 
    struct sockaddr_in address{}; 
    address.sin_family = AF_INET; // IPv4 Address family
    address.sin_addr.s_addr = INADDR_ANY; // Listen on any incoming IP interface (0.0.0.0)
    address.sin_port = htons(6379); // Bind to Port 6379 in Network Byte Order

    // 4. Bind the socket 
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        die("Binding socket");
    }

    cout << "Socket successfully bound to Port 6379!\n";

    // 5. Listen for incoming connections (backlog queue size = 10)
    if (listen(server_fd, 10) < 0) {
        die("Listen");
    }
    cout << "Server is listening on port 6379...\n";

    // Outer Loop: Continuously accept clients one after the other
    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        
        cout << "Waiting for a new connection...\n";
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            cerr << "Accept failed: " << strerror(errno) << "\n";
            continue; // Go back to top and try to accept the next client
        }
        
        cout << "Client connected! Client FD = " << client_fd << "\n";

        // Inner Loop: Handle communication with the currently connected client
        char buffer[1024]; // 1 KB buffer
        while (true) {
            int bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
            
            if (bytes_read < 0) {
                cerr << "Read failed: " << strerror(errno) << "\n";
                break; // Break inner loop to close this client
            }
            
            if (bytes_read == 0) {
                cout << "Client disconnected.\n";
                break; // Break inner loop to close this client
            }
            
            buffer[bytes_read] = '\0';
            cout << "Received: " << buffer;
            
            write(client_fd, buffer, bytes_read);
        }

        // Clean up connection with this specific client
        close(client_fd);
        cout << "Closed connection with Client FD = " << client_fd << "\n\n";
    }

    // Clean up server socket (technically unreachable now, but good practice)
    close(server_fd);
    return 0;
}

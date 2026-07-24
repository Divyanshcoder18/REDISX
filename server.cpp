// ==================================================================
// SECTION 1: HEADER FILES (Zaroori Libraries Import Kar Rahe Hain)
// ==================================================================
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <vector>
#include <poll.h>

// Humari custom modular files include kar rahe hain
#include "utils.h"
#include "hashtable.h"
#include "parser.h"
#include "threadpool.h"

using namespace std;

// Global instances
RedisDb g_db;
ThreadPool g_pool(4);
int g_aof_fd = -1; // Append-Only File descriptor (Save file handle)

// ==================================================================
// SECTION 4B: AOF PERSISTENCE ENGINE (Durability & Crash Recovery)
// ==================================================================

// Command list ko wapas RESP format me translate karke disk par append karta hai
void aof_log(int fd, const vector<string>& cmd) {
    if (fd < 0 || cmd.empty()) return;

    // Command array ka size header: *<count>\r\n
    string resp = "*" + to_string(cmd.size()) + "\r\n";
    for (const string& arg : cmd) {
        // Har word ki length aur value: $<len>\r\n<value>\r\n
        resp += "$" + to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }

    // Disk par file me write kar rahe hain
    if (write(fd, resp.data(), resp.length()) < 0) {
        cerr << "[AOF Error] Failed to write to AOF file\n";
    }
}

// Server start hote hi AOF file padh kar database recover karne waala loader
void aof_load(const string& filename) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        // Agar file nahi hai to server pehli baar chal raha hai (Normal condition)
        return;
    }

    string buffer;
    char read_buf[4096];
    ssize_t bytes_read;

    // Poori file ko read karke hum buffer string me daal lete hain
    while ((bytes_read = read(fd, read_buf, sizeof(read_buf))) > 0) {
        buffer.append(read_buf, bytes_read);
    }
    close(fd);

    // Buffer me se standard parse_request() ka use karke commands recover karenge
    vector<string> cmd;
    while (true) {
        int parse_res = parse_request(buffer, cmd);
        if (parse_res == 1) {
            // Agar complete command decode hui, to direct g_db par execute karenge
            if (cmd[0] == "SET" && cmd.size() > 2) {
                db_set(g_db, cmd[1], cmd[2]);
                if (cmd.size() >= 5 && cmd[3] == "EX") {
                    db_expire(g_db, cmd[1], stoull(cmd[4]));
                }
            } 
            else if (cmd[0] == "DEL" && cmd.size() > 1) {
                db_del(g_db, cmd[1]);
            }
            else if (cmd[0] == "EXPIRE" && cmd.size() > 2) {
                db_expire(g_db, cmd[1], stoull(cmd[2]));
            }
        } else {
            // Agar data incomplete hai ya file khatam ho gayi, replay band kar do
            break;
        }
    }
    cout << "[AOF Recovery] Database successfully restored from disk!\n";
}

// ==================================================================
// SECTION 5: COMMAND EXECUTOR (Commands Ke Responses Build Karna)
// ==================================================================
void execute_command(int fd, const vector<string>& cmd) {
    if (cmd.empty()) return;

    string response;
    string command_name = cmd[0];
    
    // Convert command name to uppercase for case insensitivity
    for (char &c : command_name) {
        c = toupper(c);
    }

    if (command_name == "PING") {
        response = "+PONG\r\n"; // Simple String response
    } 
    else if (command_name == "ECHO" && cmd.size() > 1) {
        response = "$" + to_string(cmd[1].length()) + "\r\n" + cmd[1] + "\r\n";
    } 
    else if (command_name == "SET" && cmd.size() > 2) {
        db_set(g_db, cmd[1], cmd[2]);
        
        // Support optional EX argument: SET key value EX seconds
        if (cmd.size() >= 5) {
            string opt = cmd[3];
            for (char &c : opt) c = toupper(c);
            if (opt == "EX") {
                try {
                    uint64_t sec = stoull(cmd[4]);
                    db_expire(g_db, cmd[1], sec);
                } catch (...) {}
            }
        }
        response = "+OK\r\n"; // Simple String OK response
        aof_log(g_aof_fd, cmd); // Save change to disk
    } 
    else if (command_name == "GET" && cmd.size() > 1) {
        string value = db_get(g_db, cmd[1]);
        if (value.empty()) {
            response = "$-1\r\n"; // Null Bulk String
        } else {
            response = "$" + to_string(value.length()) + "\r\n" + value + "\r\n";
        }
    } 
    else if (command_name == "DEL" && cmd.size() > 1) {
        bool deleted = db_del(g_db, cmd[1]);
        if (deleted) {
            response = ":1\r\n"; // Integer 1
            aof_log(g_aof_fd, cmd); // Save change to disk
        } else {
            response = ":0\r\n"; // Integer 0
        }
    }
    else if (command_name == "EXPIRE" && cmd.size() > 2) {
        try {
            uint64_t sec = stoull(cmd[2]);
            bool ok = db_expire(g_db, cmd[1], sec);
            if (ok) {
                response = ":1\r\n";
                aof_log(g_aof_fd, cmd); // Save change to disk
            } else {
                response = ":0\r\n";
            }
        } catch (...) {
            response = "-ERR value is not an integer or out of range\r\n";
        }
    }
    else if (command_name == "TTL" && cmd.size() > 1) {
        int64_t ttl = db_ttl(g_db, cmd[1]);
        response = ":" + to_string(ttl) + "\r\n";
    }
    else {
        response = "-ERR unknown command '" + cmd[0] + "'\r\n"; // Error response
    }

    if (write(fd, response.data(), response.length()) < 0) {
        cerr << "Write failed on Client FD = " << fd << ": " << strerror(errno) << "\n";
    }
}

// ==================================================================
// SECTION 7: MAIN EVENT LOOP (Networking & Event Demultiplexing)
// ==================================================================
int main() {
    // ------------------------------------------
    // STEP 0: RECOVER DATA & OPEN AOF FILE FOR WRITING
    // ------------------------------------------
    aof_load("appendonly.aof"); // Purana data disk se restore karein

    // New writes ke liye AOF file write/append mode me open karein
    g_aof_fd = open("appendonly.aof", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_aof_fd < 0) {
        die("Opening AOF file for writing");
    }

    // ------------------------------------------
    // STEP 1: CREATE THE MAIN SERVER SOCKET
    // ------------------------------------------
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        die("Socket creation");
    }

    set_nonblocking(server_fd);

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        die("Setting socket options");
    }

    // ------------------------------------------
    // STEP 2: BIND SOCKET TO PORT 6379
    // ------------------------------------------
    struct sockaddr_in address{}; 
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        die("Binding socket");
    }

    cout << "Socket successfully bound to Port 6379!\n";

    // ------------------------------------------
    // STEP 3: START LISTENING FOR CLIENTS
    // ------------------------------------------
    if (listen(server_fd, 10) < 0) {
        die("Listen");
    }
    cout << "Server is listening on port 6379...\n";

    // ------------------------------------------
    // STEP 4: SETUP THE EVENT WATCH LIST (poll)
    // ------------------------------------------
    vector<struct pollfd> fds;
    struct pollfd server_pfd{};
    server_pfd.fd = server_fd;
    server_pfd.events = POLLIN;
    fds.push_back(server_pfd);

    unordered_map<int, Client> clients;

    // ------------------------------------------
    // STEP 5: THE EVENT LOOP
    // ------------------------------------------
    while (true) {
        int num_events = poll(fds.data(), fds.size(), 100);
        if (num_events < 0) {
            if (errno == EINTR) continue;
            die("poll");
        }

        // CASE A: NEW CLIENT IS CONNECTING
        if (fds[0].revents & POLLIN) {
            struct sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
            
            if (client_fd >= 0) {
                set_nonblocking(client_fd);

                struct pollfd client_pfd{};
                client_pfd.fd = client_fd;
                client_pfd.events = POLLIN;
                fds.push_back(client_pfd);

                clients[client_fd] = Client{client_fd, ""};

                g_pool.enqueue([client_fd]() {
                    // Background client connection logging task
                });

                cout << "Client connected! Client FD = " << client_fd << "\n";
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                cerr << "Accept failed: " << strerror(errno) << "\n";
            }
        }

        // CASE B: EXISTING CLIENT SENT DATA OR LEFT
        for (size_t i = 1; i < fds.size(); ) {
            bool socket_closed = false;
            int client_fd = fds[i].fd;

            if (fds[i].revents & POLLIN) {
                char buffer[1024];
                int bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
                
                if (bytes_read < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        cerr << "Read failed on Client FD = " << client_fd << ": " << strerror(errno) << "\n";
                        close(client_fd);
                        clients.erase(client_fd);
                        fds.erase(fds.begin() + i);
                        socket_closed = true;
                    }
                } 
                else if (bytes_read == 0) {
                    cout << "Client disconnected. Client FD = " << client_fd << "\n";
                    close(client_fd);
                    clients.erase(client_fd);
                    fds.erase(fds.begin() + i);
                    socket_closed = true;
                } 
                else {
                    clients[client_fd].read_buffer.append(buffer, bytes_read);

                    vector<string> cmd;
                    while (true) {
                        int parse_res = parse_request(clients[client_fd].read_buffer, cmd);
                        if (parse_res == 0) {
                            break;
                        }
                        if (parse_res < 0) {
                            cerr << "Protocol error on Client FD = " << client_fd << "\n";
                            close(client_fd);
                            clients.erase(client_fd);
                            fds.erase(fds.begin() + i);
                            socket_closed = true;
                            break;
                        }
                        execute_command(client_fd, cmd);
                    }
                }
            }

            if (!socket_closed) {
                i++;
            }
        }

        active_expire_step(g_db);
    }

    close(server_fd);
    return 0;
}

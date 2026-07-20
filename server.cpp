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
#include <unordered_map>
#include <chrono>

using namespace std;

// Represents a connected client and their read buffer
struct Client {
    int fd;
    string read_buffer;
};

// --- DATABASE IN-MEMORY STORAGE STRUCTURES ---

// A single key-value node in our hash table
struct Entry {
    string key;
    string value;
    uint64_t expire_at = 0; // Expiration timestamp in ms (0 means no expiration)
    Entry* next;            // Pointer to next entry if they hash to the same bucket (chaining)
};

// Returns current Unix timestamp in milliseconds
uint64_t current_time_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// A single hash table
struct HashTable {
    Entry** table = nullptr; // Array of Entry pointers
    size_t size = 0;         // Size of the array
    size_t mask = 0;         // Size - 1 (used for fast bitwise modulo: hash & mask)
    size_t num_keys = 0;     // Number of keys stored in this table
};

// The main database containing two hash tables for incremental rehashing
struct RedisDb {
    HashTable ht[2];
    int rehash_idx = -1; // If -1, we are not rehashing. If >= 0, it tracks our rehashing progress.
};

// Standard FNV-1a 32-bit Hash Function (simple and highly efficient for strings)
uint32_t hash_function(const string& key) {
    uint32_t hash = 2166136261U;
    for (char c : key) {
        hash ^= (uint8_t)c;
        hash *= 16777619U;
    }
    return hash;
}

// Moves a single bucket of keys from ht[0] to ht[1]
void db_rehash_step(RedisDb& db) {
    if (db.rehash_idx == -1) return; // If not rehashing, do nothing

    // 1. Skip empty buckets in ht[0] until we find one with keys
    while (db.ht[0].table[db.rehash_idx] == nullptr) {
        db.rehash_idx++;
        // If we reach the end, ht[0] is completely empty!
        if (db.rehash_idx >= (int)db.ht[0].size) {
            free(db.ht[0].table);
            db.ht[0] = db.ht[1];          // Make ht[1] the primary table
            db.ht[1] = HashTable{};       // Reset ht[1] to empty
            db.rehash_idx = -1;           // Mark rehashing as complete
            return;
        }
    }

    // 2. Move all keys in this specific bucket from ht[0] to ht[1]
    Entry* curr = db.ht[0].table[db.rehash_idx];
    while (curr != nullptr) {
        Entry* next = curr->next;

        // Calculate its new bucket index in ht[1]
        uint32_t h = hash_function(curr->key) & db.ht[1].mask;

        // Insert it at the start of the list in ht[1]
        curr->next = db.ht[1].table[h];
        db.ht[1].table[h] = curr;

        // Update counts
        db.ht[0].num_keys--;
        db.ht[1].num_keys++;

        curr = next;
    }

    // 3. Clear the old bucket we just migrated in ht[0]
    db.ht[0].table[db.rehash_idx] = nullptr;
    db.rehash_idx++; // Move pointer to next bucket

    // 4. Double check if ht[0] is now completely empty
    if (db.ht[0].num_keys == 0) {
        free(db.ht[0].table);
        db.ht[0] = db.ht[1];
        db.ht[1] = HashTable{};
        db.rehash_idx = -1;
    }
}

// Prepares the database to resize by allocating the secondary table ht[1]
void db_resize(RedisDb& db) {
    if (db.rehash_idx != -1) return; // Already rehashing

    // Double the size of the current table (or start at size 4 if empty)
    size_t new_size = (db.ht[0].size == 0) ? 4 : db.ht[0].size * 2;

    // Allocate memory for the new table buckets
    db.ht[1].table = (Entry**)calloc(new_size, sizeof(Entry*));
    db.ht[1].size = new_size;
    db.ht[1].mask = new_size - 1;
    db.ht[1].num_keys = 0;

    // Point the pointer to bucket 0 to start rehashing
    db.rehash_idx = 0;
}

// Helper: Searches for a key in a single HashTable
Entry* ht_find(HashTable& ht, const string& key) {
    if (ht.size == 0) return nullptr;
    uint32_t h = hash_function(key) & ht.mask;
    Entry* curr = ht.table[h];
    while (curr != nullptr) {
        if (curr->key == key) return curr;
        curr = curr->next;
    }
    return nullptr;
}

// Forward declaration of db_del for passive_expire
bool db_del(RedisDb& db, const string& key);

// Checks if a key is expired. If expired, deletes it lazily (Passive Expiration).
bool passive_expire(RedisDb& db, const string& key) {
    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }
    if (entry != nullptr && entry->expire_at > 0) {
        if (current_time_ms() >= entry->expire_at) {
            db_del(db, key); // Expired! Delete lazily on the spot
            return true;
        }
    }
    return false;
}

// Sets an expiration time in seconds for a key
bool db_expire(RedisDb& db, const string& key, uint64_t seconds) {
    if (passive_expire(db, key)) return false; // Key already expired

    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }
    if (entry == nullptr) return false; // Key does not exist

    entry->expire_at = current_time_ms() + (seconds * 1000);
    return true;
}

// Returns remaining TTL in seconds (-2 if non-existent, -1 if no expiration, >=0 if active)
int64_t db_ttl(RedisDb& db, const string& key) {
    if (passive_expire(db, key)) return -2; // Key expired/deleted

    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }
    if (entry == nullptr) return -2; // Key does not exist

    if (entry->expire_at == 0) return -1; // No expiration set

    uint64_t now = current_time_ms();
    if (now >= entry->expire_at) {
        db_del(db, key);
        return -2;
    }

    return (int64_t)((entry->expire_at - now) / 1000); // Remaining seconds
}

// Gets the value of a key from our database. Returns empty string if not found.
string db_get(RedisDb& db, const string& key) {
    // Passive Expiration check: if key is expired, delete it lazily
    if (passive_expire(db, key)) return "";

    // Perform one step of migration if active
    if (db.rehash_idx != -1) db_rehash_step(db);

    // Search in the primary table ht[0] first
    Entry* entry = ht_find(db.ht[0], key);
    if (entry != nullptr) return entry->value;

    // If not found and we are rehashing, check the secondary table ht[1]
    if (db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
        if (entry != nullptr) return entry->value;
    }
    return ""; // Not found
}

// Sets a key to a value in our database
void db_set(RedisDb& db, const string& key, const string& value) {
    // Perform one step of migration if active
    if (db.rehash_idx != -1) db_rehash_step(db);

    // If the table is empty or too full (num_keys >= size), trigger resize
    if (db.ht[0].size == 0 || db.ht[0].num_keys >= db.ht[0].size) {
        db_resize(db);
    }

    // Check if key already exists (search both tables)
    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }

    if (entry != nullptr) {
        entry->value = value; // Update existing value
        return;
    }

    // Create a new entry node
    Entry* new_entry = new Entry{key, value, nullptr};

    // If rehashing is active, insert the new key directly into the new table ht[1]
    if (db.rehash_idx != -1) {
        uint32_t h = hash_function(key) & db.ht[1].mask;
        new_entry->next = db.ht[1].table[h];
        db.ht[1].table[h] = new_entry;
        db.ht[1].num_keys++;
    } else {
        // Otherwise, insert into ht[0]
        uint32_t h = hash_function(key) & db.ht[0].mask;
        new_entry->next = db.ht[0].table[h];
        db.ht[0].table[h] = new_entry;
        db.ht[0].num_keys++;
    }
}

// Helper: Deletes a key from a single HashTable
bool ht_delete(HashTable& ht, const string& key) {
    if (ht.size == 0) return false;
    uint32_t h = hash_function(key) & ht.mask;
    Entry* curr = ht.table[h];
    Entry* prev = nullptr;
    while (curr != nullptr) {
        if (curr->key == key) {
            // Remove node from linked list
            if (prev == nullptr) {
                ht.table[h] = curr->next;
            } else {
                prev->next = curr->next;
            }
            delete curr; // Free memory
            ht.num_keys--;
            return true;
        }
        prev = curr;
        curr = curr->next;
    }
    return false;
}

// Deletes a key from our database
bool db_del(RedisDb& db, const string& key) {
    // Perform one step of migration if active
    if (db.rehash_idx != -1) db_rehash_step(db);

    // Try deleting from primary table ht[0] first
    bool deleted = ht_delete(db.ht[0], key);
    
    // If not found in ht[0] and we are rehashing, try deleting from ht[1]
    if (!deleted && db.rehash_idx != -1) {
        deleted = ht_delete(db.ht[1], key);
    }
    return deleted;
}

// Helper: reads a single line ending in \r\n from 'buf' starting at 'pos'.
// If a line is successfully read, updates 'pos' and returns the line content (without \r\n).
// If a complete line is not found, returns an empty string and leaves 'pos' unchanged.
string read_line(const string& buf, size_t& pos) {
    size_t newline = buf.find("\r\n", pos);
    if (newline == string::npos) {
        return ""; // Incomplete line
    }
    string line = buf.substr(pos, newline - pos);
    pos = newline + 2; // Advance pos past \r\n
    return line;
}

// Attempts to parse a complete RESP command array from the buffer.
// Returns 1 if a full command is parsed (and removes it from buf).
// Returns 0 if the data is incomplete (needs more bytes).
// Returns -1 if there is a protocol error.
int parse_request(string& buf, vector<string>& cmd) {
    if (buf.empty()) return 0;
    
    size_t pos = 0;
    
    // 1. Read the array header line, e.g. "*3"
    string array_line = read_line(buf, pos);
    if (array_line.empty()) return 0; // Incomplete
    
    if (array_line[0] != '*') return -1; // Protocol error: must start with '*'
    
    // Parse how many words are in this command
    int num_elements = stoi(array_line.substr(1));
    if (num_elements <= 0) {
        buf.erase(0, pos);
        return 1;
    }
    
    vector<string> parsed_cmd;
    
    // 2. Loop to read each word
    for (int i = 0; i < num_elements; ++i) {
        // Read the string length line, e.g. "$4"
        string len_line = read_line(buf, pos);
        if (len_line.empty()) return 0; // Incomplete
        
        if (len_line[0] != '$') return -1; // Protocol error: must start with '$'
        
        int str_len = stoi(len_line.substr(1));
        
        // Check if we have the entire word content + trailing "\r\n" in the buffer
        if (pos + str_len + 2 > buf.size()) {
            return 0; // Incomplete
        }
        
        // Extract the actual word content
        string content = buf.substr(pos, str_len);
        
        // Verify it ends with "\r\n"
        if (buf.substr(pos + str_len, 2) != "\r\n") {
            return -1; // Protocol error
        }
        
        parsed_cmd.push_back(content);
        pos += str_len + 2; // Advance past the word and "\r\n"
    }
    
    // Success: remove parsed data from buffer and save the command
    buf.erase(0, pos);
    cmd = parsed_cmd;
    return 1;
}

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

// Our global in-memory database instance
RedisDb g_db;

// Executes a successfully parsed command list and sends back a RESP response
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
        // Bulk String response: $<len>\r\n<msg>\r\n
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
    } 
    else if (command_name == "GET" && cmd.size() > 1) {
        string value = db_get(g_db, cmd[1]);
        if (value.empty()) {
            response = "$-1\r\n"; // Null Bulk String (key not found or expired)
        } else {
            response = "$" + to_string(value.length()) + "\r\n" + value + "\r\n";
        }
    } 
    else if (command_name == "DEL" && cmd.size() > 1) {
        bool deleted = db_del(g_db, cmd[1]);
        if (deleted) {
            response = ":1\r\n"; // Integer 1 (deleted successfully)
        } else {
            response = ":0\r\n"; // Integer 0 (key not found)
        }
    }
    else if (command_name == "EXPIRE" && cmd.size() > 2) {
        try {
            uint64_t sec = stoull(cmd[2]);
            bool ok = db_expire(g_db, cmd[1], sec);
            response = ok ? ":1\r\n" : ":0\r\n";
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

    // Map to associate each client's file descriptor with their Client state
    unordered_map<int, Client> clients;

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

                // Register client in our state map
                clients[client_fd] = Client{client_fd, ""};

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
            int client_fd = fds[i].fd;

            // Check if this client socket is active (light is on)
            if (fds[i].revents & POLLIN) {
                char buffer[1024];
                int bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
                
                if (bytes_read < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        cerr << "Read failed on Client FD = " << client_fd << ": " << strerror(errno) << "\n";
                        close(client_fd);
                        clients.erase(client_fd); // Remove client state
                        fds.erase(fds.begin() + i); // Remove from watch list
                        socket_closed = true;
                    }
                } 
                else if (bytes_read == 0) {
                    cout << "Client disconnected. Client FD = " << client_fd << "\n";
                    close(client_fd);
                    clients.erase(client_fd); // Remove client state
                    fds.erase(fds.begin() + i); // Remove from watch list
                    socket_closed = true;
                } 
                else {
                    // Append incoming bytes to this client's read buffer
                    clients[client_fd].read_buffer.append(buffer, bytes_read);

                    // Parse and execute as many complete commands as we can find
                    vector<string> cmd;
                    while (true) {
                        int parse_res = parse_request(clients[client_fd].read_buffer, cmd);
                        if (parse_res == 0) {
                            // Message is incomplete, wait for more packets
                            break;
                        }
                        if (parse_res < 0) {
                            // Protocol error (not standard RESP)
                            cerr << "Protocol error on Client FD = " << client_fd << "\n";
                            close(client_fd);
                            clients.erase(client_fd);
                            fds.erase(fds.begin() + i);
                            socket_closed = true;
                            break;
                        }
                        
                        // Successfully parsed a complete command! Execute it.
                        execute_command(client_fd, cmd);
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

// ==================================================================
// SECTION 1: HEADER FILES (Zaroori Libraries Import Kar Rahe Hain)
// ==================================================================
#include <iostream>        // Console input/output ke liye (cout, cerr)
#include <sys/socket.h>    // Networking socket operations ke liye (socket, bind, listen, accept)
#include <netinet/in.h>    // IP address structures ke liye (sockaddr_in)
#include <unistd.h>        // OS system calls ke liye (read, write, close)
#include <cstring>       // String manipulation aur strerror error messages ke liye
#include <cerrno>        // Global error variable (errno) ke liye
#include <cstdlib>       // General utilities (exit, rand, calloc) ke liye
#include <vector>        // Dynamic list array ke liye
#include <poll.h>        // Event monitoring alert panel (poll) ke liye
#include <fcntl.h>       // Non-blocking file control (fcntl) ke liye
#include <unordered_map> // Key-value lookup map ke liye
#include <chrono>        // High-precision time calculation ke liye
#include <thread>        // Multithreading worker threads ke liye
#include <mutex>         // Thread synchronization lock ke liye
#include <condition_variable> // Worker threads wake-up signals ke liye
#include <queue>         // Task queue ke liye
#include <functional>    // std::function callbacks ke liye

using namespace std;

// ==================================================================
// SECTION 2: CLIENT MAILBOX (Har Client Ka Network Buffer Track Karna)
// ==================================================================
// Jab koi client connect hota hai, uski browser/cli se aane waale raw bytes 
// hum uske `read_buffer` (mailbox) me accumulate karte hain.
struct Client {
    int fd;              // Client socket ka unique ID number
    string read_buffer;  // Client ka raw network bytes mailbox
};

// ==================================================================
// SECTION 3: IN-MEMORY DATABASE STRUCTURES (Custom Hash Table)
// ==================================================================

// Ek single key-value data box (Linked List node)
struct Entry {
    string key;             // Ex: "name"
    string value;           // Ex: "alex"
    uint64_t expire_at = 0; // Expiration timestamp ms me (0 matlab no expiry)
    Entry* next;            // Agla Entry node agar 2 keys ka hash same aa jaye (Chaining)
};

// Current exact Unix time milliseconds me calculate karne waala helper
uint64_t current_time_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// Ek akeli Hash Table structure
struct HashTable {
    Entry** table = nullptr; // Entry pointers ka array (Buckets)
    size_t size = 0;         // Table ki total storage size
    size_t mask = 0;         // size - 1 (fast index calculate karne ke liye: hash & mask)
    size_t num_keys = 0;     // Table me kitne keys abhi stored hain
};

// Main Redis Database structure (isme 2 Hash Tables hain Incremental Rehashing ke liye)
struct RedisDb {
    HashTable ht[2];     // ht[0] is primary table, ht[1] is secondary (used during resizing)
    int rehash_idx = -1; // Agar -1 hai to rehashing band hai; >= 0 hai to rehashing active hai
};

// Standard FNV-1a Hash Function (Text key ko ek numeric bucket index number me convert karta hai)
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

// ==================================================================
// SECTION 3B: EXPIRATION & TTL SYSTEM (Keys Ki Expiry Manage Karna)
// ==================================================================

// Passive Expiration: Jab koi key search hoti hai, tab check karta hai ki wo expire to nahi ho gayi
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

// Kisi key ke upar seconds ka expiration timer set karta hai
bool db_expire(RedisDb& db, const string& key, uint64_t seconds) {
    if (passive_expire(db, key)) return false; // Key agar pehle hi expire ho chuki hai

    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }
    if (entry == nullptr) return false; // Key exist hi nahi karti

    entry->expire_at = current_time_ms() + (seconds * 1000);
    return true;
}

// Remaining TTL (Time-To-Live) seconds me return karta hai (-2: Not found, -1: No expiry, >=0: Seconds left)
int64_t db_ttl(RedisDb& db, const string& key) {
    if (passive_expire(db, key)) return -2; // Key expire ho chuki hai

    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }
    if (entry == nullptr) return -2; // Key nahi mili

    if (entry->expire_at == 0) return -1; // No expiration set

    uint64_t now = current_time_ms();
    if (now >= entry->expire_at) {
        db_del(db, key);
        return -2;
    }

    return (int64_t)((entry->expire_at - now) / 1000); // Remaining seconds
}

// Active Expiration: Background me 20 random keys check karke expired keys ko delete karta hai
void active_expire_step(RedisDb& db) {
    if (db.ht[0].size == 0 || db.ht[0].num_keys == 0) return;

    int samples = 0;
    int max_samples = 20; // Sample up to 20 keys per check
    uint64_t now = current_time_ms();

    // Start at a random bucket index
    size_t start_bucket = rand() % db.ht[0].size;

    for (size_t i = 0; i < db.ht[0].size && samples < max_samples; ++i) {
        size_t idx = (start_bucket + i) % db.ht[0].size;
        Entry* curr = db.ht[0].table[idx];

        while (curr != nullptr && samples < max_samples) {
            Entry* next = curr->next;
            samples++;

            if (curr->expire_at > 0 && now >= curr->expire_at) {
                // Key is expired! Delete it from DB
                string key_to_del = curr->key;
                db_del(db, key_to_del);
            }
            curr = next;
        }
    }
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

// ==================================================================
// SECTION 4: RESP PARSER (Client Ki Redis Language Translate Karna)
// ==================================================================

// Helper: Network buffer me se \r\n tak ki 1 akeli line padhta hai
string read_line(const string& buf, size_t& pos) {
    size_t newline = buf.find("\r\n", pos);
    if (newline == string::npos) {
        return ""; // Line poori nahi aayi, wait karo
    }
    string line = buf.substr(pos, newline - pos);
    pos = newline + 2; // Next line par move karo
    return line;
}

// Client ke mailbox (read_buffer) me se RESP command array decode karne waala translator
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

// Global in-memory database instance
RedisDb g_db;

// ==================================================================
// SECTION 5: COMMAND EXECUTOR (Commands Ke Responses Build Karna)
// ==================================================================
// Translator se aaye parsed words (ex: ["SET", "name", "alex"]) ko execute 
// karke client ko RESP format me reply bhejne waala function.
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

// ==================================================================
// SECTION 6: WORKER THREAD POOL (Background Worker Threads)
// ==================================================================
// Yeh class background worker threads ko manage karti hai taaki heavy background 
// tasks parallel me execute ho sakein aur main server thread kabhi freeze na ho.
class ThreadPool {
public:
    ThreadPool(size_t num_threads) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers.emplace_back([this]() {
                while (true) {
                    function<void()> task;
                    {
                        unique_lock<mutex> lock(this->queue_mutex);
                        this->cv.wait(lock, [this]() {
                            return this->stop || !this->tasks.empty();
                        });

                        if (this->stop && this->tasks.empty()) {
                            return;
                        }

                        task = move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task(); // Execute the background task
                }
            });
        }
    }

    void enqueue(function<void()> task) {
        {
            unique_lock<mutex> lock(queue_mutex);
            tasks.push(task);
        }
        cv.notify_one(); // Wake up one sleeping worker thread
    }

    ~ThreadPool() {
        {
            unique_lock<mutex> lock(queue_mutex);
            stop = true;
        }
        cv.notify_all();
        for (thread &worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    vector<thread> workers;
    queue<function<void()>> tasks;
    mutex queue_mutex;
    condition_variable cv;
    bool stop = false;
};

// ==================================================================
// SECTION 7: MAIN EVENT LOOP (Networking & Event Demultiplexing)
// ==================================================================
// Program yahan se shuru hota hai. Yeh main thread server socket ko listen 
// karta hai aur poll() alert panel ke jariye multiple clients ko handle karta hai.
int main() {
    // ------------------------------------------
    // STEP 1: CREATE THE MAIN SERVER SOCKET
    // ------------------------------------------
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
        // poll() blocks for up to 100ms waiting for active network traffic
        int num_events = poll(fds.data(), fds.size(), 100);
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

                // Offload a background analytics task to our worker thread pool!
                g_pool.enqueue([client_fd]() {
                    // Executed asynchronously in the background by a worker thread!
                });

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

        // Active Expiration Sweep: sample random keys and delete expired ones
        active_expire_step(g_db);
    }

    close(server_fd);
    return 0;
}

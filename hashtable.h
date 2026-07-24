#ifndef HASHTABLE_H
#define HASHTABLE_H

#include "utils.h"
#include <vector>

using namespace std;

// ==================================================================
// DATA STRUCTURES (Custom Hash Table)
// ==================================================================

// Ek single key-value data box (Linked List node)
struct Entry {
    string key;             // Ex: "name"
    string value;           // Ex: "alex"
    uint64_t expire_at = 0; // Expiration timestamp ms me (0 matlab no expiry)
    uint64_t last_accessed_at = 0; // Last time jab is key ko read/write kiya gaya (ms me)
    Entry* next;            // Agla Entry node agar 2 keys ka hash same aa jaye (Chaining)
};

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

// Standard FNV-1a Hash Function
inline uint32_t hash_function(const string& key) {
    uint32_t hash = 2166136261U;
    for (char c : key) {
        hash ^= (uint8_t)c;
        hash *= 16777619U;
    }
    return hash;
}

// Forward declarations
bool db_del(RedisDb& db, const string& key);
void db_rehash_step(RedisDb& db);
void db_resize(RedisDb& db);
Entry* ht_find(HashTable& ht, const string& key);
bool passive_expire(RedisDb& db, const string& key);
void evict_lru_step(RedisDb& db);

// Moves a single bucket of keys from ht[0] to ht[1]
inline void db_rehash_step(RedisDb& db) {
    if (db.rehash_idx == -1) return;

    while (db.ht[0].table[db.rehash_idx] == nullptr) {
        db.rehash_idx++;
        if (db.rehash_idx >= (int)db.ht[0].size) {
            free(db.ht[0].table);
            db.ht[0] = db.ht[1];
            db.ht[1] = HashTable{};
            db.rehash_idx = -1;
            return;
        }
    }

    Entry* curr = db.ht[0].table[db.rehash_idx];
    while (curr != nullptr) {
        Entry* next = curr->next;
        uint32_t h = hash_function(curr->key) & db.ht[1].mask;
        curr->next = db.ht[1].table[h];
        db.ht[1].table[h] = curr;
        db.ht[0].num_keys--;
        db.ht[1].num_keys++;
        curr = next;
    }

    db.ht[0].table[db.rehash_idx] = nullptr;
    db.rehash_idx++;

    if (db.ht[0].num_keys == 0) {
        free(db.ht[0].table);
        db.ht[0] = db.ht[1];
        db.ht[1] = HashTable{};
        db.rehash_idx = -1;
    }
}

// Prepares the database to resize by allocating the secondary table ht[1]
inline void db_resize(RedisDb& db) {
    if (db.rehash_idx != -1) return;
    size_t new_size = (db.ht[0].size == 0) ? 4 : db.ht[0].size * 2;
    db.ht[1].table = (Entry**)calloc(new_size, sizeof(Entry*));
    db.ht[1].size = new_size;
    db.ht[1].mask = new_size - 1;
    db.ht[1].num_keys = 0;
    db.rehash_idx = 0;
}

// Helper: Searches for a key in a single HashTable
inline Entry* ht_find(HashTable& ht, const string& key) {
    if (ht.size == 0) return nullptr;
    uint32_t h = hash_function(key) & ht.mask;
    Entry* curr = ht.table[h];
    while (curr != nullptr) {
        if (curr->key == key) return curr;
        curr = curr->next;
    }
    return nullptr;
}

// Passive Expiration: checks if key is expired and deletes it lazily
inline bool passive_expire(RedisDb& db, const string& key) {
    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }
    if (entry != nullptr && entry->expire_at > 0) {
        if (current_time_ms() >= entry->expire_at) {
            db_del(db, key); // Expired! Delete lazily
            return true;
        }
    }
    return false;
}

// Sets an expiration time in seconds for a key
inline bool db_expire(RedisDb& db, const string& key, uint64_t seconds) {
    if (passive_expire(db, key)) return false;

    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }
    if (entry == nullptr) return false;

    entry->expire_at = current_time_ms() + (seconds * 1000);
    return true;
}

// Returns remaining TTL in seconds (-2: Not found, -1: No expiry, >=0: active)
inline int64_t db_ttl(RedisDb& db, const string& key) {
    if (passive_expire(db, key)) return -2;

    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }
    if (entry == nullptr) return -2;

    if (entry->expire_at == 0) return -1;

    uint64_t now = current_time_ms();
    if (now >= entry->expire_at) {
        db_del(db, key);
        return -2;
    }

    return (int64_t)((entry->expire_at - now) / 1000);
}

// Active Expiration background sweep
inline void active_expire_step(RedisDb& db) {
    if (db.ht[0].size == 0 || db.ht[0].num_keys == 0) return;

    int samples = 0;
    int max_samples = 20;
    uint64_t now = current_time_ms();

    size_t start_bucket = rand() % db.ht[0].size;

    for (size_t i = 0; i < db.ht[0].size && samples < max_samples; ++i) {
        size_t idx = (start_bucket + i) % db.ht[0].size;
        Entry* curr = db.ht[0].table[idx];

        while (curr != nullptr && samples < max_samples) {
            Entry* next = curr->next;
            samples++;

            if (curr->expire_at > 0 && now >= curr->expire_at) {
                string key_to_del = curr->key;
                db_del(db, key_to_del);
            }
            curr = next;
        }
    }
}

// LRU Eviction: 5 random keys me se sabse purani unused key ko select karke delete karta hai
inline void evict_lru_step(RedisDb& db) {
    if (db.ht[0].size == 0 || db.ht[0].num_keys == 0) return;

    int samples = 0;
    int max_samples = 5;
    
    Entry* oldest_entry = nullptr;
    string oldest_key = "";
    uint64_t oldest_time = -1;

    size_t start_bucket = rand() % db.ht[0].size;

    for (size_t i = 0; i < db.ht[0].size && samples < max_samples; ++i) {
        size_t idx = (start_bucket + i) % db.ht[0].size;
        Entry* curr = db.ht[0].table[idx];

        while (curr != nullptr && samples < max_samples) {
            samples++;

            if (oldest_entry == nullptr || curr->last_accessed_at < oldest_time) {
                oldest_entry = curr;
                oldest_key = curr->key;
                oldest_time = curr->last_accessed_at;
            }
            curr = curr->next;
        }
    }

    if (oldest_entry != nullptr) {
        cout << "[LRU Eviction] Evicting key: '" << oldest_key << "' (Sabse purani key delete kar di)\n";
        db_del(db, oldest_key);
    }
}

// Gets the value of a key from our database. Returns empty string if not found.
inline string db_get(RedisDb& db, const string& key) {
    if (passive_expire(db, key)) return "";
    if (db.rehash_idx != -1) db_rehash_step(db);

    Entry* entry = ht_find(db.ht[0], key);
    if (entry != nullptr) {
        entry->last_accessed_at = current_time_ms();
        return entry->value;
    }

    if (db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
        if (entry != nullptr) {
            entry->last_accessed_at = current_time_ms();
            return entry->value;
        }
    }
    return "";
}

// Database ki maximum storage limit
const size_t MAX_KEYS = 3;

// Sets a key to a value in our database
inline void db_set(RedisDb& db, const string& key, const string& value) {
    if (db.rehash_idx != -1) db_rehash_step(db);

    if (db.ht[0].size == 0 || db.ht[0].num_keys >= db.ht[0].size) {
        db_resize(db);
    }

    Entry* entry = ht_find(db.ht[0], key);
    if (entry == nullptr && db.rehash_idx != -1) {
        entry = ht_find(db.ht[1], key);
    }

    if (entry != nullptr) {
        entry->value = value;
        entry->last_accessed_at = current_time_ms();
        return;
    }

    if (db.ht[0].num_keys + db.ht[1].num_keys >= MAX_KEYS) {
        evict_lru_step(db);
    }

    Entry* new_entry = new Entry{key, value, 0, current_time_ms(), nullptr};

    if (db.rehash_idx != -1) {
        uint32_t h = hash_function(key) & db.ht[1].mask;
        new_entry->next = db.ht[1].table[h];
        db.ht[1].table[h] = new_entry;
        db.ht[1].num_keys++;
    } else {
        uint32_t h = hash_function(key) & db.ht[0].mask;
        new_entry->next = db.ht[0].table[h];
        db.ht[0].table[h] = new_entry;
        db.ht[0].num_keys++;
    }
}

// Helper: Deletes a key from a single HashTable
inline bool ht_delete(HashTable& ht, const string& key) {
    if (ht.size == 0) return false;
    uint32_t h = hash_function(key) & ht.mask;
    Entry* curr = ht.table[h];
    Entry* prev = nullptr;
    while (curr != nullptr) {
        if (curr->key == key) {
            if (prev == nullptr) {
                ht.table[h] = curr->next;
            } else {
                prev->next = curr->next;
            }
            delete curr;
            ht.num_keys--;
            return true;
        }
        prev = curr;
        curr = curr->next;
    }
    return false;
}

// Deletes a key from our database
inline bool db_del(RedisDb& db, const string& key) {
    if (db.rehash_idx != -1) db_rehash_step(db);

    bool deleted = ht_delete(db.ht[0], key);
    if (!deleted && db.rehash_idx != -1) {
        deleted = ht_delete(db.ht[1], key);
    }
    return deleted;
}

#endif // HASHTABLE_H

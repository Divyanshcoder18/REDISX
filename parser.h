#ifndef PARSER_H
#define PARSER_H

#include "utils.h"
#include <vector>

using namespace std;

// ==================================================================
// CLIENT MAILBOX (Har Client Ka Network Buffer Track Karna)
// ==================================================================
struct Client {
    int fd;              // Client socket ka unique ID number
    string read_buffer;  // Client ka raw network bytes mailbox
};

// ==================================================================
// RESP PARSER (Client Ki Redis Language Translate Karna)
// ==================================================================

// Helper: Network buffer me se \r\n tak ki 1 akeli line padhta hai
inline string read_line(const string& buf, size_t& pos) {
    size_t newline = buf.find("\r\n", pos);
    if (newline == string::npos) {
        return ""; // Line poori nahi aayi, wait karo
    }
    string line = buf.substr(pos, newline - pos);
    pos = newline + 2; // Next line par move karo
    return line;
}

// Client ke mailbox (read_buffer) me se RESP command array decode karne waala translator
inline int parse_request(string& buf, vector<string>& cmd) {
    if (buf.empty()) return 0;
    
    size_t pos = 0;
    
    // 1. Read the array header line, e.g. "*3"
    string array_line = read_line(buf, pos);
    if (array_line.empty()) return 0; // Incomplete
    
    if (array_line[0] != '*') return -1; // Protocol error
    
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
        
        if (len_line[0] != '$') return -1; // Protocol error
        
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

#endif // PARSER_H

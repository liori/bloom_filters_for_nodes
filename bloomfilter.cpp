#include <cstdio>
#include <string>
#include <vector>
#include <cmath>
#include <cassert>
#include <cstring>
#include <iostream>
#include <mutex>
#include <thread>

std::string prefix_file_name(std::string prefix) {
    char characters[65] = { 0 };
    for(auto idx = 0; idx < prefix.size(); ++idx) {
        std::sprintf(characters + 2 * idx, "%02hhx", uint8_t(prefix[idx]));
    }
    return std::string{characters} + ".dat";
}

struct id {
    char data[32];
};

struct entry_t {
    struct id node_id;
    struct id piece_id;
};

const uint8_t filter_offset = 0;
const uint8_t filter_range = 9;

/**
 * Reads a file named with the given prefix. Expects that this file will contain 64-byte entries, where the first 32
 * bytes describe node ID, last 32 bytes describe piece ID. Assumes that all entries have the same piece ID. Estimates
 * from the file size just how big an optimal bloom filter should be, computes it, and writes it to a file. Removes the
 * original file.
 */

void bloom(std::string prefix) {
    auto file_name = prefix_file_name(prefix);
    auto input = fopen(file_name.c_str(), "rb");
    fseek(input, 0, SEEK_END);
    auto file_size = ftell(input);
    fseek(input, 0, SEEK_SET);
    auto entries = file_size / sizeof(entry_t);
    std::cout << "Blooming " << prefix_file_name(prefix) << std::endl;

    // based on go/pkg/mod/storj.io/common@v0.0.0-20240228143024-18bcefd218de/bloomfilter/filter.go:196
    auto bits_per_element = -1.44 * std::log2(0.1);
    auto hash_count = int(std::ceil(bits_per_element * std::log(2)));
    if (hash_count > 32) {
        hash_count = 32;
    }
    auto size = int(std::ceil(entries * bits_per_element / 8));
    assert(size > 0);

    std::vector<uint8_t> bitmap(size);

    entry_t entry;
    while(true) {
        if(fread(&entry, sizeof(entry), 1, input) == 0) {
            break;
        }

        // based on go/pkg/mod/storj.io/common@v0.0.0-20240228143024-18bcefd218de/bloomfilter/filter.go:86
        char id[64];
        std::memcpy(id, entry.piece_id.data, 32);
        std::memcpy(id + 32, entry.piece_id.data, 32);
        auto offset = filter_offset;
        for (auto h = hash_count; h > 0; --h) {
            auto hash = (uint64_t*)(id + offset);
            auto bit = id[offset + 8];
            auto bucket = *hash % size;
            bitmap[bucket] |= 1 << (bit % 8);
            offset = (offset + filter_range) % 32;
        }
    }

    {
        fclose(input);
        remove(file_name.c_str());
    }

    auto node_file_name = prefix_file_name(std::string(entry.node_id.data, 32));
    auto output = fopen(node_file_name.c_str(), "wb");
    fwrite(bitmap.data(), bitmap.size(), 1, output);
    fclose(output);
}

/**
 * If prefix is empty, reads stdin. Otherwise reads a file with the given hex prefix. Expects 64-byte entries, where the
 * first 32 bytes describe node ID, last 32 bytes describe piece ID. Assumes that each node ID starts with the prefix.
 * Creates 256 files also consisting of these 64-byte entries, each file containing entries whose node ID starts with
 * the original prefix, then a new unique byte. Adds recurrent invocations to the compute queue.
 */
void split(std::string prefix) {
    auto file_name = prefix_file_name(prefix);
    std::cout << "Working on " << file_name << std::endl;

    auto input = fopen(file_name.c_str(), "rb");
    fseek(input, 0, SEEK_END);
    auto file_size = ftell(input);
    fclose(input);
    auto entries = file_size / sizeof(entry_t);

    struct output_file {
        std::mutex mu;
        FILE* file;
        bool many_nodes;
        id first_node_id;
    };

    std::vector<output_file> output_files(256);

    std::vector<std::thread> threads;
    const int thread_count = std::thread::hardware_concurrency();
    for(int thread_id = 0; thread_id < thread_count; ++thread_id) {
        threads.emplace_back([&](int thread_id){
            entry_t entry;

            auto start_entry_id = entries * thread_id / thread_count;
            auto end_entry_id = entries * (thread_id + 1) / thread_count;
            auto input = fopen(file_name.c_str(), "rb");
            fseek(input, start_entry_id * sizeof(entry), SEEK_SET);

            for(uint64_t entry_id = start_entry_id; entry_id < end_entry_id; ++entry_id) {
                fread(&entry, 1, sizeof(entry), input);

                uint8_t value = entry.node_id.data[prefix.size()];
                std::lock_guard guard(output_files[value].mu);

                if(!output_files[value].file) {
                    auto file_name = prefix_file_name(std::string(entry.node_id.data, prefix.size() + 1));
                    output_files[value].file = fopen(file_name.c_str(), "wb");
                    output_files[value].first_node_id = entry.node_id;
                }
                fwrite(&entry, sizeof(entry), 1, output_files[value].file);
                if(memcmp(entry.node_id.data, output_files[value].first_node_id.data, 32) != 0) {
                    output_files[value].many_nodes = true;
                }
            }
            fclose(input);
        }, thread_id);
    }

    for(int thread_id = 0; thread_id < thread_count; ++thread_id) {
        threads[thread_id].join();
    }

    if (!prefix.empty()) {
        auto file_name = prefix_file_name(prefix);
        remove(file_name.c_str());
    }

    for(auto idx = 0; idx < 256; ++idx) {
        if (output_files[idx].file) {
            fclose(output_files[idx].file);
        }
    }

    for(auto idx = 0; idx < 256; ++idx) {
        if (output_files[idx].file) {
            if (output_files[idx].many_nodes) {
                split(prefix + char(idx));
            } else {
                bloom(prefix + char(idx));
            }
        }
    }
}


int main() {
    split("");
    return 0;
}
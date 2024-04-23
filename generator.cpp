#include <random>
#include <algorithm>
#include <unistd.h>
#include <thread>

struct id {
    uint64_t data[4];
};

int main(int argc, char** argv) {
    int nodes = atoi(argv[1]);
    long entries = atol(argv[2]);

    std::mt19937_64 gen;
    std::vector<id> node_ids(nodes);
    std::generate(&node_ids[0].data[0], &node_ids[nodes].data[0], std::ref(gen));

    std::vector<std::thread> threads;
    const int thread_count = std::thread::hardware_concurrency();
    for(int thread_id = 0; thread_id < thread_count; ++thread_id) {
        threads.emplace_back([&](int thread_id){
            std::seed_seq seed{thread_id};
            std::mt19937_64 gen(seed);
            std::uniform_int_distribution<> node_generator(0, nodes - 1);

            auto output = fopen(".dat", "ab");
            auto start_entry_id = entries * thread_id / thread_count;
            auto end_entry_id = entries * (thread_id + 1) / thread_count;
            fseek(output, start_entry_id, SEEK_SET);

            id piece_id;
            for (uint64_t i = start_entry_id; i < end_entry_id; ++i) {
                piece_id.data[0] = gen();
                piece_id.data[1] = gen();
                piece_id.data[2] = gen();
                piece_id.data[3] = gen();
                fwrite(node_ids[node_generator(gen)].data, 32, 1, output);
                fwrite(piece_id.data, 32, 1, output);
            }
            fclose(output);
        }, thread_id);
    }

    for(int thread_id = 0; thread_id < thread_count; ++thread_id) {
        threads[thread_id].join();
    }
}
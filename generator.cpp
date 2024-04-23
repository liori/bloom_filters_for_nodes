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
            fseek(output, 2 * sizeof(id) * start_entry_id, SEEK_SET);

            id to_write[20480];

            for (uint64_t i = 0; i < end_entry_id - start_entry_id;) {
                auto step = std::min(size_t(10240), end_entry_id - start_entry_id - i);
                i += step;
                for (auto subint = 0; subint < step; ++subint) {
                    to_write[2 * subint] = node_ids[node_generator(gen)];
                    to_write[2 * subint + 1].data[0] = gen();
                    to_write[2 * subint + 1].data[1] = gen();
                    to_write[2 * subint + 1].data[2] = gen();
                    to_write[2 * subint + 1].data[3] = gen();
                }
                fwrite(to_write, sizeof(id), 2 * step, output);
            }
            fclose(output);
        }, thread_id);
    }

    for(int thread_id = 0; thread_id < thread_count; ++thread_id) {
        threads[thread_id].join();
    }
}
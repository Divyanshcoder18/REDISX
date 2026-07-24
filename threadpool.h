#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

using namespace std;

// ==================================================================
// WORKER THREAD POOL (Background Worker Threads)
// ==================================================================
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

#endif // THREADPOOL_H

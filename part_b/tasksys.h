#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        std::vector<std::thread> threads;
        struct Tuple {
            IRunnable* runnable;
            int id;
            int num_total_tasks;
            std::atomic<int>* counter;
            std::condition_variable* counterCond;
            Tuple() {}
            Tuple(IRunnable* ir, int i, int n, 
                std::atomic<int>* c, std::condition_variable* cCnd) {
                runnable = ir;
                id = i;
                num_total_tasks = n;
                counter = c;
                counterCond = cCnd;
            }
        };
        struct WorkQueue {
            std::mutex WQmutex;
            std::condition_variable WQcond;
            std::queue<Tuple> workQueue;
            void push(const Tuple& t) {
                const std::lock_guard<std::mutex> lock(WQmutex);
                workQueue.push(Tuple(t.runnable, t.id, 
                    t.num_total_tasks, t.counter, t.counterCond));
                WQcond.notify_one();
            }
            Tuple pop() {
                std::unique_lock<std::mutex> lock(WQmutex);
                while (workQueue.empty()) {
                    WQcond.wait(lock);
                }
                Tuple t = workQueue.front();
                workQueue.pop();
                return t;
            }
        };
        WorkQueue workQueue;
        void func();
};

#endif

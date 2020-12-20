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
            TaskID taskID;
            Tuple() {}
            Tuple(TaskID tid, IRunnable* ir, int i, int n, 
                std::atomic<int>* c, std::condition_variable* cCnd) {
                taskID = tid;
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
                workQueue.push(Tuple(t.taskID, t.runnable, t.id, 
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
        ///async dependency sched
        // std::vector<std::vector<TaskID> > taskQueue;//task's succeed tasks 
        // std::vector<std::vector<TaskID> > taskDeps; //task's dependent tasks
        // std::vector<std::pair<IRunnable*, int> > taskHandl;
        // std::vector<std::atomic<int>* > taskWorks;//task's works-counter
        ///thread-safe vector
        template <class T>
        struct Vector {
            std::mutex mut;
            std::condition_variable cond;
            std::vector<T> storage;
            // Vector() {}
            // Vector(const std::vector<T>& vec) {
            //     storage = std::vector<T>(vec);
            // }
            void push_back(const T& val) {
                const std::lock_guard<std::mutex> lck(mut);
                storage.push_back(val);
                cond.notify_one();
            }
            unsigned int size() {
                const std::lock_guard<std::mutex> lck(mut);
                unsigned int res = storage.size();
                cond.notify_one();
                return res;
            }
            bool empty() {
                const std::lock_guard<std::mutex> lck(mut);
                bool res = storage.empty();
                cond.notify_one();
                return res;
            }
            T& at(int index) {
                const std::lock_guard<std::mutex> lck(mut);
                T& res = storage.at(index);
                cond.notify_one();
                return res;
            }
            T& operator[](int index) {
                return Vector::at(index);
            }
            void clear() {
                const std::lock_guard<std::mutex> lck(mut);
                storage.clear();
                cond.notify_one();
            }
        };
        Vector<std::vector<TaskID> > taskQueue;
        Vector<std::vector<TaskID> > taskDeps;
        Vector<std::pair<IRunnable*, int> > taskHandl;
        Vector<std::atomic<int>* > taskWorks;
        std::mutex counterMutex;
        std::condition_variable counterCond;
};

#endif

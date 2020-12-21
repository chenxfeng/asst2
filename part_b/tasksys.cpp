#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}
#include <exception>
#include <cassert>
void TaskSystemParallelThreadPoolSleeping::func() {
    Tuple aJob;
    while (true) {
        aJob = workQueue.pop();
        if (aJob.id == -1) return ;
        aJob.runnable->runTask(aJob.id, aJob.num_total_tasks);

        *(aJob.counter) -= 1;//-- operator isn't OK
        if (aJob.counter->load() == 0) {
            ///notice sync if waiting
            aJob.counterCond->notify_one();
            // printf("job %d in %d has jobs: %d\n", aJob.taskID, taskQueue.size(), taskQueue[aJob.taskID].size());
        }
        ///thread get into an special area
        threadCounter -= 1;
        ///if run async With dependency
        int zero = 0, nega = -1;
        ///zero != counter   ==>  zero is modified to counter and ret false
        ///zero == counter   ==>  counter is modified to nega and ret true
        if (aJob.counter->compare_exchange_strong(zero, nega)) {
            const std::lock_guard<std::mutex> lck(*(taskDone[aJob.taskID]));
            // assert(aJob.taskID < taskQueue.size());
            if (!(taskQueue.empty() || taskQueue[aJob.taskID].empty())) {
                // printf("job %d in %d before jobs: %d\n", aJob.taskID, taskQueue.size(), taskQueue[aJob.taskID].size());
                ///start the succeed task
                for (int i = 0; i < taskQueue[aJob.taskID].size(); ++i) {
                    TaskID tid = taskQueue[aJob.taskID][i];
                    ///if all dependent task has finished
                    bool isReady = true;
                    for (int j = 0; j < taskDeps.at(tid).size(); ++j) {
                        if (taskDeps.at(tid).at(j) == aJob.taskID) continue;
                        if (taskWorks.at(taskDeps.at(tid).at(j))->load() != -1) {
                            isReady = false;
                            break;
                        }
                    }
                    // printf("job %d ready: %d with %d jobs\n", tid, isReady, taskWorks.at(tid)->load());
                    if (isReady/* && counter->load() == handle.second*/) {
                        std::pair<IRunnable*, int> handle = taskHandl.at(tid);
                        std::atomic<int>* counter = taskWorks.at(tid);
                        for (int j = 0; j < handle.second; j++) {
                            workQueue.push(Tuple(tid, handle.first, j, handle.second, 
                                counter, &counterCond));
                        }
                        // printf("job %d ready: %d vs %d\n", tid, handle.second, counter->load());
                    }
                }
            }
        }
        ///thread get out this special area
        threadCounter += 1;
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int i = 0; i < num_threads; ++i) {
        threads.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::func, this));
    }
    threadCounter.store(0);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int i = 0; i < threads.size(); i++) {
        workQueue.push(Tuple(0, NULL, -1, 0, NULL, NULL));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::atomic<int> counter(num_total_tasks);
    std::mutex counterLock;
    std::condition_variable counterCond;
    for (int i = 0; i < num_total_tasks; i++) {
        workQueue.push(Tuple(0, runnable, i, num_total_tasks, 
            &counter, &counterCond));
    }
    while (true) {//busy wait
        // printf("test counter %d \n", counter);
        std::unique_lock<std::mutex> lock(counterLock);
        if (counter.load() != 0) 
            counterCond.wait(lock);
        if (counter.load() == 0) break;
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //
    // printf("begin %d %d %d %d\n", taskQueue.size(), taskDeps.size(), taskHandl.size(), taskWorks.size());
    TaskID taskId = taskQueue.size();
    taskQueue.push_back(Vector<TaskID>());
    taskDone.push_back(new std::mutex());
    taskDeps.push_back(std::vector<TaskID>(deps));
    taskHandl.push_back(std::pair<IRunnable*, int>(runnable, num_total_tasks));
    taskWorks.push_back(new std::atomic<int>(num_total_tasks));
    if (taskDeps[taskId].empty()) {
        ///launch task without dependency
        for (int i = 0; i < num_total_tasks; i++) {
            workQueue.push(Tuple(taskId, taskHandl[taskId].first, i, 
                            taskHandl[taskId].second, taskWorks[taskId], 
                            &counterCond));
        }
    } else {
        bool isReady = true;
        for (int i = 0; i < taskDeps[taskId].size(); ++i) {
            ///lock before detecting work-state to guarantee thread-safety
            const std::lock_guard<std::mutex> lck(*(taskDone[taskDeps[taskId].at(i)]));
            if (taskWorks[taskDeps[taskId].at(i)]->load() > -1) {
                isReady = false;
                taskQueue[taskDeps[taskId].at(i)].push_back(taskId);
            }
        }
        ///if all deps has done(-1), then we can add this task to executing queue
        if (isReady) {
            for (int i = 0; i < num_total_tasks; i++) {
                workQueue.push(Tuple(taskId, taskHandl[taskId].first, i, 
                                taskHandl[taskId].second, taskWorks[taskId], 
                                &counterCond));
            }
        }
    }
    // printf("doing %d %d %d %d\n", taskQueue.size(), taskDeps.size(), taskHandl.size(), taskWorks.size());
    return taskId;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // printf("waiting %d %d %d %d\n", taskQueue.size(), taskDeps.size(), taskHandl.size(), taskWorks.size());
    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    for (int i = 0; i < taskWorks.size(); ++i) {
        while (true) {
            ///must busy-waiting; or we can lock this loop code-block
            if (taskWorks.at(i)->load() == -1)//0) 
                break;
        }
        // printf("task %d of %d tasks finish\n", i, taskWorks.size());
    }
    // printf("finish %d %d %d %d\n", taskQueue.size(), taskDeps.size(), taskHandl.size(), taskWorks.size());
    ///barrier for waiting last job to finish
    while (true) {
        if (threadCounter.load() == 0) break;
    }
    for (int i = 0; i < taskWorks.size(); ++i) {
        delete taskDone[i];
        delete taskWorks[i];
    }
    taskQueue.clear();
    taskDeps.clear();
    taskHandl.clear();
    taskWorks.clear();
    return;
}

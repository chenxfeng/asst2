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
            ///if run async With dependency
            if (taskQueue.size()) {
                printf("job %d in %d before jobs: %d\n", aJob.taskID, taskQueue.size(), taskQueue[aJob.taskID].size());
                assert(aJob.taskID < taskQueue.size());
                ///start the succeed task
                for (int i = 0; i < taskQueue[aJob.taskID].size(); ++i) {
                    TaskID tid = taskQueue[aJob.taskID][i];
                    ///if all dependent task has finished
                    bool isReady = true;
                // try {
                    for (int j = 0; j < taskDeps.at(tid).size(); ++j) {
                        // assert(j < taskDeps[taskQueue[aJob.taskID].at(i)].size());
                        // assert(taskDeps[taskQueue[aJob.taskID].at(i)].at(j) < taskWorks.size());
                        if (taskWorks.at(taskDeps.at(tid).at(j))->load() != 0) {
                            isReady = false;
                            break;
                        }
                    }
                // } catch(std::exception& e) {
                //     printf("1 exception catched: %s\n", e.what());
                // } catch (...) {
                //     printf("1 ... exception\n");
                // }
                // try {
                    std::pair<IRunnable*, int> handle = taskHandl.at(tid);
                    if (isReady && taskWorks.at(tid)->load() != 0) {
                        for (int j = 0; j < handle.second; j++) {
                            workQueue.push(Tuple(tid, handle.first, j, handle.second, 
                                taskWorks.at(tid), &counterCond));
                        }
                        printf("job %d ready: %d\n", tid, taskWorks.at(tid)->load());
                    }
                // } catch(std::exception& e) {
                //     printf("2 exception catched: %s\n", e.what());
                // } catch (...) {
                //     printf("2 ... exception\n");
                // }
                }
            }
        }
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
    // printf("begin %d %d %d %d\n", taskQueue.size(), taskDeps.size(), taskHandl.size(), taskWorks.size());
    //
    // TODO: CS149 students will implement this method in Part B.
    //
    TaskID taskId = taskQueue.size();
    taskQueue.push_back(std::vector<TaskID>());
    taskDeps.push_back(std::vector<TaskID>(deps));
    taskHandl.push_back(std::pair<IRunnable*, int>(runnable, num_total_tasks));
    taskWorks.push_back(new std::atomic<int>(num_total_tasks));
    ///task launch without dependency
    if (taskDeps[taskId].empty()) {
        for (int i = 0; i < num_total_tasks; i++) {
            workQueue.push(Tuple(taskId, taskHandl[taskId].first, i, 
                            taskHandl[taskId].second, taskWorks[taskId], 
                            &counterCond));
        }
    } else {
        for (int i = 0; i < taskDeps[taskId].size(); ++i) {
            taskQueue[taskDeps[taskId].at(i)].push_back(taskId);
        }
    }
    // printf("doing %d %d %d %d\n", taskQueue.size(), taskDeps.size(), taskHandl.size(), taskWorks.size());
    return taskId;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    printf("waiting %d %d %d %d\n", taskQueue.size(), taskDeps.size(), taskHandl.size(), taskWorks.size());
    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    // std::mutex counterLock;
    for (int i = 0; i < taskWorks.size(); ++i) {
        while (true) {
            // std::unique_lock<std::mutex> lck(counterLock);
            // if (taskWorks.at(i)->load() != 0) 
            //     counterCond.wait(lck);
            ///must busy-waiting; or lock this loop code-block
            if (taskWorks.at(i)->load() == 0) 
                break;
        }
        // printf("task %d of %d tasks finish\n", i, taskWorks.size());
    }
    printf("finish %d %d %d %d\n", taskQueue.size(), taskDeps.size(), taskHandl.size(), taskWorks.size());
    for (int i = 0; i < taskWorks.size(); ++i) delete taskWorks[i];
    taskQueue.clear();
    taskDeps.clear();
    taskHandl.clear();
    taskWorks.clear();

    return;
}

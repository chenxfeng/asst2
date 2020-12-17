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
            ///start the succeed task
            for (int i = 0; i < taskQueue.size(); ++i) {
                ///if all dependent task has finished
                bool isReady = true;
                for (int j = 0; j < taskDeps[taskQueue.at(i)].size(); ++j) {
                    if (taskWorks[taskDeps[taskQueue.at(i)].at(j)].load() != 0) {
                        isReady = false;
                        break;
                    }
                }
                if (isReady) {
                    for (int j = 0; j < taskHandl[taskQueue.at(i)].second; j++) {
                        workQueue.push(Tuple(taskHandl[taskQueue.at(i)].first, 
                            j, taskHandl[taskQueue.at(i)].second, 
                            &(taskWorks[taskQueue.at(i)]), &counterCond));
                    }
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
        workQueue.push(Tuple(NULL, -1, 0, NULL, NULL));
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
        workQueue.push(Tuple(runnable, i, num_total_tasks, 
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
    taskQueue.push_back(std::vector<TaskID>());
    taskDeps.push_back(deps);
    taskHandl.push_back(std::pair<IRunnable*, int>(runnable, num_total_tasks));
    taskWorks.push_back(std::atomic<int>(num_total_tasks));
    ///task launch with no deps
    if (deps.empty()) {
        for (int i = 0; i < num_total_tasks; i++) {
            workQueue.push(Tuple(runnable, i, num_total_tasks, 
                &(taskWorks.back()), &counterCond));
        }
    } else {
        for (int i = 0; i < deps.size(); ++i) {
            taskQueue[deps.at(i)].push_back(taskQueue.size()-1);
        }
    }
    return taskQueue.size()-1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    std::mutex counterLock;
    for (int i = 0; i < taskWorks.size(); ++i) {
        while (true) {
            std::unique_lock<std::mutex> lck(counterLock);
            if (taskWorks.at(i).load() != 0) 
                counterCond.wait(lck);
            if (taskWorks.at(i).load() == 0) 
                break;
        }
    }
    taskQueue.clear();
    taskDeps.clear();
    taskHandl.clear();
    taskWorks.clear();
    return;
}

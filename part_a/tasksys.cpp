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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->numOfThread = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::func(IRunnable* runnable, 
    int num_total_tasks, std::atomic<int>* taskId) {
    int id;
    while (true) {
        id = (*taskId) ++;
        if (id >= num_total_tasks) return ;
        runnable->runTask(id, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::atomic<int> taskId(0);
    for (int i = 0; i < this->numOfThread; ++i) {
        threads.push_back(std::thread(&TaskSystemParallelSpawn::func, this, runnable, 
            num_total_tasks, &taskId));
    }
    for (int i = 0; i < this->numOfThread; ++i) {
        threads[i].join();
    }
    threads.clear();
    // for (int i = 0; i < num_total_tasks; i++) {
    //     threads.push_back(std::thread(&TaskSystemParallelSpawn::func, this, runnable, i, num_total_tasks));
    // }
    // for (int i = 0; i < threads.size(); ++i) {
    //     threads[i].join();
    // }
    // threads.clear();
    // int taskId = 0;
    // while (true && taskId < num_total_tasks) {
    //     if (taskNum >= numOfThread) continue;///guarantee max numOfThread
    //     taskNum ++;
    //     threads.push_back(std::thread(&TaskSystemParallelSpawn::func, this, runnable, taskId++, num_total_tasks));
    // }
    // for (int i = 0; i < threads.size(); ++i) {
    //     threads[i].join();
    // }
    // threads.clear();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
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

void TaskSystemParallelThreadPoolSpinning::func() {
    Tuple aJob;
    while (true) {
        if (exitFlag) return ;
        // printf("workquque: %d\n", workQueue.size());

        mutex.lock();
        if (workQueue.size()) { 
            aJob = workQueue.front();
            workQueue.pop();
        } else {
            aJob.id = -1;
        }
        mutex.unlock();

        if (aJob.id == -1) continue;
        aJob.runnable->runTask(aJob.id, aJob.num_total_tasks);
        *(aJob.counter) --;
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    exitFlag = false;
    for (int i = 0; i < num_threads; ++i) {
        threads.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::func, this));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    exitFlag.store(true);
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::atomic<int> counter(num_total_tasks);
    for (int i = 0; i < num_total_tasks; i++) {
        mutex.lock();
        workQueue.push(Tuple(runnable, i, num_total_tasks, &counter));
        mutex.unlock();
    }
    while (counter.load() != 0) {
        continue;//busy wait
    }
    // printf("exit TaskSystemParallelThreadPoolSpinning::run\n");
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
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

        aJob.counterLock->lock();
        *(aJob.counter) -= 1;//-- operator isn't OK
        if (*(aJob.counter) == 0) aJob.counterCond->notify_one();
        aJob.counterLock->unlock();
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
        workQueue.push(Tuple(NULL, -1, 0, NULL, NULL, NULL));
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
    int counter = num_total_tasks;
    std::mutex counterLock;
    std::condition_variable counterCond;
    for (int i = 0; i < num_total_tasks; i++) {
        workQueue.push(Tuple(runnable, i, num_total_tasks, &counter, 
            &counterLock, &counterCond));
    }
    while (true) {//busy wait
        // printf("test counter %d \n", counter);
        std::unique_lock<std::mutex> lock(counterLock);
        if (counter != 0) 
            counterCond.wait(lock);
        if (counter == 0) break;
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

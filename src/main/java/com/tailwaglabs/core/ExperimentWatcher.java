package com.tailwaglabs.core;

import java.util.Timer;
import java.util.TimerTask;

public class ExperimentWatcher extends Thread {

    Timer timer = new Timer();

    TimerTask task = new TimerTask() {
        @Override
        public void run() {
            myMethod();
        }
    };

    private int experimentDuration = 10 * 60 * 1000;

    /**
     * This thread is responsible for detecting when an experiment starts.
     * It's also responsible to check if an experiment exceeded 10 mins
     * 
     * The TempsMNigrator class runs this thread when it starts.
     */
    @Override
    public void run() {
        Thread.currentThread().setName("Thread_Experiment_Watcher");
        
    }


    // Call this when an experiment starts
    private void startTimer() {
        timer.schedule(task, experimentDuration);
    }

    // Call this when an experimnent ends
    private void stopTimer() {
        task.cancel();
        timer.cancel();
        timer.purge();
    }

    public void myMethod() {
        // TODO: Tis method runs after 10 minutes
        // ALERT Experiment duration
    }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author chetan
 */
public class Locker {

    private Integer waiting_readers = new Integer(0);
    private Integer waiting_writers = new Integer(0);
    private int readers = 0;
    private int writers = 0;

    public synchronized void acquireReadLock() {
        while (writers > 0) {
            try {
                waiting_readers++;
                waiting_readers.wait();
            } catch (InterruptedException ex) {
                Logger.getLogger(Locker.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        waiting_readers--;
        readers++;
    }

    public synchronized void releaseReadLock() {
        if (--readers == 0) {
            if (waiting_writers > 0) {
                waiting_writers.notify();
            }
        }
    }

    public synchronized void acquireWriteLock() {
        while (readers > 0 || writers > 0) {
            try {
                waiting_writers++;
                waiting_writers.wait();
            } catch (InterruptedException ex) {
                Logger.getLogger(Locker.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        waiting_writers--;
        writers++;
    }

    public synchronized void releaseWriteLock() {
        if (--writers == 0) {
            if (waiting_readers > 0) {
                waiting_readers.notifyAll();
            }
            else if (waiting_writers > 0) {
                waiting_writers.notify();
            }
        }
    }
}
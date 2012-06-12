/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech;

import com.malhartech.Buffer.Data;
import com.sun.org.apache.xml.internal.utils.StylesheetPIHandler;

/**
 *
 * @author chetan
 */
public class DataList {

    private class DataArray {

        int offset;
        int count;
        int starting_window;
        int ending_window;
        Data data[];
        DataArray next;

        public DataArray(int capacity) {
            this.offset = 0;
            this.count = 0;
            this.starting_window = 0;
            this.ending_window = 0;
            data = new Data[capacity];
            next = null;
        }

        public void add(Data d) {
            data[count++] = d;

            if (d.getType() == Data.DataType.BEGIN_WINDOW) {
                if (starting_window == 0) {
                    starting_window = d.getBeginwindow().getWindowId();
                }
                ending_window = d.getBeginwindow().getWindowId();
            }
        }
        
        public void purge(int count) {
            this.offset = count;
            this.count -= count;
        }
        
    }
    
    Locker locker;
    DataArray first;
    DataArray last;
    int capacity;

    public DataList(int capacity) {
        this.capacity = capacity;
        this.locker = new Locker();
        first = last = new DataArray(capacity);
    }

    public void add(Data d) {
        locker.acquireWriteLock();

        if (last.count == capacity) {
            last.next = new DataArray(capacity);
            last = last.next;
        }
        last.add(d);

        locker.releaseWriteLock();
    }

    public void purge(int ending_id) {
        locker.acquireWriteLock();

        while (first != last && ending_id > first.ending_window) {
            first = first.next;
        }
        
        if (ending_id <= first.ending_window) {
            int offset = 0;
            while (offset < capacity) {
                Data d = first.data[offset++];
                if (d.getType() == Data.DataType.END_WINDOW &&
                        d.getEndwindow().getWindowId() == ending_id) {
                    break;
                }
            }
            first.offset = offset;
            first.count -= offset;
            while (offset < capacity) {
                Data d = first.data[offset++];
                if (d.getType() == Data.DataType.BEGIN_WINDOW) {
                    first.starting_window = d.getBeginwindow().getWindowId();
                }
            }            
        }
        else {
            first.offset = 0;
            first.count = 0;
            first.ending_window = 0;
            first.starting_window = 0;
        }
        
        locker.releaseWriteLock();
    }
}

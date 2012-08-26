/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.util.SerializedData;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */
class DataListIterator implements Iterator<SerializedData>
{
    private static final Logger logger = LoggerFactory.getLogger(DataListIterator.class);
    DataList.DataArray da;
    final DataIntrospector di;
    SerializedData previous = null;
    SerializedData current = new SerializedData();

    /**
     * 
     * @param da
     * @param di 
     */
    public DataListIterator(DataList.DataArray da, DataIntrospector di)
    {
        this.da = da;
        this.di = di;

        current.bytes = da.data;
    }

    /**
     * 
     * @return boolean
     */
    public boolean hasNext()
    {
        da.getNextData(current);
        switch (current.size) {
            case -1:
                return false;

            case 0:
                if (da.next == null) {
                    return false;
                }

                da = da.next;
                current.offset = 0;
                current.bytes = da.data;
                return hasNext();

            default:
                if (di.getType(current) == DataType.NO_DATA) {
                    current.offset += current.size;
                    return hasNext();
                }

                return true;
        }
    }

    /**
     * 
     * @return {@link com.malhartech.bufferserver.util.SerializedData}
     */
    public SerializedData next()
    {
        previous = current;
        current = new SerializedData();
        current.offset = previous.offset + previous.size;
        current.bytes = previous.bytes;
        return previous;
    }

    /**
     * Removes from the underlying collection the last element returned by the iterator (optional operation). This method can be called only once per call to
     * next. The behavior of an iterator is unspecified if the underlying collection is modified while the iteration is in progress in any way other than by
     * calling this method.
     */
    public void remove()
    {
        if (previous == null) {
            throw new IllegalStateException("Nothing to remove");
        }

        di.wipeData(previous);
    }

    /**
     * 
     * @return DataType
     */
    DataType getType()
    {
        return di.getType(previous);
    }

    /**
     * 
     * @return long
     */
    long getWindowId()
    {
        return di.getWindowId(previous);
    }

    /**
     * 
     * @return Object
     */
    Object getData()
    {
        return di.getData(previous);
    }
}

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 * 
 * 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */

/**
 * 
 * com.malhartech.util package consists of utilities code that Malhar provides and uses internally. The current utilities include
 * CircularBuffer: Takes a class T and provides a circular buffer. get() on the buffer consumes the object from tail, and add() adds to the head
 * StablePriorityQueue: Implements a priority queue (Queue<E>) and is mainly used to queue tuples. 
 * StableWrapper: Used to wrap around long int values safely. Used for windowIds
 */

package com.malhartech.util;


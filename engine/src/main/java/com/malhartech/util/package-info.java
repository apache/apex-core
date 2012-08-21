/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 * 
 * 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */

/**
 * 
 * com.malhartech.util package consists of utilities code that Malhar provides and uses internally.<br>
 * The current utilities include<br>
 * <b>CircularBuffer</b>: Takes a class T and provides a circular buffer. get() on the buffer consumes the object from tail, and add() adds to the head<br>
 * <b>StablePriorityQueue</b>: Implements a priority queue (Queue<E>) and is mainly used to queue tuples.<br> 
 * <b>StableWrapper</b>: Used to wrap around long int values safely. Used for windowIds<br>
 */

package com.malhartech.util;


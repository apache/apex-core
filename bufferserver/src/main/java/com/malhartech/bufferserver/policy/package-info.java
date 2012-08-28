/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */


/**
 * 
 * <b>com.malhartech.bufferserver.policy</b>Implements partion policies and provides basic classes/interface to write your own<p>
 * <br>
 * <b>com.malhartech.bufferserver.policy</b> includes<br>
 * <b>{@link com.malhartech.bufferserver.policy.AbstractPolicy}</b>: The base class for specifying partition policies, implements interface {@link com.malhartech.bufferserver.policy.Policy}<br>
 * <b>{@link com.malhartech.bufferserver.policy.GiveAll}</b>: Implements policy of giving a tuple to all nodes<br>
 * <b>{@link com.malhartech.bufferserver.policy.LeastBusy}</b>: Implements load balancing by sending the tuple to the least busy partition<br>
 * <b>{@link com.malhartech.bufferserver.policy.Policy}</b>: The base interface for implementing/specifying partition policies<br>
 * <b>{@link com.malhartech.bufferserver.policy.RandomOne}</b>: Randomly distributes tuples to downstream nodes. A random load balancing policy<br>
 * <b>{@link com.malhartech.bufferserver.policy.RoundRobin}</b>: Distributes to downstream nodes in a roundrobin fashion. A round robin load balancing policy<br>
 * <br>
 * 
 */

package com.malhartech.bufferserver.policy;

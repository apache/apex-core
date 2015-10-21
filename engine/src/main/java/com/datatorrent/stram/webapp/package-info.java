/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 *
 * <b>com.datatorrent.stram.webapp</b> package is the web service layer of Malhar's streaming platform<p>
 * <br>
 * The webservices layer interacts with the streaming application master (stram). The internals of Hadoop are taken care
 * off and an common set of functionality is provided for all streaming related runtime data.<br>
 * <br>
 * The webservices layer consists of the following data:<br>
 * <b>{@link com.datatorrent.stram.webapp.AppInfo}</b>: Provides application level data like user, appId, elapsed time, etc.<br>
 * <b>{@link com.datatorrent.stram.webapp.OperatorInfo}</b>: Provides data on the operator. This includes throughput, container id etc.<br>
 * <b>{@link com.datatorrent.stram.webapp.OperatorsInfo}</b>: Provides data on all the operators of the data.<br>
 * <b>{@link com.datatorrent.stram.webapp.StramWebApp}</b>: TBD<br>
 * <b>{@link com.datatorrent.stram.webapp.StramWebServices}</b>: TBD<br>
 * <b>Access and Authoriation</b>: TBD<br>
 * <br>
 *
 */

package com.datatorrent.stram.webapp;


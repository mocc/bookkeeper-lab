/**
 *
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
 *
 */
package org.apache.hedwig.client.netty.impl;

import org.apache.hedwig.protocol.PubSubProtocol.Message;


/**
 * Define the policy how to consume messages.
 */
interface ConsumePolicy {

    /**
     * Consume a specific message.
     *
     * @param msg
     *          Specific message.
     * @param consumer
     *          Consumer to do the actual consume.
     */
    public void messageConsumed(Message msg, Consumer consumer);
}

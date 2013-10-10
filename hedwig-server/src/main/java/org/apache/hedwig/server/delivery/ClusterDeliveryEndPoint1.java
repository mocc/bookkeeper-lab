/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.delivery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterDeliveryEndPoint1 implements DeliveryEndPoint, ThrottlingPolicy {
    private static final Logger logger = LoggerFactory.getLogger(ClusterDeliveryEndPoint1.class);

    volatile boolean closed = false;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    final LinkedHashMap<DeliveryEndPoint, DeliveryState> endpoints = new LinkedHashMap<DeliveryEndPoint, DeliveryState>();
    // endpoints store all clients' deliveryEndPoints which are not throttled
    final LinkedHashMap<DeliveryEndPoint, DeliveryState> throttledEndpoints = new LinkedHashMap<DeliveryEndPoint, DeliveryState>();
    // throttledEndpoints store all clients'deliveryEndpoints throttled
    final HashMap<Long, DeliveredMessage> pendings = new HashMap<Long, DeliveredMessage>();
    final String label;
    final ScheduledExecutorService scheduler;

    /*
     * modified by hrq.
     */
    static class DeliveryState {
        SortedSet<Long> msgs = new TreeSet<Long>();
        int messageWindowSize; // this is a message window size for every client

        public DeliveryState(int messageWindowSize) {
            this.messageWindowSize = messageWindowSize;
        }
    }

    class DeliveredMessage {
        final PubSubResponse msg;
        volatile long lastDeliveredTime;
        volatile DeliveryEndPoint lastDeliveredEP = null;

        DeliveredMessage(PubSubResponse msg) {
            this.msg = msg;
            this.lastDeliveredTime = MathUtils.now();
        }

        void resetDeliveredTime(DeliveryEndPoint ep) {
            DeliveryEndPoint oldEP = this.lastDeliveredEP;
            if (null != oldEP) {
                DeliveryState state = endpoints.get(oldEP);
                if (null != state) {
                    state.msgs.remove(msg.getMessage().getMsgId().getLocalComponent());
                }
            }
            this.lastDeliveredTime = MathUtils.now();
            this.lastDeliveredEP = ep;
        }
    }

    class ClusterDeliveryCallback implements DeliveryCallback {

        final DeliveryEndPoint ep;
        final DeliveryState state;
        final DeliveredMessage msg;
        final long deliveredTime;

        ClusterDeliveryCallback(DeliveryEndPoint ep, DeliveryState state, DeliveredMessage msg) {
            this.ep = ep;
            this.state = state;
            this.msg = msg;
            this.deliveredTime = msg.lastDeliveredTime;
            // add this msgs to current delivery endpoint state
            this.state.msgs.add(msg.msg.getMessage().getMsgId().getLocalComponent());
        }

        @Override
        public void sendingFinished() {
            // nop
        }

        @Override
        public void transientErrorOnSend() {
            closeAndRedeliver(ep, state);
        }

        @Override
        public void permanentErrorOnSend() {
            closeAndRedeliver(ep, state);
        }

    };

    /*
     * modified by hrq.
     */
    class RedeliveryTask implements Runnable {

        final DeliveryState state;

        RedeliveryTask(DeliveryState state) {
            this.state = state;
        }

        @Override
        public void run() {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    return;
                }
            } finally {
                closeLock.readLock().unlock();
            }
            logger.debug("Redelivery Task begin !!");
            Set<DeliveredMessage> msgs = new HashSet<DeliveredMessage>();

            synchronized (endpoints) {
                synchronized (throttledEndpoints) {
                    if (endpoints.containsValue(state) || throttledEndpoints.containsValue(state)) {
                        for (long seqid : state.msgs) {
                            DeliveredMessage msg = pendings.get(seqid);
                            if (null != msg) {
                                msgs.add(msg);
                            }
                        }
                    } else
                        return;
                }
            }

            for (DeliveredMessage msg : msgs) {
                DeliveryEndPoint ep = send(msg);
                if (null == ep) {
                    // no delivery channel found
                    ClusterDeliveryEndPoint1.this.close();
                    return;
                }
            }
        }

    }

    /*
     * modified by hrq.
     */
    public ClusterDeliveryEndPoint1(String label, ScheduledExecutorService scheduler) {
        this.label = label;
        this.scheduler = scheduler;
    }

    /*
     * modified by hrq.
     */
    public void addDeliveryEndPoint(DeliveryEndPoint endPoint, int messageWindowSize) {
        addDeliveryEndPoint(endPoint, new DeliveryState(messageWindowSize));
    }

    private void addDeliveryEndPoint(DeliveryEndPoint endPoint, DeliveryState state) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            synchronized (endpoints) {
                endpoints.put(endPoint, state);
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private void closeAndRedeliver(DeliveryEndPoint ep, DeliveryState state) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            // redeliver the state
            logger.debug("closeAndRedeliver all unconsumed messages in " + ep.toString());
            scheduler.submit(new RedeliveryTask(state));
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /*
     * modified by hrq.
     */
    public void removeDeliveryEndPoint(DeliveryEndPoint endPoint) {
        DeliveryState state = null;
        synchronized (endpoints) {
            synchronized (throttledEndpoints) {
                if (endpoints.containsKey(endPoint)) {
                    state = endpoints.remove(endPoint);
                    logger.debug(endPoint.toString() + "is removed from endpoints.");
                } else if (throttledEndpoints.containsKey(endPoint)) {
                    state = throttledEndpoints.remove(endPoint);
                    logger.debug(endPoint.toString() + "is removed from throttledEndpoints.");
                }

                if (null == state) {
                    return;
                } else if (state.msgs.size() != 0) {
                    return;
                }
            }
        }

        closeAndRedeliver(endPoint, state);
    }

    // the caller should synchronize
    private Entry<DeliveryEndPoint, DeliveryState> pollDeliveryEndPoint() {
        if (endpoints.isEmpty()) {
            return null;
        } else {
            Iterator<Entry<DeliveryEndPoint, DeliveryState>> iter = endpoints.entrySet().iterator();
            Entry<DeliveryEndPoint, DeliveryState> entry = iter.next();
            logger.debug("poll one client on sending." + "  channelEP is: " + entry.getKey().toString()
                    + "  window size is: " + entry.getValue().messageWindowSize + "  Num of unconsumed messages: "
                    + entry.getValue().msgs.size());
            iter.remove();
            return entry;
        }
    }

    public boolean hasAvailableDeliveryEndPoints() {
        synchronized (endpoints) {
            return !endpoints.isEmpty();
        }
    }

    /*
     * modified by hrq
     */
    @Override
    public boolean messageConsumed(long newSeqIdConsumed) {
        DeliveredMessage msg;
        DeliveryState state = null;

        msg = pendings.remove(newSeqIdConsumed);
        DeliveryEndPoint lastDeliveredEP = msg.lastDeliveredEP;

        if (null != msg && null != lastDeliveredEP) {
            synchronized (endpoints) {
                synchronized (throttledEndpoints) {
                    if (endpoints.containsKey(lastDeliveredEP)) {
                        state = endpoints.get(lastDeliveredEP);

                        if (state.msgs.size() != 0) {
                            state.msgs.remove(newSeqIdConsumed);
                            logger.debug("message is consumed in endpoints: "
                                    + msg.msg.getMessage().getBody().toStringUtf8());
                        }

                    } else if (throttledEndpoints.containsKey(lastDeliveredEP)) {
                        state = throttledEndpoints.get(lastDeliveredEP);

                        if (state.msgs.size() != 0) {
                            state.msgs.remove(newSeqIdConsumed);
                            logger.debug("message is consumed in throttledEndpoints: "
                                    + msg.msg.getMessage().getBody().toStringUtf8());
                            endpoints.put(lastDeliveredEP, state);
                            throttledEndpoints.remove(lastDeliveredEP);
                            logger.debug(lastDeliveredEP.toString()
                                    + "move from throttledEndpoints to endpoints on consuming");
                        }

                    } else
                        return false;
                }
            }
            return true;
        }
        return false;
    }

    /*
     * modified by hrq
     */
    @Override
    public boolean shouldThrottle(long lastSeqIdDelivered) {
        synchronized (endpoints) {
            return endpoints.isEmpty();
        }
    }

    @Override
    public void send(final PubSubResponse response, final DeliveryCallback callback) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                callback.permanentErrorOnSend();
                return;
            }
            DeliveryEndPoint ep = send(new DeliveredMessage(response));
            if (null == ep) {
                // no delivery endpoint
                callback.permanentErrorOnSend();
            } else {
                // callback after sending the message
                callback.sendingFinished();
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /*
     * modified by hrq.
     */
    private DeliveryEndPoint send(final DeliveredMessage msg) {
        Entry<DeliveryEndPoint, DeliveryState> entry = null;

        DeliveryCallback dcb;
        synchronized (endpoints) {
            entry = pollDeliveryEndPoint();
            if (null == entry) {
                // no delivery endpoint found
                return null;
            }
            // update the treeSet "msg" of deliveryState

            dcb = new ClusterDeliveryCallback(entry.getKey(), entry.getValue(), msg);
            long seqid = msg.msg.getMessage().getMsgId().getLocalComponent();
            msg.resetDeliveredTime(entry.getKey());
            pendings.put(seqid, msg);

            // we should check whether this deliveryEndpoint should be
            // throttled,
            // then we can decide to put into endpoints or throttledEndpoints

            if (entry.getValue().msgs.size() < entry.getValue().messageWindowSize)
                addDeliveryEndPoint(entry.getKey(), entry.getValue());
            else {
                synchronized (throttledEndpoints) {
                    throttledEndpoints.put(entry.getKey(), entry.getValue());
                    logger.debug(entry.getKey().toString() + "is moved from endpoints to throttledEndpoints on sending");
                }
            }

        }

        entry.getKey().send(msg.msg, dcb);
        // if this operation fails, trigger redelivery of this message.
        return entry.getKey();
    }

    /*
     * modified by hrq
     */
    public void sendSubscriptionEvent(PubSubResponse resp) {
        List<Entry<DeliveryEndPoint, DeliveryState>> eps;
        synchronized (endpoints) {
            synchronized (throttledEndpoints) {
                eps = new ArrayList<Entry<DeliveryEndPoint, DeliveryState>>(endpoints.entrySet());
                eps.addAll(throttledEndpoints.entrySet());
            }
        }

        for (final Entry<DeliveryEndPoint, DeliveryState> entry : eps) {
            entry.getKey().send(resp, new DeliveryCallback() {

                @Override
                public void sendingFinished() {
                    // do nothing
                }

                @Override
                public void transientErrorOnSend() {
                    closeAndRedeliver(entry.getKey(), entry.getValue());
                }

                @Override
                public void permanentErrorOnSend() {
                    closeAndRedeliver(entry.getKey(), entry.getValue());
                }

            });
        }
    }

    @Override
    public void close() {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
    }

}

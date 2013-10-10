package org.apache.hedwig.server.delivery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterDeliveryEndPoint2 implements DeliveryEndPoint, ThrottlingPolicy {
    private static final Logger logger = LoggerFactory.getLogger(ClusterDeliveryEndPoint2.class);

    volatile boolean closed = false;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    // endpoints store all clients' deliveryEndPoints which are not throttled
    final Map<ClusterEndPoint, DeliveryState> endpoints = Collections
            .synchronizedMap(new TreeMap<ClusterEndPoint, DeliveryState>());

    // throttledEndpoints store all clients'deliveryEndpoints throttled
    final Map<ClusterEndPoint, DeliveryState> throttledEndpoints = Collections
            .synchronizedMap(new TreeMap<ClusterEndPoint, DeliveryState>());

    final HashMap<Long, DeliveredMessage> pendings = new HashMap<Long, DeliveredMessage>();
    final String label;
    final ScheduledExecutorService scheduler;

    /*
     * add by hrq
     */
    class ClusterEndPoint implements Comparable<ClusterEndPoint> {
        DeliveryEndPoint endPoint;
        long timeStamp;

        ClusterEndPoint(DeliveryEndPoint endPoint) {
            this.endPoint = endPoint;
            this.timeStamp = System.currentTimeMillis();
        }

        @Override
        public int compareTo(ClusterEndPoint ep) {
            return this.timeStamp < ep.timeStamp ? -1 : ((this.timeStamp == ep.timeStamp) ? 0 : 1);
        }

        public void send(PubSubResponse response, DeliveryCallback callback) {
            this.endPoint.send(response, callback);
        }

    }

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
        volatile ClusterEndPoint lastDeliveredEP = null;

        DeliveredMessage(PubSubResponse msg) {
            this.msg = msg;
            this.lastDeliveredTime = MathUtils.now();
        }

        void resetDeliveredTime(ClusterEndPoint ep) {
            ClusterEndPoint oldEP = this.lastDeliveredEP;
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

        final ClusterEndPoint ep;
        final DeliveryState state;
        final DeliveredMessage msg;
        final long deliveredTime;

        ClusterDeliveryCallback(ClusterEndPoint ep, DeliveryState state, DeliveredMessage msg) {
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
                    }
                }
            }

            for (DeliveredMessage msg : msgs) {
                ClusterEndPoint ep = send(msg);
                if (null == ep) {
                    // no delivery channel found
                    ClusterDeliveryEndPoint2.this.close();
                    return;
                }
            }
        }

    }

    /*
     * modified by hrq.
     */
    public ClusterDeliveryEndPoint2(String label, ScheduledExecutorService scheduler) {
        this.label = label;
        this.scheduler = scheduler;
    }

    /*
     * modified by hrq.
     */
    public void addDeliveryEndPoint(DeliveryEndPoint endPoint, int messageWindowSize) {
        addDeliveryEndPoint(new ClusterEndPoint(endPoint), new DeliveryState(messageWindowSize));
    }

    private void addDeliveryEndPoint(ClusterEndPoint endPoint, DeliveryState state) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            endpoints.put(endPoint, state);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private void closeAndRedeliver(ClusterEndPoint ep, DeliveryState state) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            // redeliver the state
            scheduler.submit(new RedeliveryTask(state));
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /*
     * modified by hrq.
     * 
     * call methods in FIFODeliveryManager should be modified at the same time
     */
    public void removeDeliveryEndPoint(ClusterEndPoint endPoint) {
        DeliveryState state = null;
        synchronized (endpoints) {
            synchronized (throttledEndpoints) {
                if (endpoints.containsKey(endPoint)) {
                    state = endpoints.remove(endPoint);
                } else {
                    if (throttledEndpoints.containsKey(endPoint))
                        state = throttledEndpoints.remove(endPoint);
                }
            }
        }

        if (null == state) {
            return;
        } else if (state.msgs == null) {
            return;
        }
        closeAndRedeliver(endPoint, state);
    }

    // the caller should synchronize
    private Entry<ClusterEndPoint, DeliveryState> pollDeliveryEndPoint() {
        if (endpoints.isEmpty()) {
            return null;
        } else {
            Iterator<Entry<ClusterEndPoint, DeliveryState>> iter = endpoints.entrySet().iterator();
            Entry<ClusterEndPoint, DeliveryState> entry = iter.next();
            logger.debug("poll one client on sending." + "  channelEP is: " + entry.getKey().endPoint.toString()
                    + "  window size is: " + entry.getValue().messageWindowSize + "  Num of unconsumed messages: "
                    + entry.getValue().msgs.size());
            iter.remove();
            return entry;
        }
    }

    public boolean hasAvailableDeliveryEndPoints() {
        return !endpoints.isEmpty();
    }

    /*
     * modified by hrq
     */
    @Override
    public boolean messageConsumed(long newSeqIdConsumed) {
        DeliveredMessage msg;
        DeliveryState state = null;

        msg = pendings.remove(newSeqIdConsumed);
        ClusterEndPoint lastDeliveredEP = msg.lastDeliveredEP;

        if (null != msg && null != lastDeliveredEP) {
            synchronized (endpoints) {
                synchronized (throttledEndpoints) {
                    if (endpoints.containsKey(lastDeliveredEP)) {
                        state = endpoints.get(lastDeliveredEP);

                        if (state.msgs.size() != 0) {
                            state.msgs.remove(newSeqIdConsumed);
                        }

                    } else if (throttledEndpoints.containsKey(lastDeliveredEP)) {
                        state = throttledEndpoints.get(lastDeliveredEP);

                        if (state.msgs.size() != 0) {
                            state.msgs.remove(newSeqIdConsumed);
                            endpoints.put(lastDeliveredEP, state);
                            throttledEndpoints.remove(lastDeliveredEP);
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
        return endpoints.isEmpty();
    }

    @Override
    public void send(final PubSubResponse response, final DeliveryCallback callback) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                callback.permanentErrorOnSend();
                return;
            }
            ClusterEndPoint ep = send(new DeliveredMessage(response));
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
    private ClusterEndPoint send(final DeliveredMessage msg) {
        Entry<ClusterEndPoint, DeliveryState> entry = null;

        DeliveryCallback dcb;
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

        // we should check whether this deliveryEndpoint should be throttled,
        if (entry.getValue().msgs.size() < entry.getValue().messageWindowSize)
            addDeliveryEndPoint(entry.getKey(), entry.getValue());
        else
            throttledEndpoints.put(entry.getKey(), entry.getValue());

        entry.getKey().send(msg.msg, dcb);
        // if this operation fails, trigger redelivery of this message.
        return entry.getKey();
    }

    /*
     * modified by hrq
     */
    public void sendSubscriptionEvent(PubSubResponse resp) {
        List<Entry<ClusterEndPoint, DeliveryState>> eps;
        synchronized (endpoints) {
            synchronized (throttledEndpoints) {
                eps = new ArrayList<Entry<ClusterEndPoint, DeliveryState>>(endpoints.entrySet());
                eps.addAll(throttledEndpoints.entrySet());
            }
        }

        for (final Entry<ClusterEndPoint, DeliveryState> entry : eps) {
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

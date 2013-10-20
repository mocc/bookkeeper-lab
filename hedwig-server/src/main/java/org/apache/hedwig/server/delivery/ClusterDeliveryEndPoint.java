package org.apache.hedwig.server.delivery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterDeliveryEndPoint implements DeliveryEndPoint, ThrottlingPolicy {
    private static final Logger logger = LoggerFactory.getLogger(ClusterDeliveryEndPoint.class);

    volatile boolean closed = false;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    // endpoints store all clients' deliveryEndPoints which are not throttled
    final ConcurrentHashMap<DeliveryEndPoint, DeliveryState> endpoints = new ConcurrentHashMap<DeliveryEndPoint, DeliveryState>();
    final ConcurrentHashMap<Long, DeliveredMessage> pendings = new ConcurrentHashMap<Long, DeliveredMessage>();
    final LinkedBlockingQueue<DeliveryEndPoint> deliverableEP = new LinkedBlockingQueue<DeliveryEndPoint>();
    final ConcurrentLinkedQueue<DeliveryEndPoint> throttledEP = new ConcurrentLinkedQueue<DeliveryEndPoint>();

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
            Set<DeliveredMessage> msgs = new HashSet<DeliveredMessage>();

            for (long seqid : state.msgs) {
                DeliveredMessage msg = pendings.get(seqid);
                if (null != msg) {
                    msgs.add(msg);
                }
            }

            for (DeliveredMessage msg : msgs) {
                DeliveryEndPoint ep = send(msg);
                if (null == ep) {
                    // no delivery channel in endpoints
                    ClusterDeliveryEndPoint.this.close();
                    return;
                }
            }
        }

    }

    /*
     * modified by hrq.
     */
    public ClusterDeliveryEndPoint(String label, ScheduledExecutorService scheduler) {
        this.label = label;
        this.scheduler = scheduler;
    }

    /*
     * modified by hrq.
     */
    public void addDeliveryEndPoint(DeliveryEndPoint channelEP, int messageWindowSize) {
        addDeliveryEndPoint(channelEP, new DeliveryState(messageWindowSize));
    }

    private void addDeliveryEndPoint(DeliveryEndPoint endPoint, DeliveryState state) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            deliverableEP.add(endPoint);
            endpoints.put(endPoint, state);
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
            if (null == state) {
                return;
            }
            if (state.msgs.isEmpty()) {
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
     */
    public void removeDeliveryEndPoint(DeliveryEndPoint endPoint) {
        synchronized (deliverableEP) {
            if (deliverableEP.contains(endPoint)) {
                deliverableEP.remove(endPoint);
            } else
                throttledEP.remove(endPoint);
        }

        DeliveryState state = endpoints.remove(endPoint);
        if (endpoints.isEmpty()) {
            close();
        }
        if (null == state) {
            return;
        }
        if (state.msgs.isEmpty()) {
            return;
        }
        closeAndRedeliver(endPoint, state);
    }

    public boolean hasAvailableDeliveryEndPoints() {
        return !deliverableEP.isEmpty();
    }

    /*
     * modified by hrq
     */
    @Override
    public boolean messageConsumed(long newSeqIdConsumed) {
        DeliveredMessage msg;

        msg = pendings.remove(newSeqIdConsumed);
        DeliveryEndPoint lastDeliveredEP = msg.lastDeliveredEP;

        if (null != msg && null != lastDeliveredEP) {

            DeliveryState state = endpoints.get(lastDeliveredEP);
            if (state != null) {
                state.msgs.remove(newSeqIdConsumed);
            }
            if (throttledEP.contains(lastDeliveredEP)) {
                throttledEP.remove(lastDeliveredEP);
                deliverableEP.offer(lastDeliveredEP);
                return true;
            }
        }
        return false;
    }

    /*
     * modified by hrq
     */
    @Override
    public boolean shouldThrottle(long lastSeqIdDelivered) {
        return deliverableEP.isEmpty();
    }

    @Override
    public void send(final PubSubResponse response, final DeliveryCallback callback) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                callback.permanentErrorOnSend();
                return;
            }
        } finally {
            closeLock.readLock().unlock();
        }
        DeliveryEndPoint ep = send(new DeliveredMessage(response));
        if (null == ep) {
            // no delivery endpoint in cluster
            callback.permanentErrorOnSend();
        } else {
            // callback after sending the message
            callback.sendingFinished();
        }
    }

    /*
     * modified by hrq.
     */
    private DeliveryEndPoint send(final DeliveredMessage msg) {

        DeliveryCallback dcb;
        DeliveryEndPoint clusterEP = null;
        while (!endpoints.isEmpty()) {
            try {
                clusterEP = deliverableEP.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
            if (null == clusterEP) {
                continue;
            }

            long seqid = msg.msg.getMessage().getMsgId().getLocalComponent();
            // update sending message and remove messageID from old_EP
            msg.resetDeliveredTime(clusterEP);
            pendings.put(seqid, msg);

            DeliveryState state = endpoints.get(clusterEP);
            // add messageID to new_EP
            dcb = new ClusterDeliveryCallback(clusterEP, state, msg);

            // check whether this deliveryEndpoint should be throttled,
            if (state.msgs.size() < state.messageWindowSize)
                deliverableEP.offer(clusterEP);
            else
                throttledEP.offer(clusterEP);

            clusterEP.send(msg.msg, dcb);
            // if this operation fails, trigger redelivery of this message.
            return clusterEP;
        }
        return null;

    }

    /*
     * modified by hrq
     */
    public void sendSubscriptionEvent(PubSubResponse resp) {
        List<DeliveryEndPoint> eps = new ArrayList<DeliveryEndPoint>(deliverableEP);
        eps.addAll(throttledEP);

        for (final DeliveryEndPoint clusterEP : eps) {
            clusterEP.send(resp, new DeliveryCallback() {

                @Override
                public void sendingFinished() {
                    // do nothing
                }

                @Override
                public void transientErrorOnSend() {
                    closeAndRedeliver(clusterEP, endpoints.get(clusterEP));
                }

                @Override
                public void permanentErrorOnSend() {
                    closeAndRedeliver(clusterEP, endpoints.get(clusterEP));
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

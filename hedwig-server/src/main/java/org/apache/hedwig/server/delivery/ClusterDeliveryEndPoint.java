package org.apache.hedwig.server.delivery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
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
        public DeliveryState getState(){
        	return state;
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
    /*<-- added by liuyao*/
    class TimeOutRedeliveryTask implements Runnable {


        //final DeliveryState state;
    	final Set<DeliveredMessage> msgs;

        TimeOutRedeliveryTask(Set<DeliveredMessage> msgs) {
            this.msgs = msgs;
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
            for (DeliveredMessage msg : msgs) {
                DeliveryEndPoint ep = send(msg);
                if (null == ep) {
                    // no delivery channel found
                    ClusterDeliveryEndPoint.this.close();
                    return;
                }
            }
        }

    }
    /* added by liuyao -->*/
   

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
            //System.out.println("add endpoint:::::::::::::"+endPoint);
            endpoints.put(endPoint, state);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public void closeAndRedeliver(DeliveryEndPoint ep, DeliveryState state) {
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

    /*<-- added by liuyao*/
    public void closeAndTimeOutRedeliver(DeliveryEndPoint ep,Set<DeliveredMessage> msgs){
    	closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            // redeliver the state
            scheduler.submit(new TimeOutRedeliveryTask(msgs));
        } finally {
            closeLock.readLock().unlock();
        }
    }
    public void closeAndTimeOutRedeliver(Set<DeliveredMessage> msgs){
    	closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            // redeliver the state
            scheduler.submit(new TimeOutRedeliveryTask(msgs));
        } finally {
            closeLock.readLock().unlock();
        }
    }
    /* added by liuyao -->*/


    /*
     * modified by hrq.
     */

    public void removeDeliveryEndPoint(DeliveryEndPoint endPoint) {
        synchronized (deliverableEP) {
            if (deliverableEP.contains(endPoint)) {
                deliverableEP.remove(endPoint);
                //System.out.println("deliverableEP:::remove a channel"+endPoint.toString());
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


    // the caller should synchronize
    private Entry<DeliveryEndPoint, DeliveryState> pollDeliveryEndPoint() {
        if (endpoints.isEmpty()) {
            return null;
        } else {
            Iterator<Entry<DeliveryEndPoint, DeliveryState>> iter = endpoints.entrySet().iterator();
            Entry<DeliveryEndPoint, DeliveryState> entry = iter.next();
            iter.remove();
            return entry;
        }
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
        //DeliveryEndPoint lastDeliveredEP = msg.lastDeliveredEP;
        DeliveryEndPoint lastDeliveredEP=null;
        if(null != msg)lastDeliveredEP = msg.lastDeliveredEP;

        if (null != msg && null != lastDeliveredEP) {

            DeliveryState state = endpoints.get(lastDeliveredEP);
            if (state != null) {
                state.msgs.remove(newSeqIdConsumed);
            }
            synchronized(deliverableEP){
            	if (throttledEP.contains(lastDeliveredEP)) {
                    throttledEP.remove(lastDeliveredEP);
                    deliverableEP.offer(lastDeliveredEP);
                    return true;
                }
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

    /*<-- modified by liuyao*/


    /*
     * modified by hrq.
     */

    private DeliveryEndPoint send(final DeliveredMessage msg) {

        Entry<DeliveryEndPoint, DeliveryState> entry ;



        DeliveryCallback dcb;

        DeliveryEndPoint clusterEP = null;
        while (!endpoints.isEmpty()) {
            try {
                clusterEP = deliverableEP.poll(1, TimeUnit.SECONDS);
                //System.out.println("deliverableEP size::::::::::::::::::::::::"+deliverableEP);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();

            }
            if (null == clusterEP) {
                continue;
            }
            DeliveryState state = endpoints.get(clusterEP);
            
            msg.resetDeliveredTime(clusterEP);
            dcb = new ClusterDeliveryCallback(clusterEP, state, msg);
            
            long seqid = msg.msg.getMessage().getMsgId().getLocalComponent();
            pendings.put(seqid, msg);

            // check whether this deliveryEndpoint should be throttled,
            if (state.messageWindowSize==0 || state.msgs.size() < state.messageWindowSize){
                deliverableEP.offer(clusterEP);
                
            }
            else{
                throttledEP.offer(clusterEP);
            }

            clusterEP.send(msg.msg, dcb);
            //System.out.println(clusterEP.toString()+"send:::::::::"+msg.msg.getMessage().getBody().toStringUtf8());
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

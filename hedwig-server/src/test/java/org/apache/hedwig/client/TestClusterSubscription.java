package org.apache.hedwig.client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionType;
import org.apache.hedwig.server.PubSubServerStandAloneTestBase;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestClusterSubscription extends PubSubServerStandAloneTestBase {
    // Client side variables
    protected HedwigClient client1, client2, client3;
    protected Publisher publisher1, publisher2, publisher3;
    protected Subscriber subscriber1, subscriber2, subscriber3;
    protected boolean isAutoSendConsumeMessageEnabled;

    protected class ClusterClientConfiguration extends ClientConfiguration {
        @Override
        public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
            return getDefaultHedwigAddress();
        }

        @Override
        public boolean isAutoSendConsumeMessageEnabled() {
            return TestClusterSubscription.this.isAutoSendConsumeMessageEnabled;

        }
    }

    public ClientConfiguration getClusterClientConfiguration() {
        return new ClusterClientConfiguration();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        client1.close();
        client2.close();
        client3.close();
        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testClusterSubscriptionWithAutoConsume() throws Exception {
        TestClusterSubscription.this.isAutoSendConsumeMessageEnabled = true;
        final int messageWindowSize = 20;
        ClientConfiguration conf = getClusterClientConfiguration();

        client1 = new HedwigClient(conf);
        client2 = new HedwigClient(conf);
        client3 = new HedwigClient(conf);

        publisher1 = client1.getPublisher();
        publisher2 = client2.getPublisher();
        publisher3 = client3.getPublisher();

        subscriber1 = client1.getSubscriber();
        subscriber2 = client2.getSubscriber();
        subscriber3 = client3.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testClusterSubscriptionWithAutoConsume");
        ByteString subscriberId = ByteString.copyFromUtf8("mysubid");

        final String prefix = "message";
        final int numMessages = 100;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final ConcurrentHashMap<String, MessageSeqId> receivedMsgs = new ConcurrentHashMap<String, MessageSeqId>();

        SubscriptionOptions opts1 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(messageWindowSize).build();

        subscriber1.subscribe(topic, subscriberId, opts1);
        subscriber2.subscribe(topic, subscriberId, opts1);
        subscriber3.subscribe(topic, subscriberId, opts1);

        subscriber1.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber2.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber3.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);

            }
        });

        for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher1.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);
        }

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publish responses.", numMessages, publishedMsgs.size());

        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.", numMessages, numReceived.get());
        assertEquals("Should be expected " + numMessages + " messages in map.", numMessages, receivedMsgs.size());

    }

    @Test(timeout = 60000)
    public void testClusterSubscriptionWithThrottle() throws Exception {
        TestClusterSubscription.this.isAutoSendConsumeMessageEnabled = false;
        final int messageWindowSize = 80;
        ClientConfiguration conf = getClusterClientConfiguration();

        client1 = new HedwigClient(conf);
        client2 = new HedwigClient(conf);
        client3 = new HedwigClient(conf);

        publisher1 = client1.getPublisher();
        publisher2 = client2.getPublisher();
        publisher3 = client3.getPublisher();

        subscriber1 = client1.getSubscriber();
        subscriber2 = client2.getSubscriber();
        subscriber3 = client3.getSubscriber();

        ByteString topic = ByteString.copyFromUtf8("testClusterSubscriptionWithThrottle");
        ByteString subscriberId = ByteString.copyFromUtf8("mysubid");

        final String prefix = "message";
        final int numMessages = 100;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final ConcurrentHashMap<String, MessageSeqId> receivedMsgs = new ConcurrentHashMap<String, MessageSeqId>();

        SubscriptionOptions opts1 = SubscriptionOptions.newBuilder().setSubscriptionType(SubscriptionType.CLUSTER)
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setMessageWindowSize(messageWindowSize).build();

        subscriber1.subscribe(topic, subscriberId, opts1);
        subscriber2.subscribe(topic, subscriberId, opts1);
        subscriber3.subscribe(topic, subscriberId, opts1);

        subscriber1.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (messageWindowSize == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber2.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (messageWindowSize == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

        subscriber3.startDelivery(topic, subscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                // TODO Auto-generated method stub
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.putIfAbsent(str, msg.getMsgId());
                if (messageWindowSize == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);

            }
        });

        for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            ByteString data = ByteString.copyFromUtf8(str);
            Message msg = Message.newBuilder().setBody(data).build();
            publisher1.asyncPublishWithResponse(topic, msg, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);
        }

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publish responses.", numMessages, publishedMsgs.size());

        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(30, TimeUnit.SECONDS));
        Thread.sleep(10000);
        assertEquals("Should be expected " + messageWindowSize + " messages.", messageWindowSize, numReceived.get());
        assertEquals("Should be expected " + messageWindowSize + " messages in map.", messageWindowSize,
                receivedMsgs.size());

        for (int i = 0; i < messageWindowSize; i++) {
            final String str = prefix + i;
            MessageSeqId pubId = publishedMsgs.get(str);
            MessageSeqId revId = receivedMsgs.get(str);
            assertTrue("Doesn't receive same message seq id for " + str, pubId.equals(revId));
        }

    }

}
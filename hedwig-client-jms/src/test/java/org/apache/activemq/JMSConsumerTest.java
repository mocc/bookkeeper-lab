/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;




import junit.framework.Test;


import javax.jms.Destination;

import org.apache.hedwig.jms.MessagingSessionFacade;
import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionFactoryImpl;
import org.apache.hedwig.jms.spi.HedwigConnectionImpl;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases used to test the JMS message consumer.
 */
public class JMSConsumerTest extends JmsTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JMSConsumerTest.class);

    public Destination destination;
    public int deliveryMode;
    public int prefetch;
    public int ackMode;
    public MessagingSessionFacade.DestinationType destinationType;
    public boolean durableConsumer;

    public static Test suite() {
        return suite(JMSConsumerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestMessageListenerWithConsumerCanBeStopped() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new MessagingSessionFacade.DestinationType[] {
                MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testMessageListenerWithConsumerCanBeStopped() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done1 = new CountDownLatch(1);
        final CountDownLatch done2 = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = (MessageConsumer)session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 1) {
                    done1.countDown();
                }
                if (counter.get() == 2) {
                    done2.countDown();
                }
            }
        });

        // Send a first message to make sure that the consumer dispatcher is
        // running
        sendMessages(session, destination, 1);
        assertTrue(done1.await(1, TimeUnit.SECONDS));
        assertEquals(1, counter.get());

        // Stop the consumer.
        connection.stop();

        // Send a message, but should not get delivered.
        sendMessages(session, destination, 1);
        assertFalse(done2.await(1, TimeUnit.SECONDS));
        assertEquals(1, counter.get());

        // Start the consumer, and the message should now get delivered.
        connection.start();
        assertTrue(done2.await(1, TimeUnit.SECONDS));
        assertEquals(2, counter.get());
    }

    public void testMessageListenerWithConsumerCanBeStoppedConcurently() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch closeDone = new CountDownLatch(1);

        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = createDestination(session, MessagingSessionFacade.DestinationType.TOPIC);

        final Map<Thread, Throwable> exceptions =
            Collections.synchronizedMap(new HashMap<Thread, Throwable>());
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Uncaught exception:", e);
                exceptions.put(t, e);
            }
        });

        final int numOutStanding = (connection.getHedwigClientConfig().getMaximumOutstandingMessages() * 2 / 3) + 1;

        final MessageConsumer consumer = (MessageConsumer)session.createConsumer(destination);

        final class AckAndClose implements Runnable {
            private Message message;

            public AckAndClose(Message m) {
                this.message = m;
            }

            public void run() {
                try {
                    message.acknowledge();
                    int count = counter.incrementAndGet();
                    if (590 == count) {
                        // close in a separate thread is ok by jms
                        consumer.close();
                        closeDone.countDown();
                    }
                } catch (Exception e) {
                    LOG.error("Exception on close or ack:", e);
                    exceptions.put(Thread.currentThread(), e);
                }
            }
        };

        final AtomicInteger listenerReceivedCount = new AtomicInteger(0);
        // final ExecutorService executor = Executors.newSingleThreadExecutor();
        final ExecutorService executor = Executors.newCachedThreadPool();
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message m) {
                // close can be in a different thread, but NOT acknowledge iirc
                // - this will not cause a problem for us though ...
                // ack and close eventually in separate thread
                int val = listenerReceivedCount.incrementAndGet();
                // System.out.println("message count : " + val + ", message : " + m);
                executor.execute(new AckAndClose(m));
                // new AckAndClose(m).run();
            }
        });

        // preload the queue
        sendMessages(session, destination, 600);

        assert closeDone.await(10, TimeUnit.SECONDS) :
        "closeDone : " + closeDone.getCount() + ", counter : " + counter.get()
            + ", listenerReceivedCount : " + listenerReceivedCount.get();
        // await possible exceptions
        Thread.sleep(1000);
        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }


    public void initCombosForTestMutiReceiveWithPrefetch1() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("ackMode", new Object[] {
                Integer.valueOf(Session.AUTO_ACKNOWLEDGE), Integer.valueOf(Session.DUPS_OK_ACKNOWLEDGE),
                Integer.valueOf(Session.CLIENT_ACKNOWLEDGE)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testMutiReceiveWithPrefetch1() throws Exception {

        // Set prefetch to 1
        connection.start();

        // Use all the ack modes
        Session session = connection.createSession(false, ackMode);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 4);

        // Make sure 4 messages were delivered.
        Message message = null;
        for (int i = 0; i < 4; i++) {
            message = consumer.receive(1000);
            assertNotNull(message);
        }
        assertNull(consumer.receiveNoWait());
        assert null != message;
        message.acknowledge();
    }

    public void initCombosForTestDurableConsumerSelectorChange() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testDurableConsumerSelectorChange() throws Exception {

        // Receive a message with the JMS API
        if (null == connection.getClientID()) connection.setClientID(getName() + "test");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);
        MessageConsumer consumer = session.createDurableSubscriber((Topic)destination, "test", "color='red'", false);

        // Send the messages
        TextMessage message = session.createTextMessage("1st");
        message.setStringProperty("color", "red");
        producer.send(message);

        Message m = consumer.receive(1000);
        assertNotNull(m);
        assertEquals("1st", ((TextMessage)m).getText());

        // Change the subscription.
        consumer.close();
        consumer = session.createDurableSubscriber((Topic)destination, "test", "color='blue'", false);

        message = session.createTextMessage("2nd");
        message.setStringProperty("color", "red");
        producer.send(message);
        message = session.createTextMessage("3rd");
        message.setStringProperty("color", "blue");
        producer.send(message);

        // Selector should skip the 2nd message.
        m = consumer.receive(1000);
        assertNotNull(m);
        assertEquals("3rd", ((TextMessage)m).getText());

        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestSendReceiveBytesMessage() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testSendReceiveBytesMessage() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        BytesMessage message = session.createBytesMessage();
        message.writeBoolean(true);
        message.writeBoolean(false);
        producer.send(message);

        // Make sure only 1 message was delivered.
        BytesMessage m = (BytesMessage)consumer.receive(1000);
        assertNotNull(m);
        assertTrue(m.readBoolean());
        assertFalse(m.readBoolean());

        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestSetMessageListenerAfterStart() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testSetMessageListenerAfterStart() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // See if the message get sent to the listener
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 4) {
                    done.countDown();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);

        assertTrue(done.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestPassMessageListenerIntoCreateConsumer() {
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testPassMessageListenerIntoCreateConsumer() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();
        SessionImpl session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 4) {
                    done.countDown();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);

        assertTrue(done.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestMessageListenerOnMessageCloseUnackedWithPrefetch1StayInQueue() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("ackMode", new Object[] {Integer.valueOf(Session.CLIENT_ACKNOWLEDGE)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testMessageListenerOnMessageCloseUnackedWithPrefetch1StayInQueue() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch sendDone = new CountDownLatch(1);
        final CountDownLatch got2Done = new CountDownLatch(1);

        // Set prefetch to 1
        // This test case does not work if optimized message dispatch is used as
        // the main thread send block until the consumer receives the
        // message. This test depends on thread decoupling so that the main
        // thread can stop the consumer thread.
        if (null == connection.getClientID()) connection.setClientID(getName() + "test-client-id-1");
        connection.start();

        // Use all the ack modes
        Session session = connection.createSession(false, ackMode);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id1");
        consumer.setMessageListener(new MessageListener() {
            private final HedwigConnectionImpl _connection = connection;
            public void onMessage(Message m) {
                try {
                    TextMessage tm = (TextMessage)m;
                    LOG.info("Got in first listener: " + tm.getText());
                    assertEquals(messageTextPrefix + counter.get(), tm.getText());
                    counter.incrementAndGet();
                    if (counter.get() == 2) {
                        sendDone.await();
                        _connection.close();
                        got2Done.countDown();
                    }
                    // will fail when we close connection when counter == 2 !
                    tm.acknowledge();
                } catch (Throwable e) {
                    // e.printStackTrace();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);
        sendDone.countDown();

        // Wait for first 2 messages to arrive.
        assert got2Done.await(5, TimeUnit.SECONDS) :
        "counter1 : " + counter.get() + ", got2Done : " + got2Done.getCount() + ", sendDone : " + sendDone.getCount();

        // Re-start connection.
        connection.close();
        connection = (HedwigConnectionImpl)factory.createConnection();
        if (null == connection.getClientID()) connection.setClientID(getName() + "test-client-id-1");
        connections.add(connection);

        // Pickup the remaining messages.
        final CountDownLatch done2 = new CountDownLatch(1);
        session = connection.createSession(false, ackMode);
        consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id1");
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message m) {
                try {
                    TextMessage tm = (TextMessage)m;
                    LOG.info("Got in second listener: " + tm.getText());
                    // order is not guaranteed as the connection is started before the listener is set.
                    // assertEquals(messageTextPrefix + counter.get(), tm.getText());
                    counter.incrementAndGet();
                    tm.acknowledge();
                    if (counter.get() == 4) {
                        done2.countDown();
                    }
                } catch (Throwable e) {
                    LOG.error("unexpected ex onMessage: ", e);
                }
            }
        });

        connection.start();

        assert done2.await(2000, TimeUnit.MILLISECONDS) :
        "count2 : " + done2.getCount() + ", counter : " + counter.get();
        Thread.sleep(200);

        // assert msg 2 was redelivered as close() from onMessages() will only ack in auto_ack and dups_ok mode
        assert 5 == counter.get(): "count3 : " + done2.getCount() + ", counter : " + counter.get();
    }

    public void initCombosForTestMessageListenerAutoAckOnCloseWithPrefetch1() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("ackMode", new Object[] {
                Integer.valueOf(Session.AUTO_ACKNOWLEDGE), Integer.valueOf(Session.CLIENT_ACKNOWLEDGE)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testMessageListenerAutoAckOnCloseWithPrefetch1() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch sendDone = new CountDownLatch(1);
        final CountDownLatch got2Done = new CountDownLatch(1);

        // Set prefetch to 1
        // This test case does not work if optimized message dispatch is used as
        // the main thread send block until the consumer receives the
        // message. This test depends on thread decoupling so that the main
        // thread can stop the consumer thread.
        if (null == connection.getClientID()) connection.setClientID(getName() + "test-client-id-2");
        connection.start();

        // Use all the ack modes
        Session session = connection.createSession(false, ackMode);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id2");
        final List<Message> receivedMessages = new ArrayList<Message>(8);
        consumer.setMessageListener(new MessageListener() {
            final HedwigConnectionImpl _connection = connection;
            public void onMessage(Message m) {
                try {
                    TextMessage tm = (TextMessage)m;
                    LOG.info("Got in first listener: " + tm.getText());
                    assertEquals(messageTextPrefix + counter.get(), tm.getText());
                    counter.incrementAndGet();
                    m.acknowledge();
                    receivedMessages.add(m);
                    if (counter.get() == 2) {
                        sendDone.await();
                        _connection.close();
                        got2Done.countDown();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);
        sendDone.countDown();

        // Wait for first 2 messages to arrive.
        assert got2Done.await(5, TimeUnit.SECONDS) :
        "counter : " + counter.get() + ", got2Done : " + got2Done.getCount() + ", sendDone : " + sendDone.getCount();

        // Re-start connection.
        connection.close();
        connection = (HedwigConnectionImpl)factory.createConnection();
        if (null == connection.getClientID()) connection.setClientID(getName() + "test-client-id-2");
        connections.add(connection);

        // Pickup the remaining messages.
        final CountDownLatch done2 = new CountDownLatch(1);
        session = connection.createSession(false, ackMode);
        consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id2");
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message m) {
                try {
                    TextMessage tm = (TextMessage)m;
                    LOG.info("Got in second listener: " + tm.getText());
                    counter.incrementAndGet();
                    m.acknowledge();
                    receivedMessages.add(m);
                    if (counter.get() == 4) {
                        done2.countDown();
                    }
                } catch (Throwable e) {
                    LOG.error("unexpected ex onMessage: ", e);
                }
            }
        });

        connection.start();

        assert done2.await(5, TimeUnit.SECONDS) : "count : " + done2.getCount() + ", counter : " + counter.get();
        Thread.sleep(200);

        // close from onMessage with Auto_ack will ack
        // Make sure only 4 messages were delivered.
        assert 4 == counter.get() :
        "counter : " + counter.get() + ", got2Done : " + got2Done.getCount() + ", sendDone : "
            + sendDone.getCount() + ", messages : " + receivedMessages;
    }

    public void initCombosForTestMessageListenerWithConsumerWithPrefetch1() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testMessageListenerWithConsumerWithPrefetch1() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 4) {
                    done.countDown();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);

        assertTrue(done.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestMessageListenerWithConsumer() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testMessageListenerWithConsumer() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch done = new CountDownLatch(1);

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message m) {
                counter.incrementAndGet();
                if (counter.get() == 4) {
                    done.countDown();
                }
            }
        });

        // Send the messages
        sendMessages(session, destination, 4);

        assertTrue(done.await(1000, TimeUnit.MILLISECONDS));
        Thread.sleep(200);

        // Make sure only 4 messages were delivered.
        assertEquals(4, counter.get());
    }

    public void initCombosForTestUnackedWithPrefetch1StayInQueue() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("ackMode", new Object[] {
                Integer.valueOf(Session.AUTO_ACKNOWLEDGE), Integer.valueOf(Session.DUPS_OK_ACKNOWLEDGE),
                Integer.valueOf(Session.CLIENT_ACKNOWLEDGE)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testUnackedWithPrefetch1StayInQueue() throws Exception {

        // Set prefetch to 1
        if (null == connection.getClientID()) connection.setClientID(getName() + "test-client-id-3");
        connection.start();

        // Use all the ack modes
        Session session = connection.createSession(false, ackMode);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id3");

        // Send the messages
        sendMessages(session, destination, 4);

        // Only pick up the first 2 messages.
        Message message = null;
        for (int i = 0; i < 2; i++) {
            message = consumer.receive(1000);
            assertNotNull(message);
            assert (message instanceof TextMessage);
            assert (((TextMessage) message).getText().equals(messageTextPrefix  + i))
                : "Received message " + ((TextMessage) message).getText() + " .. i = " + i;
        }
        assert null != message;
        message.acknowledge();

        connection.close();
        connection = (HedwigConnectionImpl)factory.createConnection();
        if (null == connection.getClientID()) connection.setClientID(getName() + "test-client-id-3");
        // Use all the ack modes
        session = connection.createSession(false, ackMode);
        consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id3");
        connections.add(connection);
        connection.start();

        // Pickup the rest of the messages.
        for (int i = 0; i < 2; i++) {
            message = consumer.receive(1000);
            assertNotNull(message);
            assert (message instanceof TextMessage);
            assert (((TextMessage) message).getText().equals(messageTextPrefix  + (i + 2))) :
            "Received message " + ((TextMessage) message).getText() + " .. i = " + i;
        }
        message.acknowledge();
        // assertNull(consumer.receiveNoWait());
        {
            Message msg = consumer.receiveNoWait();
            assert null == msg : "Unexpected message " + msg;
        }

    }

    public void initCombosForTestPrefetch1MessageNotDispatched() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testPrefetch1MessageNotDispatched() throws Exception {

        // Set prefetch to 1
        connection.start();

        Session session = connection.createSession(true, 0);
        destination = SessionImpl.asTopic("TEST");
        MessageConsumer consumer = session.createConsumer(destination);

        // The prefetch should fill up with 1 message.
        // Since prefetch is still full, the 2nd message should get dispatched
        // to another consumer.. lets create the 2nd consumer test that it does
        // make sure it does.
        HedwigConnectionImpl connection2 = (HedwigConnectionImpl)factory.createConnection();
        connection2.start();
        connections.add(connection2);
        Session session2 = connection2.createSession(true, 0);
        MessageConsumer consumer2 = session2.createConsumer(destination);

        // Send 2 messages to the destination.
        sendMessages(session, destination, 2);
        session.commit();

        // Pick up the first message.
        Message message1 = consumer.receive(1000);
        assertNotNull(message1);
        assertNotNull(consumer.receive(1000));

        // Pick up the 2nd messages.
        Message message2 = consumer2.receive(5000);
        assertNotNull(message2);
        assertNotNull(consumer2.receive(1000));

        session.commit();
        session2.commit();

        assertNull(consumer.receiveNoWait());

    }

    public void initCombosForTestDontStart() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT)});
        addCombinationValues("destinationType", new Object[] { MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testDontStart() throws Exception {

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 1);

        // Make sure no messages were delivered.
        assertNull(consumer.receive(1000));
    }

    public void initCombosForTestStartAfterSend() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testStartAfterSend() throws Exception {

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 1);

        // Start the conncection after the message was sent.
        connection.start();

        // Make sure only 1 message was delivered.
        assertNotNull(consumer.receive(1000));
        assertNull(consumer.receiveNoWait());
    }

    public void initCombosForTestReceiveMessageWithConsumer() {
        addCombinationValues("deliveryMode", new Object[] {
                Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testReceiveMessageWithConsumer() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer = session.createConsumer(destination);

        // Send the messages
        sendMessages(session, destination, 1);

        // Make sure only 1 message was delivered.
        Message m = consumer.receive(1000);
        assertNotNull(m);
        assertEquals("0", ((TextMessage)m).getText());
        assertNull(consumer.receiveNoWait());
    }


    public void testDupsOkConsumer() throws Exception {

        // Receive a message with the JMS API
        connection.start();
        Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        destination = createDestination(session, MessagingSessionFacade.DestinationType.TOPIC);
        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id4");

        // Send the messages
        sendMessages(session, destination, 4);

        // Make sure only 4 message are delivered.
        for( int i=0; i < 4; i++){
            Message m = consumer.receive(1000);
            assertNotNull(m);
        }
        assertNull(consumer.receive(1000));

        // Close out the consumer.. no other messages should be left on the queue.
        consumer.close();

        consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id4");
        assertNull(consumer.receive(1000));
    }

    public void testRedispatchOfUncommittedTx() throws Exception {

        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        destination = createDestination(session, MessagingSessionFacade.DestinationType.TOPIC);
        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id2");

        sendMessages(connection, destination, 2);

        assertNotNull(consumer.receive(1000));
        assertNotNull(consumer.receive(1000));

        // install another consumer while message dispatch is unacked/uncommitted

        // no commit so will auto rollback and get re-dispatched to redisptachConsumer
        session.close();

        Session redispatchSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer redispatchConsumer
            = redispatchSession.createDurableSubscriber((Topic) destination, "subscriber-id2");

        Message msg = redispatchConsumer.receive(1000);
        assertNotNull(msg);
        // assertTrue("redelivered flag set", msg.getJMSRedelivered());

        msg = redispatchConsumer.receive(1000);
        assertNotNull(msg);
        // assertTrue(msg.getJMSRedelivered());
        redispatchSession.commit();

        assertNull(redispatchConsumer.receive(500));
        redispatchSession.close();
    }


    public void testRedispatchOfRolledbackTx() throws Exception {

        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        destination = createDestination(session, MessagingSessionFacade.DestinationType.TOPIC);
        MessageConsumer consumer = session.createDurableSubscriber((Topic) destination, "subscriber-id1");

        sendMessages(connection, destination, 2);

        assertNotNull(consumer.receive(1000));
        assertNotNull(consumer.receive(1000));

        // install another consumer while message dispatch is unacked/uncommitted

        session.rollback();
        session.close();

        Session redispatchSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer redispatchConsumer
            = redispatchSession.createDurableSubscriber((Topic) destination, "subscriber-id1");

        Message msg = redispatchConsumer.receive(1000);
        assertNotNull(msg);
        // assertTrue(msg.getJMSRedelivered());
        msg = redispatchConsumer.receive(1000);
        assertNotNull(msg);
        // assertTrue(msg.getJMSRedelivered());
        redispatchSession.commit();

        assertNull(redispatchConsumer.receive(500));
        redispatchSession.close();
    }

    public void initCombosForTestAckOfExpired() {
        addCombinationValues("destinationType",
                new Object[] {MessagingSessionFacade.DestinationType.TOPIC});
    }

    public void testAckOfExpired() throws Exception {
        HedwigConnectionFactoryImpl fact = new HedwigConnectionFactoryImpl();
        connection = fact.createConnection();

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = (Destination) (destinationType == MessagingSessionFacade.DestinationType.QUEUE ?
                session.createTopic("test") : session.createTopic("test"));

        MessageConsumer consumer = session.createConsumer(destination);

        Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = sendSession.createProducer(destination);
        final int count = 4;


        // producer.setTimeToLive(0);
        for (int i = 0; i < count; i++) {
            TextMessage message = sendSession.createTextMessage("no expiry" + i);
            producer.send(message);
        }

        MessageConsumer amqConsumer = (MessageConsumer) consumer;

        for(int i=0; i<count; i++) {
            TextMessage msg = (TextMessage) amqConsumer.receive();
            assertNotNull(msg);
            assertTrue("message has \"no expiry\" text: " + msg.getText(), msg.getText().contains("no expiry"));

            // force an ack when there are expired messages
            msg.acknowledge();
        }
    }
}

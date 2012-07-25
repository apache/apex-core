/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.StreamConfiguration;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractActiveMQInputStream
  extends AbstractObjectInputStream
  implements MessageListener, ExceptionListener
{
  private static final Logger logger = Logger.getLogger(
    AbstractActiveMQInputStream.class.getName());
  private boolean transacted;
  private int maxiumMessages;
  private int receiveTimeOut;
  private MessageConsumer consumer;
  private Connection connection;
  private Session session;
  private MessageProducer replyProducer;

  public abstract Object getObject(Object object);

  private void internalSetup(StreamConfiguration config) throws Exception
  {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
      config.get("user"),
      config.get("password"),
      config.get("url"));

    connection = connectionFactory.createConnection();
    if (config.getBoolean("durable", false)) {
      String clientid = config.get("clientId");
      if (clientid != null && clientid.length() > 0 && !"null".equals(clientid)) {
        getConnection().setClientID(clientid);
      }
    }
    getConnection().setExceptionListener(this);
    getConnection().start();

    setAckMode(config.get("ackMode"));
    session = getConnection().createSession(config.getBoolean("transacted", false), this.ackMode);

    Destination destination;
    if (config.getBoolean("topic", false)) {
      destination = getSession().createTopic(config.get("subject"));
    }
    else {
      destination = getSession().createQueue(config.get("subject"));
    }

    replyProducer = getSession().createProducer(null);
    replyProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

    if (config.getBoolean("durable", false) && config.getBoolean("topic", false)) {
      consumer = getSession().createDurableSubscriber((Topic) destination, config.get("consumerName"));
    }
    else {
      consumer = getSession().createConsumer(destination);
    }

    maxiumMessages = config.getInt("maximumMessages", 0);
    receiveTimeOut = config.getInt("receiveTimeOut", 0);
    transacted = config.getBoolean("transacted", false);
  }

  public void setup(StreamConfiguration config)
  {
    try {
      internalSetup(config);
    }
    catch (Exception e) {
      logger.log(Level.SEVERE, "Exception while setting up ActiveMQ consumer.", e.getCause());
    }
  }
  private int ackMode = Session.AUTO_ACKNOWLEDGE;

  public void setAckMode(String ackMode)
  {
    if ("CLIENT_ACKNOWLEDGE".equals(ackMode)) {
      this.ackMode = Session.CLIENT_ACKNOWLEDGE;
    }
    if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
      this.ackMode = Session.AUTO_ACKNOWLEDGE;
    }
    if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
      this.ackMode = Session.DUPS_OK_ACKNOWLEDGE;
    }
    if ("SESSION_TRANSACTED".equals(ackMode)) {
      this.ackMode = Session.SESSION_TRANSACTED;
    }
  }

  public void activate()
  {
    try {
      getConsumer().setMessageListener(this);
    }
    catch (JMSException ex) {
      Logger.getLogger(AbstractActiveMQInputStream.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      replyProducer.close();
      getConsumer().close();
      getSession().close();
      getConnection().close();

      replyProducer = null;
      consumer = null;
      session = null;
      connection = null;
    }
    catch (JMSException ex) {
      Logger.getLogger(AbstractActiveMQInputStream.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void onException(JMSException jmse)
  {
    logger.log(Level.SEVERE, "Exception thrown by ActiveMQ consumer setup.", jmse.getCause());
  }

  public void onMessage(Message message)
  {
    /**
     * make sure that we do not get called again if we have processed enough
     * messages already.
     */
    if (maxiumMessages > 0) {
      if (--maxiumMessages == 0) {
        try {
          getConsumer().setMessageListener(null);
        }
        catch (JMSException ex) {
          Logger.getLogger(AbstractActiveMQInputStream.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }


    Object o = getObject(message);
    if (o != null) {
      sendTuple(o);
    }

    try {
      if (message.getJMSReplyTo() != null) {
        replyProducer.send(message.getJMSReplyTo(), session.createTextMessage("Reply: " + message.getJMSMessageID()));
      }
      if (transacted) {
        //if ((messagesReceived % batch) == 0) {
        //System.out.println("Commiting transaction for last " + batch + " messages; messages so far = " + messagesReceived);
        session.commit();
        //}
      }
      else if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
        // we can use window boundary to ack the message.
        //if ((messagesReceived % batch) == 0) {
        //System.out.println("Acknowledging last " + batch + " messages; messages so far = " + messagesReceived);
        message.acknowledge();
        //}
      }
    }
    catch (JMSException ex) {
      Logger.getLogger(AbstractActiveMQInputStream.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  /**
   * @return the maxiumMessages
   */
  public int getMaxiumMessages()
  {
    return maxiumMessages;
  }

  /**
   * @return the receiveTimeOut
   */
  public int getReceiveTimeOut()
  {
    return receiveTimeOut;
  }

  /**
   * @return the consumer
   */
  public MessageConsumer getConsumer()
  {
    return consumer;
  }

  /**
   * @return the session
   */
  public Session getSession()
  {
    return session;
  }

  /**
   * @return the connection
   */
  public Connection getConnection()
  {
    return connection;
  }
}

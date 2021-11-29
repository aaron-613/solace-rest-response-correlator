/*
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

package com.solace.aaron.rest;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RestCorrelator {

    private static final Logger logger = LogManager.getLogger();
    
    /** when a new message arrives, only put it into a queue for later processing */
    static class MessageReceiver implements XMLMessageListener {
        @Override
        public void onReceive(BytesXMLMessage msg) {
            String topic = msg.getDestination().getName();
            if (topic.startsWith("POST") || topic.startsWith("PUT")) {  // incoming request
                requests.put(msg.getCorrelationId(), msg);
                reqsReceived++;
            } else if (topic.matches("#P2P/v:.*?/_rest.*")) {  // response coming through
                String corrId = msg.getCorrelationId();
                BytesXMLMessage reqMsg = requests.get(corrId);
                if (reqMsg == null) {
                    logger.error("Received a response to a message I don't have a request for!");
                    logger.error(UsefulUtils.solaceMsgToJson(msg));
                    return;
                }
                // here is where I verify the response is valid or not
                SDTMap props = msg.getProperties();
                int returnCode = -1;
                try {
                    returnCode = props.getShort("JMS_Solace_HTTP_status_code");
                } catch (SDTException e) {
                    logger.error("Response message somehow doesn't have HTTP status code set!");
                    logger.error(UsefulUtils.solaceMsgToJson(msg));
                    return;
                }
                if (returnCode == 201) {  // success!
                    reqsSuccess++;
                    requests.remove(corrId);
                } else if (returnCode == 200) {  // not success, so need to publish notification
                    reqsFailed++;
                    logger.error("Failed request, 200 response receied");
                    logger.error(UsefulUtils.solaceMsgToJson(reqMsg));
                } else {  // some other error code??
                    
                }
            }
        }

        @Override
        public void onException(JCSMPException e) {
            // won't (shouldn't?) get this since Direct
            logger.warn("Consumer received exception: %s%n",e);
            // oh well!
        }
    }
    
    
	
	
	// MAIN CLASS ///////////////////////////////////////////////////////////////////////////////////////////////////
	
    static Map<String,BytesXMLMessage> requests = new HashMap<>();
	static ScheduledExecutorService threads = Executors.newScheduledThreadPool(5);
	static volatile int reqsReceived = 0;  // only 1 thread updating this... and only 1 reading it, so ok for non-Atomic
    static volatile int reqsSuccess = 0;
    static volatile int reqsFailed = 0;

	
	
    public static void main(String... args) throws JCSMPException, IOException {
        
        // Check command line arguments
        if (args.length != 1) {
            System.err.println("Usage: RestCorrelator <properties-file>");
            System.err.println();
            System.exit(-1);
        }
        Properties props = new Properties();
/*        try {
            InputStream is = new FileInputStream(args[0]);
            props.load(is);
            is.close();
        } catch (FileNotFoundException e) {
            System.err.println("Can't find your properties file: '"+args[0]+"'");
        }
*/
        props.setProperty("host","localhost");
        props.setProperty("vpn","gw");
        props.setProperty("username","asfd");
        logger.info("RestCorrelator initializing...");
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, props.getOrDefault("host","localhost"));     // host:port
        if (props.get("vpn") != null)
            properties.setProperty(JCSMPProperties.VPN_NAME, props.get("vpn")); // message-vpn
        if (props.get("username") != null)
            properties.setProperty(JCSMPProperties.USERNAME, props.get("username")); // client-username
        if (props.get("password") != null)
            properties.setProperty(JCSMPProperties.PASSWORD, props.get("password")); // client-password

        //properties.setProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS, true);
        properties.setProperty(JCSMPProperties.IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR, true);
        properties.setProperty(JCSMPProperties.IGNORE_DUPLICATE_SUBSCRIPTION_ERROR, true);
        JCSMPChannelProperties cp = new JCSMPChannelProperties();
        cp.setReconnectRetries(-1);

        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES,cp);
        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties, null, new SessionEventHandler() {
            
            @Override
            public void handleEvent(SessionEventArgs event) {
                System.err.println("Session event received! "+event.toString());
            }
        });
        session.setProperty(JCSMPProperties.CLIENT_NAME, "RESTCorr_"+session.getProperty(JCSMPProperties.CLIENT_NAME));
        System.err.println("Connecting...");
        session.connect();
        System.err.println("Connected.");
		
        props.setProperty("subscriptions", "POST/hello2,#P2P/v:*/_rest*/POST/hello2");
        Set<String> subs = new HashSet<>(Arrays.asList(props.getProperty("subscriptions").split(",")));
        if (subs.size() == 1 && subs.contains(">")) subs.add("#P2P/QUE/>");
        for (String sub : subs) {
            System.err.println("Adding subscription: "+sub);
            session.addSubscription(JCSMPFactory.onlyInstance().createTopic(sub));
        }
        final XMLMessageConsumer cons = session.getMessageConsumer(new MessageReceiver());
        cons.start();
        // nice little output printer while it's running...
        Runnable statsPrinter = new Runnable() {
            @Override
            public void run() {
                System.err.println("Total requests received:  "+reqsReceived);
                System.err.println("Total requests success:   "+reqsSuccess);
                System.err.println("Total requests failed:    "+reqsFailed);
                System.err.println();
            }
        };
        threads.scheduleAtFixedRate(statsPrinter,5,5,TimeUnit.SECONDS);
        
        System.err.println("Awaiting messages...");
		System.err.println("Press [ENTER] to quit program and dump results!");
    	try {  // block the main thread from exiting, wait until we're told to exit
    	    System.in.read();
            System.err.println("[ENTER] detected! Shutting down subscriptions.");
    		// remove the subscriptions to stop messages, but keep the consumer alive to finish receiving all messages
            for (String sub : subs) {
                System.err.println("About to remove subscription: "+sub);
                session.removeSubscription(JCSMPFactory.onlyInstance().createTopic(sub),false);
            }
            System.err.println("Stopping. Waiting 1 second...");
     		Thread.sleep(1000);
    		threads.shutdownNow();
        } catch (IOException e) {
            // who cares
		} catch (InterruptedException e) {
            // who cares
        } catch (RuntimeException e) {
            System.err.println("Error detected. Hard shutdown.");
            e.printStackTrace();
        }
        // Close consumer & session
        cons.close();
        session.closeSession();
        System.err.println("Disconnected.");

    }
}

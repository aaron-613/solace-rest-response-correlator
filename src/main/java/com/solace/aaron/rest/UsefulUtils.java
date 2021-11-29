package com.solace.aaron.rest;

import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;

public class UsefulUtils {

//    private static final JCSMPFactory f = JCSMPFactory.onlyInstance();
    

    @SuppressWarnings("deprecation")
    static JsonStructure solaceMsgToJson(BytesXMLMessage msg) {
        JsonObjectBuilder job = Json.createObjectBuilder();
        // topic or queue
        job.add("destination",msg.getDestination().getName());
        job.add("destinationType", msg.getDestination() instanceof Topic ? "Topic" : "Queue");

        // metadata / headers
        if (msg.getApplicationMessageId() != null) {
            job.add("applicationMessageId",msg.getApplicationMessageId());
        }
        if (msg.getApplicationMessageType() != null) job.add("applicationMessageType",msg.getApplicationMessageType());
        if (msg.getConsumerIdList() != null && !msg.getConsumerIdList().isEmpty()) {
            JsonArrayBuilder jab = Json.createArrayBuilder();
            for (Long l : msg.getConsumerIdList()) {
                jab.add(l);
            }
            job.add("consumerIdList", jab.build());
        }
        if (msg.getContentLength() > 0) job.add("contentLength", msg.getContentLength());
        if (msg.getCorrelationId() != null) job.add("correlationId",msg.getCorrelationId());
        job.add("cos", msg.getCos().toString());
        try {
            job.add("deliveryCount", msg.getDeliveryCount());
        } catch (UnsupportedOperationException e) {
            // ignore
        }
        job.add("deliveryMode", msg.getDeliveryMode().toString());
        if (msg.isDMQEligible()) job.add("dmqEligible", msg.isDMQEligible());
        if (msg.getExpiration() > 0) job.add("expiration", msg.getExpiration());
        if (msg.getHTTPContentEncoding() != null) job.add("httpContentEncoding", msg.getHTTPContentEncoding());
        if (msg.getHTTPContentType() != null) job.add("httpContentType", msg.getHTTPContentType());
        if (msg.getMessageId() != null) job.add("mesageId", msg.getMessageId());  // deprecated, but still dump it out
        job.add("priority",msg.getPriority());
        if (msg.getRedelivered()) job.add("redelivered", msg.getRedelivered());
        if (msg.getReplicationGroupMessageId() != null) job.add("replicationGroupMessageId", msg.getReplicationGroupMessageId().toString());
        if (msg.isReplyMessage()) job.add("replyMessage", msg.isReplyMessage());
        if (msg.getReplyTo() != null) job.add("replyTo",msg.getReplyTo().getName());
        if (msg.getSenderId() != null) job.add("senderId",msg.getSenderId());
        if (msg.getSenderTimestamp() != null) job.add("senderTimestamp",msg.getSenderTimestamp());
        if (msg.getSequenceNumber() != null) job.add("sequenceNumber",msg.getSequenceNumber());
        if (msg.getTimeToLive() > 0) job.add("timeToLive", msg.getTimeToLive());

        // properties
        if (msg.getProperties() != null) job.add("properties", sdtMapToJson(msg.getProperties()));

        // payload
        if (msg instanceof TextMessage) {
            job.add("messageClass", "TextMessage");
            // let's test to see if it's JSON..!?!?
            try {
                JsonReader reader = Json.createReader(new StringReader(((TextMessage)msg).getText()));
                job.add("payload", reader.read());
            } catch (RuntimeException e) {  // nope!
                job.add("payload", ((TextMessage)msg).getText());
            }
        } else if (msg instanceof BytesMessage) {
            job.add("messageClass", "BytesMessage");
            job.add("payload", new String(Base64.getEncoder().encode(msg.getAttachmentByteBuffer().array())));
        } else if (msg instanceof MapMessage) {
            job.add("messageClass", "MapMessage");
            job.add("payload", new String(Base64.getEncoder().encode(msg.getAttachmentByteBuffer().array())));
        } else if (msg instanceof StreamMessage) {
            job.add("messageClass", "StreamMessage");
            job.add("payload", new String(Base64.getEncoder().encode(msg.getAttachmentByteBuffer().array())));
        } else {
            job.add("messageClass", msg.getClass().getName());
            job.add("payload", new String(Base64.getEncoder().encode(msg.getAttachmentByteBuffer().array())));
        }
        return job.build();
    }
    
    static JsonStructure sdtMapToJson(SDTMap map) {
        JsonObjectBuilder job = Json.createObjectBuilder();
        try {
            for (String key : map.keySet()) {
                Object o = map.get(key);
                if (o instanceof String) {
                    job.add(key, (String)o);
                } else if (o instanceof SDTMap) {
                    job.add(key, sdtMapToJson((SDTMap)o));
                } else if (o instanceof SDTStream) {
                    job.add(key, sdtStreamToJson((SDTStream)o));
                } else if (o instanceof Double) {
                    job.add(key, (Double)o);
                } else if (o instanceof Float) {
                    job.add(key, (Float)o);
                } else if (o instanceof Integer) {
                    job.add(key, (Integer)o);
                } else if (o instanceof Long) {
                    job.add(key, (Long)o);
                } else if (o instanceof Boolean) {
                    job.add(key, (Boolean)o);
                } else if (o instanceof Short) {
                    job.add(key, (Short)o);
                } else if (o instanceof Byte) {
                    job.add(key, (Byte)o);
                } else if (o instanceof ByteArray) {
                    job.add(key, new String(Base64.getEncoder().encode(((ByteArray)o).asBytes())));
                } else if (o instanceof Character) {
                    job.add(key, (Character)o);
                } else if (o instanceof Destination) {
                    job.add(key, ((Destination)o).getName());
                } else {
                    System.err.println("Unhandled type "+o.getClass().getName()+"!!  "+key+", "+o);
                }
            }
            
        } catch (SDTException e) {
            e.printStackTrace();
        }
        return job.build();
    }

    static JsonStructure sdtStreamToJson(SDTStream stream) {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        try {
            while (stream.hasRemaining()) {
                Object o = stream.read();
                if (o instanceof String) {
                    jab.add((String)o);
                } else if (o instanceof SDTMap) {
                    jab.add(sdtMapToJson((SDTMap)o));
                } else if (o instanceof SDTStream) {
                    jab.add(sdtStreamToJson((SDTStream)o));
                } else if (o instanceof Double) {
                    jab.add((Double)o);
                } else if (o instanceof Float) {
                    jab.add((Float)o);
                } else if (o instanceof Integer) {
                    jab.add((Integer)o);
                } else if (o instanceof Long) {
                    jab.add((Long)o);
                } else if (o instanceof Boolean) {
                    jab.add((Boolean)o);
                } else if (o instanceof Short) {
                    jab.add((Short)o);
                } else if (o instanceof Byte) {
                    jab.add((Byte)o);
                } else if (o instanceof ByteArray) {
                    jab.add(new String(Base64.getEncoder().encode(((ByteArray)o).asBytes())));
                } else if (o instanceof Character) {
                    jab.add((Character)o);
                } else if (o instanceof Destination) {
                    jab.add(((Destination)o).getName());
                } else {
                    System.err.println("Unhandled type "+o.getClass().getName()+"!!");
                }
            }
            
        } catch (SDTException e) {
            e.printStackTrace();
        }
        return jab.build();
    }

    static String prettyPrint(JsonStructure json) {
        //Map<String, Boolean> config = buildConfig(JsonGenerator.PRETTY_PRINTING);
        Map<String, Object> config = new HashMap<>();
        config.put(JsonGenerator.PRETTY_PRINTING, true);
        JsonWriterFactory writerFactory = Json.createWriterFactory(config);
        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = writerFactory.createWriter(stringWriter);
        jsonWriter.write(json);
        jsonWriter.close();
        return stringWriter.toString();
    }
    
    
    
/*    static BytesXMLMessage formatResponseMessage(BytesXMLMessage msg) {
        if ("pretty".equals(rmo.getParam("format"))) {
            TextMessage outMsg = f.createMessage(TextMessage.class);
            JsonObjectBuilder job = Json.createObjectBuilder();
            job.add("message", UsefulUtils.solaceMsgToJson(msg));
            outMsg.setText(UsefulUtils.prettyPrint(job.build()) + "\n");
            return outMsg;
        } else if ("dump".equals(rmo.getParam("format"))) {
            TextMessage outMsg = f.createMessage(TextMessage.class);
            outMsg.setText(String.format("%-40s%s%n%n%s",  // 40 spaces, align left, pring msgId, then \n\n message
                    "RestQ msgId:",rmo.uuid, msg.dump()));  // already has \n at end of dump()
            return outMsg;
        } else {
            TextMessage outMsg = f.createMessage(TextMessage.class);
            JsonObjectBuilder job = Json.createObjectBuilder();
            job.add("msgId", rmo.uuid);
            job.add("message", UsefulUtils.solaceMsgToJson(msg));
            outMsg.setText(job.build().toString() + "\n");
            return outMsg;
        }
    }
*/

    static String getJcsmpExceptionMessage(Exception e) {
        if (e instanceof JCSMPErrorResponseException) {
            JCSMPErrorResponseException e2 = (JCSMPErrorResponseException)e;
            return e2.getResponsePhrase();
        } else if (e.getCause() != null && e.getCause() instanceof JCSMPErrorResponseException) {
            JCSMPErrorResponseException e2 = (JCSMPErrorResponseException)e.getCause();
            return e2.getResponsePhrase();
        } else {  // not sure
            return e.getMessage();
        }
    }
        

    
    
    
    
    private UsefulUtils() {
        throw new AssertionError("don't instantiate");
    }
}

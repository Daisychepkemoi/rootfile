package com.cellulant.c2bconsumer.service;

import com.cellulant.c2bconsumer.GenericC2BConsumerProperties;
import java.util.ArrayList;
import java.util.List;
import com.cellulant.c2bconsumer.logger.LogWriter;
import com.cellulant.c2bconsumer.requests.GenericC2BRequest;
import com.cellulant.c2bconsumer.requests.PostPaymentRequest;
import com.cellulant.c2bconsumer.responses.PostPaymentResponse;
import com.cellulant.c2bconsumer.requests.PostPaymentRequest.Credential;
import com.cellulant.c2bconsumer.requests.PostPaymentRequest.Packet;
import com.cellulant.c2bconsumer.requests.PostPaymentRequest.Payload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

@Service
@RequiredArgsConstructor
public class C2BConsumerService implements C2BConsumer {

    @Autowired
    RestTemplate restTemplate;
    private final GenericC2BConsumerProperties c2bProperties;
    @Autowired
    AmqpTemplate rabbitTemplate;

    LogWriter logger = new LogWriter();
    Gson gson = new Gson();

    @Override
    public synchronized void processC2BMessage(GenericC2BRequest c2bmessage) {
        try {
            logger.infoLog(c2bmessage, "Processing c2b message >>  " + gson.toJson(c2bmessage));
            
            Map<String, String> map = getClientDetails(c2bmessage.getPayerClientCode());
            
            map = (!map.isEmpty()) ? map : getClientDetails("DEFAULT");
            
            if (!map.isEmpty()) {
                if (!Strings.isNullOrEmpty(c2bmessage.getPayBill())) {
                    c2bmessage = validateServicePaybill(c2bmessage);
                }
                if (!Strings.isNullOrEmpty(c2bmessage.getServiceID())) {
                    
                    PostPaymentRequest postPaymentRequest = preparePostPaymentRequest(c2bmessage, map);
                    
                    initiatePostPayment(c2bmessage, postPaymentRequest, map.get("cpgURL"));
                    
                } else {
                    logger.errorLog(c2bmessage, "Could Not validate Service ID ");
                }
            }
        } catch (JsonProcessingException | ClassNotFoundException | SQLException ex) {
            logger.debugLog(c2bmessage, "Exception << processC2BMessage >> " + ex.getLocalizedMessage());
            requeueMessage(c2bmessage, c2bProperties.getC2bRetryQueue());
        }
    }

    public synchronized Map<String, String> getClientDetails(@NonNull String fileName) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            var clientDetails = c2bProperties.getPayerClientDetailsPath().createRelative(fileName + ".json").getInputStream();
            return mapper.readValue(clientDetails, new TypeReference<Map<String, String>>() {
            });
        } catch (IOException e) {
            return Map.of();
        }
    }

    public synchronized GenericC2BRequest validateServicePaybill(GenericC2BRequest c2bmessage) throws ClassNotFoundException, SQLException {
        logger.infoLog(c2bmessage, "Received a C2B request with Paybill, going to validate for paybillMapping ");
        Class.forName(c2bProperties.getDbDriverClassName()); Connection con = DriverManager.getConnection(c2bProperties.getDbUrl(), c2bProperties.getDbUsername(), c2bProperties.getDbPassword());
        String sqlQuery = "select m.serviceID from paybills p inner join paybillMappings m on p.paybillID = m.paybillID where p.paybill = ?";
        try ( PreparedStatement pstmt = con.prepareStatement(sqlQuery)) {
            pstmt.setString(1, c2bmessage.getPayBill()); logger.infoLog(c2bmessage, "Generated Sql Query >> " + pstmt.toString()); ResultSet resultSet = pstmt.executeQuery(); while (resultSet.next()) {c2bmessage.setServiceID(resultSet.getString(1));}
        } finally {con.close(); }
        return c2bmessage;
    }

    /**
     *
     * @param c2bmessage
     * @param map
     * @return PostPaymentRequest
     */
    public synchronized PostPaymentRequest preparePostPaymentRequest(GenericC2BRequest c2bmessage, Map<String, String> map) {
        PostPaymentRequest postPaymestRequest = new PostPaymentRequest();
        Payload payload = new Payload();
        Packet packet = new Packet();
        Credential credentials = new Credential();
        
        credentials.setUsername(map.get("username"));
        credentials.setPassword(map.get("password"));

        postPaymestRequest.setFunction(c2bProperties.getCpgFunction());

        packet.setAccountNumber(c2bmessage.getAccountNumber());
        packet.setAmount(c2bmessage.getAmount());
        packet.setCurrencyCode(c2bmessage.getCurrencyCode());
        packet.setDatePaymentReceived(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()));
        packet.setMSISDN(c2bmessage.getMsisdn());
        packet.setNarration(c2bmessage.getNarration());
        packet.setPayerTransactionID(c2bmessage.getPayerTransactionID());
        packet.setServiceID(c2bmessage.getServiceID());
        packet.setExtraData(c2bmessage.getExtraData());

        List<Packet> listIems = new ArrayList<>();
        listIems.add(packet);
        
        payload.setCredentials(credentials);
        payload.setPacket(listIems);
        
        postPaymestRequest.setPayload(payload);
        
        logger.infoLog(c2bmessage, "PostPayment Payload >>  " + gson.toJson(postPaymestRequest));
        
        return postPaymestRequest;
    }

    /**
     *
     * @param postPaymentRequest
     * @param c2bmessage
     * @param cpgURL
     * @return
     * @throws com.fasterxml.jackson.core.JsonProcessingException
     */
    public synchronized boolean initiatePostPayment(GenericC2BRequest c2bmessage, PostPaymentRequest postPaymentRequest, String cpgURL) throws JsonProcessingException {
        Boolean isChargeSuccess = true;
        ObjectMapper objectMapper = new ObjectMapper();
        
        logger.infoLog(c2bmessage, "PostPayment  URL >> " + cpgURL);
        
        restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
        
        HttpHeaders headers = new HttpHeaders();
        
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        HttpEntity<String> entity = new HttpEntity<>(gson.toJson(postPaymentRequest), headers);
        
        String response = restTemplate.exchange(cpgURL, HttpMethod.POST, entity, String.class).getBody();
        
        logger.infoLog(c2bmessage, "Raw response from PostPayment API >>  " + response);
        
        PostPaymentResponse postPaymentResponse = objectMapper.readValue(response, PostPaymentResponse.class);
        
        processPostPaymentResponse(postPaymentResponse, c2bmessage);

        return isChargeSuccess;
    }

    /**
     *
     * @param postPaymentResponse
     * @param c2bmessage
     */
    public synchronized void processPostPaymentResponse(PostPaymentResponse postPaymentResponse, GenericC2BRequest c2bmessage) {
        int authStatus = postPaymentResponse.getAuthStatus().getAuthStatusCode();
        
        String authStatusDescription = postPaymentResponse.getAuthStatus().getAuthStatusDescription();
        
        if (authStatus == c2bProperties.getCpgAuthStatusCode()) {
            
            logger.infoLog(c2bmessage, "CPG Authentication Successful, authStatus >> " + authStatus);
            
            int statusCode = postPaymentResponse.getResults().get(0).getStatusCode();
            String statusDescription = postPaymentResponse.getResults().get(0).getStatusDescription();
            
            if (statusCode == c2bProperties.getSuccessCPGStatusCode() || statusCode == c2bProperties.getDuplicateCPGStatusCode()) {
                logger.infoLog(c2bmessage, "Post Payment Successful, statusCode >> " + statusCode);
                logger.infoLog(c2bmessage, "Post Payment  statusDescription >> " + statusDescription);
            } else {
                logger.errorLog(c2bmessage, "CPG Payment ACK Failed, statusCode >> " + statusCode);
                logger.errorLog(c2bmessage, "CPG Payment ACK statusDescription >> " + statusDescription);
            }
        } else {
            logger.errorLog(c2bmessage, "CPG Authentication Failed, authStatus " + authStatus);
            logger.errorLog(c2bmessage, "CPG Authentication Message >> " + authStatusDescription);
        }
    }

    /**
     *
     * @param c2bmessage
     * @param queue
     * @return
     */
    public synchronized boolean requeueMessage(GenericC2BRequest c2bmessage, String queue) {
        try {
            int time = (c2bProperties.getTtl()) / 60000;
            
            logger.infoLog(c2bmessage, "Requeuing the request ");
            
            logger.errorLog(c2bmessage, "The transaction will be re-tried after " + time + " Minutes");
            
            rabbitTemplate.convertAndSend(queue, c2bmessage);
            
            return true;
        } catch (AmqpException e) {
            logger.errorLog(c2bmessage, "Error while trying to requeue the message >> " + e.getLocalizedMessage());return false;
        }
    }
}

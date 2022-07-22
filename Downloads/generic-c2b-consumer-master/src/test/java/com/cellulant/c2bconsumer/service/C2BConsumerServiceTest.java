package com.cellulant.c2bconsumer.service;

import com.cellulant.c2bconsumer.GenericC2BConsumerProperties;
import com.cellulant.c2bconsumer.requests.GenericC2BRequest;
import com.cellulant.c2bconsumer.requests.PostPaymentRequest;
import com.cellulant.c2bconsumer.responses.PostPaymentResponse;
import com.cellulant.c2bconsumer.responses.PostPaymentResponse.AuthStatus;
import com.cellulant.c2bconsumer.responses.PostPaymentResponse.Result;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Service;

/**
 *
 * @author Yenusu
 */
@SpringBootTest()
@Service
@RequiredArgsConstructor()
public class C2BConsumerServiceTest {

    @Autowired
    GenericC2BConsumerProperties c2bProperties;

    PostPaymentRequest postPaymentRequest;

    PostPaymentResponse postPaymentResponse;

    GenericC2BRequest c2bmessage;
    
    AuthStatus authStatus;
    
    ArrayList<Result> results;
    
    Result result;

    String transactionID;

    @Autowired
    private C2BConsumerService c2bConsumerService;

    @BeforeEach
    void init() {
        c2bmessage = new GenericC2BRequest();
        postPaymentRequest = new PostPaymentRequest();
        postPaymentResponse = new PostPaymentResponse();
        transactionID = "GEN097564";

        Map<String, String> extraData = new HashMap<>();
        extraData.put("customerNames", "Yenusu Kiryamwibo");
        extraData.put("callbackURL", "http://ca1-h5.staging.cellulant.africa:9000/stubs/GEN/GenericC2B/callback.php");

        c2bmessage.setPayBill(null);
        c2bmessage.setAmount("1000");
        c2bmessage.setMsisdn("256776656200");
        c2bmessage.setAccountNumber("12345");
        c2bmessage.setServiceID("2661");
        c2bmessage.setPayerTransactionID("GEN097564");
        c2bmessage.setCurrencyCode("RWF");
        c2bmessage.setPayerClientCode("DEFAULT");
        c2bmessage.setNarration("C2B Request for 256776656200");
        c2bmessage.setExtraData(extraData);
        
        
        authStatus = new AuthStatus();
        results = new ArrayList<>();
        result = new Result();
        authStatus.setAuthStatusCode(139);
        authStatus.setAuthStatusDescription("transaction Pending");
        
        result.setBeepTransactionID("12345678");
        result.setPayerTransactionID("GEN097564");
        result.setStatusCode(140);
        result.setStatusDescription("successful Transaction");
        results.add(result);
        postPaymentResponse.setAuthStatus(authStatus);
        postPaymentResponse.setResults(results);

    }

    @Test
    void testProcessC2BMessage() {
        c2bConsumerService.processC2BMessage(c2bmessage);
        assertNotNull(c2bmessage);
    }
    @Test
    void testProcessC2BErrorMessage() {
        c2bmessage.setPayerClientCode("DEFAULT");
        c2bConsumerService.processC2BMessage(c2bmessage);
        assertNotNull(c2bmessage);
    }

    /*@Test
    void testProcessC2BMessageWithPaybill() {
        c2bmessage.setPayBill("A123");
        c2bConsumerService.processC2BMessage(c2bmessage);
        assertNotNull(c2bmessage);
    }*/
    
    @Test
    void testProcessC2BMessageWithInvalidPaybill() {
        c2bmessage.setServiceID("");
        c2bConsumerService.processC2BMessage(c2bmessage);
        assertNotNull(c2bmessage);
    }

    @Test
    void testPreparePostPaymentRequest() {
        Map<String, String> map = c2bConsumerService.getClientDetails("DEFAULT");
        postPaymentRequest = c2bConsumerService.preparePostPaymentRequest(c2bmessage, map);
        assertNotNull(postPaymentRequest);
    }

    @Test
    void testGetClientDetails() {
        Map<String, String> map = c2bConsumerService.getClientDetails("DEFAULT");
        assertEquals("mvend_merchant_api_user", map.get("username"));
    }

    /*@Test
    void testValidateServicePaybill() {
        try {
            c2bmessage.setPayBill("A123");
            GenericC2BRequest msg = c2bConsumerService.validateServicePaybill(c2bmessage);
            assertEquals("2661", msg.getServiceID());
        } catch (ClassNotFoundException | SQLException ex) {
            assertNotNull(ex.getLocalizedMessage());
        }
    }*/

    @Test
    void testInitiatePostPayment() {
        try {
            Map<String, String> map = c2bConsumerService.getClientDetails("DEFAULT");
            postPaymentRequest = c2bConsumerService.preparePostPaymentRequest(c2bmessage, map);
            boolean postResponse = c2bConsumerService.initiatePostPayment(c2bmessage, postPaymentRequest, map.get("cpgURL"));
            assertTrue(postResponse);
        } catch (JsonProcessingException ex) {
            assertNotNull(ex.getLocalizedMessage());
        }
    }
    
    @Test
    void testInitiateFailedPostPayment() {
        try {
            c2bmessage.setServiceID("54634");
            Map<String, String> map = c2bConsumerService.getClientDetails("DEFAULT");
            postPaymentRequest = c2bConsumerService.preparePostPaymentRequest(c2bmessage, map);
            boolean postResponse = c2bConsumerService.initiatePostPayment(c2bmessage, postPaymentRequest, map.get("cpgURL"));
            assertTrue(postResponse);
        } catch (JsonProcessingException ex) {
            assertNotNull(ex.getLocalizedMessage());
        }
    }

    @Test
    void testProcessPostPaymentResponse() throws JsonProcessingException { 
        c2bConsumerService.processPostPaymentResponse(postPaymentResponse, c2bmessage);
        assertNotNull(postPaymentResponse);
    }
    
    @Test
    void testFailedProcessPostPaymentResponse() throws JsonProcessingException { 
        authStatus.setAuthStatusCode(179);
        c2bConsumerService.processPostPaymentResponse(postPaymentResponse, c2bmessage);
        assertNotNull(postPaymentResponse);
    }
    
    @Test
    void testRejectedProcessPostPaymentResponse() throws JsonProcessingException { 
        result.setStatusCode(152);
        c2bConsumerService.processPostPaymentResponse(postPaymentResponse, c2bmessage);
        assertNotNull(postPaymentResponse);
    }
    
    @Test
    public void testRequeueMessage() {
        try {
            boolean requeueMessage = c2bConsumerService.requeueMessage(c2bmessage, c2bProperties.getC2bRetryQueue());
            assertNotNull(requeueMessage);
        } catch (NullPointerException ex) {
            assertNotNull(ex.getLocalizedMessage());
        }
    }

}

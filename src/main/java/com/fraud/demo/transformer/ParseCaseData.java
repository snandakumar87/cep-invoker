package com.fraud.demo.transformer;

import com.google.gson.Gson;
import fraud.analysis.demo.transaction.CEPFraud;
import fraud.analysis.demo.transaction.CurrentTxn;
import fraud.analysis.demo.transaction.Transaction;
import fraud.analysis.demo.transaction.fraud.analysis.Serde.RuleModel;
import iso.std.iso._20022.tech.xsd.pacs_008_001.CreditTransferTransaction25;
import iso.std.iso._20022.tech.xsd.pacs_008_001.FIToFICustomerCreditTransferV06;
import iso.std.iso._20022.tech.xsd.pacs_008_001.GroupHeader70;
import org.apache.camel.Body;
import org.dom4j.rule.Rule;
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.runtime.*;
import org.kie.api.runtime.rule.EntryPoint;
import rtp.demo.creditor.domain.payments.Payment;

import javax.swing.plaf.nimbus.State;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class ParseCaseData {

    static KieContainerSessionsPool pool = null;

    ParseCaseData() {

        txns = new ArrayList<>();
        loadRef();
        KieServices kieServices = KieServices.Factory.get();
        KieBaseConfiguration config = kieServices.newKieBaseConfiguration();
        config.setOption(EventProcessingOption.STREAM);
        KieContainer kieContainer = kieServices.getKieClasspathContainer();
        pool = kieContainer.newKieSessionsPool(10);
    }
    public static String process(@Body String body) {


        Transaction transaction = new Gson().fromJson(body,Transaction.class);

        KieSession kieSession = pool.newKieSession();
        System.out.println("new session"+new Date());
        EntryPoint entryPoint = kieSession.getEntryPoint("Reference");
        long start = new Date().getTime();
        kieSession = loadHundredTxns(kieSession,entryPoint);

        long cnt = invokeCEPRule(kieSession,transaction);
        kieSession.dispose();
        System.out.println(cnt+"rule count");
        if(cnt != 0) {
            Long ruleDetectionTime = new Date().getTime() - start;
            RuleModel ruleModel = new RuleModel();
            ruleModel.setFraudReason("No of rules matching input set" + cnt);
            ruleModel.setFraudIdentified(ruleDetectionTime);
            ruleModel.setTimeToStaticFraudDetection(entryPoint.getFactCount());
            transaction.setRuleModel(ruleModel);



            return new Gson().toJson(transaction);
        }else {
            return null;
        }
    }

    public static Long invokeCEPRule(KieSession kieSession, Transaction transaction) {


        CurrentTxn currentTxn = new CurrentTxn();
        currentTxn.setCardNumber(transaction.getCardNumber());
        currentTxn.setMerchId(transaction.getMerchId());
        currentTxn.setMcc(transaction.getMcc());
        currentTxn.setPos(transaction.getPos());
        currentTxn.setTxnAmt(transaction.getTxnAmt());
        currentTxn.setTxnCntry(transaction.getTxnCntry());
        currentTxn.setTxnType(transaction.getTxnType());
        currentTxn.setTxnTS(new Date().getTime());
        currentTxn.setTransactionId(transaction.getTransactionId());

        Long stDt = new Date().getTime();

        kieSession.insert(currentTxn);
        kieSession.fireAllRules();

        System.out.println("time diff"+(new Date().getTime() - stDt));



        Collection<?> fraudResponse = kieSession.getObjects(new ClassObjectFilter(CEPFraud.class));

        long cnt = fraudResponse.stream().filter(x-> {
                    CEPFraud fraud = (CEPFraud) x;
                    fraud.getTransactionId().equals(transaction.getTransactionId());
                    return true;
                }
        ).count();





        return cnt;
    }

    static List<Transaction> txns = new ArrayList<>();

    Transaction transaction;

    public static KieSession loadHundredTxns(KieSession kieSession, EntryPoint ep) {

        for(Transaction txn:txns) {
            ep.insert(txn);
        }
        return kieSession;
    }

    public void loadRef() {

        long startDate = new Date().getTime();
        for (int i = 0; i <= 1000; i++) {
            transaction = new Transaction();
            transaction.setTransactionId("TXN" + i);
            transaction.setTxnTS(startDate + 6000);
            transaction.setCardNumber("2345796540876432");
            transaction.setTxnType("ATM");
            transaction.setMcc("ATM");
            transaction.setPos("9100");
            transaction.setMerchId("MERCH1");
            transaction.setTxnCntry("US");
            transaction.setTxnAmt(1000.0);
            transaction.setDataWeight1("dataWeight");
            transaction.setDataWeight2("dataWeight");
            transaction.setDataWeight3("dataWeight1");
            transaction.setDataWeight4("dataWeight1");
            transaction.setDataWeight1("dataWeight1");
            transaction.setDataWeight2("dataWeight1");
            transaction.setDataWeight3("dataWeight1");
            transaction.setDataWeight4("dataWeight1");
            transaction.setDataWeight5("dataWeight1");
            transaction.setDataWeight6("dataWeight1");
            transaction.setDataWeight7("dataWeight1");
            transaction.setDataWeight8("dataWeight1");
            transaction.setDataWeight9("dataWeight1");
            transaction.setDataWeight10("dataWeight1");
            transaction.setDataWeight11("dataWeight1");
            transaction.setDataWeight12("dataWeight1");
            transaction.setDataWeight13("dataWeight1");
            transaction.setDataWeight14("dataWeight1");
            transaction.setDataWeight15("dataWeight1");
            transaction.setDataWeight16("dataWeight1");
            transaction.setDataWeight17("dataWeight1");
            transaction.setDataWeight18("dataWeight1");
            transaction.setDataWeight19("dataWeight1");
            transaction.setDataWeight20("dataWeight1");
            transaction.setDataWeight21("dataWeight1");
            transaction.setDataWeight22("dataWeight1");
            transaction.setDataWeight23("dataWeight1");
            transaction.setDataWeight24("dataWeight1");
            transaction.setDataWeight25("dataWeight1");
            transaction.setDataWeight26("dataWeight1");
            transaction.setDataWeight27("dataWeight1");
            transaction.setDataWeight28("dataWeight1");
            transaction.setDataWeight29("dataWeight1");
            transaction.setDataWeight30("dataWeight1");
            transaction.setDataWeight31("dataWeight1");
            transaction.setDataWeight32("dataWeight1");
            transaction.setDataWeight33("dataWeight1");
            transaction.setDataWeight34("dataWeight1");
            transaction.setDataWeight35("dataWeight1");
            transaction.setDataWeight36("dataWeight1");
            transaction.setDataWeight37("dataWeight1");
            transaction.setDataWeight38("dataWeight1");
            transaction.setDataWeight39("dataWeight1");
            transaction.setDataWeight40("dataWeight1");
            transaction.setDataWeight41("dataWeight1");
            transaction.setDataWeight42("dataWeight1");
            transaction.setDataWeight43("dataWeight1");
            transaction.setDataWeight44("dataWeight1");
            transaction.setDataWeight45("dataWeight1");
            transaction.setDataWeight46("dataWeight1");
            transaction.setDataWeight47("dataWeight1");

            txns.add(transaction);


        }

    }


}

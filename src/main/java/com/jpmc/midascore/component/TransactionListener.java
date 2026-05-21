package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private static final String INCENTIVE_URL = "http://localhost:8080/incentive";

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public TransactionListener(DatabaseConduit databaseConduit, RestTemplateBuilder builder) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = builder.build();
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {
        UserRecord sender = databaseConduit.findUser(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUser(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = restTemplate.postForObject(INCENTIVE_URL, transaction, Incentive.class);
        float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0f;

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        databaseConduit.saveTransaction(new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount));

        if ("wilbur".equals(sender.getName())) {
            logger.info("WILBUR BALANCE: {}", sender.getBalance());
        }
        if ("wilbur".equals(recipient.getName())) {
            logger.info("WILBUR BALANCE: {}", recipient.getBalance());
        }
    }
}

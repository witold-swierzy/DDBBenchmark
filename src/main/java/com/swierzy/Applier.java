package com.swierzy;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.net.URI;
import java.util.Map;

public class Applier extends Thread {

    private String endPoint;
    private int numOfOperations;
    private int threadId;
    private String tableName;
    DynamoDbClient client;
    private long startTime    = 0;
    private long endTime      = 0;
    private long insStartTime = 0;
    private long insEndTime   = 0;
    private long updStartTime = 0;
    private long updEndTime   = 0;
    private long selStartTime = 0;
    private long selEndTime   = 0;
    private long delStartTime = 0;
    private long delEndTime   = 0;
    private boolean execInserts,execUpdates,execSelects,execDeletes;

    public Applier(int threadId,
                   String tableName,
                   String endPoint,
                   int numOfOperations,
                   boolean execInserts,
                   boolean execUpdates,
                   boolean execSelects,
                   boolean execDeletes) {
        this.threadId  = threadId;
        this.tableName = tableName;
        this.endPoint  = endPoint;
        this.numOfOperations = numOfOperations;
        this.execInserts = execInserts;
        this.execUpdates = execUpdates;
        this.execSelects = execSelects;
        this.execDeletes = execDeletes;
        client = DynamoDbClient.builder()
                .region(Region.AF_SOUTH_1)
                .endpointOverride(URI.create(endPoint))
                .build();
    }

    public void run () {

        int start = numOfOperations*threadId + 1;
        int end   = numOfOperations*threadId + numOfOperations + 1;
        startTime = System.currentTimeMillis();
        if (execInserts) {
            insStartTime = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                client.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(Map.of(
                                "PK", AttributeValue.builder().s(String.valueOf(i)).build(),
                                "SK", AttributeValue.builder().s("NAME").build(),
                                "StringValue", AttributeValue.builder().s("OldValue" + i).build()))
                        .build());
            }
            insEndTime = System.currentTimeMillis();
        }
        if (execUpdates) {
            updStartTime = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                client.updateItem(UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of(
                                "PK", AttributeValue.builder().s(String.valueOf(i)).build(),
                                "SK", AttributeValue.builder().s("NAME").build()))
                        .updateExpression("SET #T = :t")
                        .expressionAttributeNames(Map.of("#T", "StringValue"))
                        .expressionAttributeValues(Map.of(":t", AttributeValue.builder().s("NewValue" + i).build()))
                        .build()
                );
            }
            updEndTime = System.currentTimeMillis();
        }
        if (execSelects) {
            selStartTime = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                client.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of(
                                "PK", AttributeValue.builder().s(String.valueOf(i)).build(),
                                "SK", AttributeValue.builder().s("NAME").build()))
                        .build()
                );
            }
            selEndTime = System.currentTimeMillis();
        }
        if (execDeletes) {
            delStartTime = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                client.deleteItem(DeleteItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of(
                                "PK", AttributeValue.builder().s(String.valueOf(i)).build(),
                                "SK", AttributeValue.builder().s("NAME").build()))
                        .build()
                );
            }
            delEndTime = System.currentTimeMillis();
        }
        endTime    = System.currentTimeMillis();
    }

    public long getExecutionTime() {
        return endTime - startTime;
    }

    public long getInsertTime() {
        return insEndTime - insStartTime;
    }

    public long getUpdateTime() {
        return updEndTime - updStartTime;
    }

    public long getSelectTime() {
        return selEndTime - selStartTime;
    }

    public long getDeleteTime() {
        return delEndTime - delStartTime;
    }

}


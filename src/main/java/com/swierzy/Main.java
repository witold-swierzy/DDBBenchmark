package com.swierzy;


import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/*import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.model.Bucket;*/

public class Main {
    static long startTime    = 0;
    static long endTime      = 0;
    static long expStartTime = 0;
    static long expEndTime   = 0;
    static String tableStatus = "";
    static boolean execInserts = Boolean.valueOf(System.getenv("BENCH_INSERTS"));
    static boolean execUpdates = Boolean.valueOf(System.getenv("BENCH_UPDATES"));
    static boolean execSelects = Boolean.valueOf(System.getenv("BENCH_SELECTS"));
    static boolean execDeletes = Boolean.valueOf(System.getenv("BENCH_DELETES"));
    static boolean execExport  = Boolean.valueOf(System.getenv("BENCH_EXPORT"));
    static String tableArn = "";
    static String ociConfigFileName = System.getenv("BENCH_CONFIG");
    static String ociRegionName     = System.getenv("BENCH_REGION");
    static String ociCompartmentId  = System.getenv("BENCH_COMPARTMENT_ID");
    //static ConfigFileReader.ConfigFile configFile;

    public static void main(String[] args) {
        String tableName    = System.getenv("BENCH_TABLE");
        String endPoint     = System.getenv("BENCH_URI");
        int threadPoolSize  = Integer.parseInt(System.getenv("BENCH_THREADS"));
        int numOfOperations = Integer.parseInt(System.getenv("BENCH_OPS"));
        long capacity = Long.valueOf(System.getenv("BENCH_CAP"));
        Applier[] appliers = new Applier[threadPoolSize];

        try {
            System.out.println("DDB Benchmark initialization");
            DynamoDbClient client = DynamoDbClient.builder()
                    .region(Region.AF_SOUTH_1)
                    .endpointOverride(URI.create(endPoint))
                    .build();
            ListTablesRequest listRequest = ListTablesRequest.builder().build();
            ListTablesResponse listResponse = client.listTables(listRequest);

            if (!listResponse.tableNames().contains(tableName)) {
                CreateTableRequest createTableRequest = CreateTableRequest.builder()
                        .tableName(tableName)
                        .keySchema(
                                KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build()
                        )
                        .attributeDefinitions(
                                AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName("SK").attributeType(ScalarAttributeType.S).build()
                        )
                        .provisionedThroughput(
                                ProvisionedThroughput.builder().readCapacityUnits(capacity).writeCapacityUnits(capacity).build()
                        )
                        .build();
                client.createTable(createTableRequest);
            }

            System.out.println("Checking table status for table : "+tableName);
            while (!tableStatus.equals("ACTIVE")) {
                DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                        .tableName(tableName)
                        .build();
                TableDescription tableDesc = client.describeTable(tableRequest).table();
                tableStatus = tableDesc.tableStatusAsString();
                System.out.println(tableName+" status : "+tableStatus);
                if (!tableStatus.equals("ACTIVE"))
                    Thread.sleep(1000);
                else
                    tableArn = tableDesc.tableArn();
            }


            for (int i = 0; i < appliers.length; i++)
                appliers[i] = new Applier(i,
                        tableName,
                        endPoint,
                        numOfOperations,
                        execInserts,
                        execUpdates,
                        execSelects,
                        execDeletes);
            System.out.println("DDB Benchmark initialized successfully.");
            System.out.println("Starting Threads");
            startTime = System.currentTimeMillis();
            for (int i = 0; i < appliers.length; i++)
                appliers[i].start();

            for (int i = 0; i < appliers.length; i++)
                appliers[i].join();

            if (execExport) {
                expStartTime = System.currentTimeMillis();

                client.exportTableToPointInTime(b -> b
                        .tableArn(tableArn)
                        .exportTime(Instant.parse("2023-09-20T12:00:00Z"))
                        .s3Bucket("dynamo")
                        .s3Prefix("prefix")
                        .clientToken("abrakadabra")
                        .exportFormat(ExportFormat.DYNAMODB_JSON));
                expEndTime   = System.currentTimeMillis();
            }
            client.close();
            endTime = System.currentTimeMillis();
            System.out.println("DDB Test completed.");
            System.out.println("Table Name                      : "+tableName);
            System.out.println("Capacity                        : "+capacity);
            System.out.println("Number of threads               : "+threadPoolSize);
            System.out.println("Number of CRUD ops sets         : "+numOfOperations);
            System.out.println("Total execution time (ms)       : "+(endTime-startTime));
            System.out.println("Execution times of threads (ms) : ");
            System.out.println("Export time (ms)                : "+(expEndTime));
            for (int i=0; i<threadPoolSize;i++)
                System.out.println("Thread #"+i+", total : "+appliers[i].getExecutionTime() +
                        " ,Insert : "+appliers[i].getInsertTime() +
                        " ,Update : "+appliers[i].getUpdateTime() +
                        " ,Select : "+appliers[i].getSelectTime() +
                        " ,Delete : "+appliers[i].getDeleteTime() );

        }
        catch (Exception e) { e.printStackTrace(); };
    }
}

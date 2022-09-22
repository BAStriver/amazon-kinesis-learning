/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import lombok.val;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisBaseClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.kinesis.common.KinesisClientUtil;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Uses the Kinesis Client Library (KCL) 2.2.9 to continuously consume and process stock trade
 * records from the stock trades stream. KCL monitors the number of shards and creates
 * record processor instances to read and process records from each shard. KCL also
 * load balances shards across all the instances of this processor.
 *
 */
public class StockTradesProcessor {

    private static final Log LOG = LogFactory.getLog(StockTradesProcessor.class);

    private static final Logger ROOT_LOGGER = Logger.getLogger("");
    private static final Logger PROCESSOR_LOGGER =
            Logger.getLogger("com.amazonaws.services.kinesis.samples.stocktrades.processor.StockTradeRecordProcessor");

    private static void checkUsage(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: " + StockTradesProcessor.class.getSimpleName()
                    + " <application name> <stream name> <region>");
            System.exit(1);
        }
    }

    /**
     * Sets the global log level to WARNING and the log level for this package to INFO,
     * so that we only see INFO messages for this processor. This is just for the purpose
     * of this tutorial, and should not be considered as best practice.
     *
     */
    private static void setLogLevels() {
        ROOT_LOGGER.setLevel(Level.WARNING);
        // Set this to INFO for logging at INFO level. Suppressed for this example as it can be noisy.
        PROCESSOR_LOGGER.setLevel(Level.WARNING);
    }

    public static void main(String[] args) throws Exception {

        // aws sts assume-role --role-arn arn:aws:iam::962862317094:role/DeveloperRole --role-session-name bas-developer --profile developer
        // aws kinesis list-streams --profile developer --region us-west-1
        setLogLevels();

        String streamName = "StockTradeStream";
        Region region = Region.of("us-west-1");

//        System.setProperty("aws.accessKeyId", "ASIA61LYQ7YTFYX47H4H");
//        System.setProperty("aws.secretKey", "IxLe7U5K1JVM71TpLWsxBeuGMEWZ9Z7GAmDobRX3");
        AwsSessionCredentials awsSessionCredentials = AwsSessionCredentials.create("ASIA61LYQAW5FYX37H4H", "IxLe7U1K5JVM7456GWsxBeuGMEWZ9Z7GAmDobRX3",
                "FwoGZXIvYXdzEMn//////////wEaDGuJQC41JRTaTsaM7CKxAS72n7pfajAUFm8tdQhRwyr3/m2LYNmRQh88Yl+g4Y+YLunVBx6gnBreKOD9q2sKD3r6MRvOw6jD3FSZyznwpBVTYGGwzP/Lau3SRadeioXW7wEVAPGyNLd8fUZYuOzSqPc4j4ddUDrvu1so6D2ftDzgTAY2rfqtfEXALqmxbWu2Why1fHS7TOxYMjSoM54Qu4XqCa+MYA/M0FbT4JNJVbmYYxpq2xNkcOrJ4kvG9gDSnCiYuKeZBjItxIm6Rn3mUaxJR1MA7BHuLdV4F6V2F/lfB6oAaxM5j3p82sLMdB8n4MjcaPm6");

        DefaultCredentialsProvider defaultCredentialsProvider = DefaultCredentialsProvider.builder().profileName("developer").build();

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder().region(region)
                        .credentialsProvider(defaultCredentialsProvider)
                /*.credentialsProvider(StaticCredentialsProvider.create(awsSessionCredentials))*/);

        CompletableFuture<ListStreamsResponse> listStreamsResponseCompletableFuture = kinesisClient.listStreams();
        System.out.println(listStreamsResponseCompletableFuture.get());

        PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(UUID.randomUUID().toString()) // We use the ticker symbol as the partition key, explained in the Supplemental Information section below.
                .streamName(streamName)
                .data(SdkBytes.fromByteArray("BAS".getBytes()))
                .build();
        CompletableFuture<PutRecordResponse> result = kinesisClient.putRecord(request);
        System.out.println(result.get());
    }

}

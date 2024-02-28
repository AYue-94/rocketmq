package org.apache.rocketmq.tieredstore.provider.s3;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class TieredStorageS3Client {
    private static final TieredMessageStoreConfig tieredMessageStoreConfig = new TieredMessageStoreConfig();
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AwsBasicCredentials basicCredentials = AwsBasicCredentials.create("aaa", "bbb");
        S3AsyncClient client = S3AsyncClient.builder()
            .credentialsProvider(() -> basicCredentials)
            .region(Region.of("default"))
            .build();
        GetObjectRequest request = GetObjectRequest.builder().bucket("bucket").key("aaa").range("bytes=" + 0 + "-" + 222).build();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        client.getObject(request, AsyncResponseTransformer.toBytes()).whenComplete((response, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(response.asByteArray());
            }
        });

        future.get();
    }
}

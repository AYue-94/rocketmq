package org.apache.rocketmq.tieredstore.provider.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.AppendObjectRequest;
import com.aliyun.oss.model.AppendObjectResult;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.provider.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class AliyunFileSegment extends TieredFileSegment {

    private static final Logger LOGGER = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final Client client;

    private final String fullPath;

    private volatile Boolean created;

    public AliyunFileSegment(TieredMessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {
        super(storeConfig, fileType, filePath, baseOffset);
        client = Client.getInstance(storeConfig);
        String dir = "rmq_data/" + storeConfig.getBrokerClusterName() + "/" + filePath;
        String name = fileType.toString() + "_" + TieredStoreUtil.offset2FileName(baseOffset);
        // rmq_data/cluster/broker/topic/queue/file_type_offset
        this.fullPath = dir + "/" + name;
    }

    @Override
    public String getPath() {
        return this.fullPath;
    }

    @Override
    public long getSize() {
        try {
            ObjectMetadata metadata = client.getMetadata(this.fullPath);
            return metadata.getContentLength();
        } catch (Exception e) {
            LOGGER.info("[aliyun] getSize error : {}", e.getMessage());
            return 0;
        }
    }

    @Override
    public boolean exists() {
//        return client.exist(this.fullPath);
        return true;
    }

    @Override
    public void createFile() {
//        if (created == null) {
//            synchronized (this) {
//                if (created == null) {
//                    if (exists()) {
//                        created = true;
//                        return;
//                    }
//                    client.create(this.fullPath);
//                    created = true;
//                }
//            }
//        }
    }

    @Override
    public void destroyFile() {
        try {
            client.delete(this.fullPath);
        } catch (Exception e) {

        }
    }

    @Override
    public CompletableFuture<ByteBuffer> read0(long position, int length) {
        return client.getAsync(this.fullPath, position, length);
    }

    @Override
    public CompletableFuture<Boolean> commit0(FileSegmentInputStream inputStream, long position, int length,
        boolean append) {
        return client.appendAsync(fullPath, position, inputStream).thenApply(none -> true);
    }

    private static class Client {

        private static final Logger LOGGER = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

        private static volatile Client client = null;

        private final OSS oss;

        private final String bucket;

        private final ExecutorService asyncExecutor = Executors.newFixedThreadPool(8);

        private Client(TieredMessageStoreConfig config) {
            this.oss = new OSSClientBuilder()
                .build(config.getObjectStoreEndpoint(), config.getObjectStoreAccessKey(), config.getObjectStoreSecretKey());
            this.bucket = config.getObjectStoreBucket();
        }

        private static Client getInstance(TieredMessageStoreConfig config) {
            if (client == null) {
                synchronized (Client.class) {
                    if (client == null) {
                        client = new Client(config);
                    }
                }
            }
            return client;
        }

        public boolean exist(String path) {
            LOGGER.info("[aliyun] exist path = {}", path);
            try {
                ObjectMetadata metadata = oss.headObject(bucket, path);
                LOGGER.info("[aliyun] exist path = {} metadata = {}", path, metadata);
                return metadata != null;
            } catch (OSSException ossException) {
                return false;
            }
        }

        private void append(String path, long position, InputStream inputStream) throws IOException {
            LOGGER.info("[aliyun] append path = {} position = {}", path, position);
            AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucket, path, inputStream);
            appendObjectRequest.setPosition(position);
            AppendObjectResult result = oss.appendObject(appendObjectRequest);
            LOGGER.info("[aliyun] append path = {} position = {} next_position = {}", path, position, result.getNextPosition());
        }

        public void delete(String path) {
            oss.deleteObject(bucket, path);
        }

        public void create(String path) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
            try {
                append(path, 0, inputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    inputStream.close();
                } catch (IOException e) {

                }
            }
        }

        public CompletableFuture<Void> appendAsync(String path, long position, InputStream stream) {
//            CompletableFuture<Void> future = new CompletableFuture<>();
//            try {
//                this.append(path, position, stream);
//                future.complete(null);
//            } catch (IOException e) {
//                future.completeExceptionally(e);
//            }
//            return future;
            return CompletableFuture.runAsync(() -> {
                try {
                    this.append(path, position, stream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, this.asyncExecutor);
        }

        public CompletableFuture<ByteBuffer> getAsync(String path, long position, int length) {
//            CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
//
//            LOGGER.info("[aliyun] get path = {} position = {} length = {}", path, position, length);
//            GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, path);
//            getObjectRequest.setRange(position, position + length - 1);
//            OSSObject object = oss.getObject(getObjectRequest);
//            LOGGER.info("[aliyun] get path = {} position = {} length = {} response = {}", path, position, length, object);
//            try {
//                byte[] byteArray = IOUtils.readStreamAsByteArray(object.getObjectContent());
//                ByteBuffer byteBuffer = ByteBuffer.allocate(length);
//                byteBuffer.put(byteArray);
//                byteBuffer.flip();
//                byteBuffer.limit(length);
//                LOGGER.info("[aliyun] get path = {} position = {} length = {} response.buffer = {}", path, position, length, byteBuffer.toString());
//                future.complete(byteBuffer);
//            } catch (Exception e) {
//                future.completeExceptionally(e);
//            } finally {
//                try {
//                    object.close();
//                } catch (IOException ignore) {}
//            }
//            return future;
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return this.getSync(path, position, length);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, asyncExecutor);
        }

        private ByteBuffer getSync(String path, long position, int length) throws IOException {
            OSSObject object = null;
            try {
                LOGGER.info("[aliyun] get path = {} position = {} length = {}", path, position, length);
                GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, path);
                getObjectRequest.setRange(position, position + length - 1);
                object = oss.getObject(getObjectRequest);
                LOGGER.info("[aliyun] get path = {} position = {} length = {} response = {}", path, position, length, object);
                byte[] byteArray = IOUtils.readStreamAsByteArray(object.getObjectContent());
                ByteBuffer byteBuffer = ByteBuffer.allocate(length);
                byteBuffer.put(byteArray);
                byteBuffer.flip();
                byteBuffer.limit(length);
                LOGGER.info("[aliyun] get path = {} position = {} length = {} response.buffer = {}", path, position, length, byteBuffer.toString());
                return byteBuffer;
            } finally {
                if (object != null) {
                    try {
                        object.close();
                    } catch (Exception ignore) {

                    }
                }
            }
        }

        public ObjectMetadata getMetadata(String path) {
            return oss.getObjectMetadata(bucket, path);
        }
    }
}

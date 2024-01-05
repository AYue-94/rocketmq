package org.apache.rocketmq.tools.command.consumer;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.CustomizedRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.junit.Test;

public class UpdateSubGroupSubCommandTest {


    @Test
    public void test() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setInstanceName(UUID.randomUUID().toString());
        mqAdminExt.setNamesrvAddr("127.0.0.1:9876");
        mqAdminExt.start();
        SubscriptionData subscriptionData = mqAdminExt.querySubscription("fifoConsumer1", "MyOrderTopic");

        System.out.println(subscriptionData);
        Set<String> masterSet =
            CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, "DefaultCluster");
        for (String addr : masterSet) {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName("fifoConsumer1");
            subscriptionGroupConfig.setRetryMaxTimes(3);
            GroupRetryPolicy groupRetryPolicy = new GroupRetryPolicy();
            groupRetryPolicy.setType(GroupRetryPolicyType.CUSTOMIZED);
            groupRetryPolicy.setCustomizedRetryPolicy(new CustomizedRetryPolicy(new long[]{TimeUnit.SECONDS.toMillis(1)}));
            subscriptionGroupConfig.setGroupRetryPolicy(groupRetryPolicy);
            subscriptionGroupConfig.setConsumeMessageOrderly(true); // 设置消费组顺序消费
            mqAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
        }
        mqAdminExt.shutdown();
    }
}

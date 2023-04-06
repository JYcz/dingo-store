/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.service.version;

import com.google.common.util.concurrent.ListenableFuture;
import io.dingodb.sdk.service.connector.ServiceConnector;
import io.dingodb.version.Version;
import io.dingodb.version.VersionServiceGrpc;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class VersionService {

    private final ServiceConnector serviceConnector;

    public VersionService(ServiceConnector serviceConnector) {
        this.serviceConnector = serviceConnector;
    }

    //public int getCurrVersion() {
    //
    //}

    public int getNewVersion(Version.VersionType type, long id, int currVersion) throws InterruptedException,
                                                                                        ExecutionException {
        ManagedChannel channel = Grpc.newChannelBuilder("127.0.0.1:8899", InsecureChannelCredentials.create())
            .build();
        VersionServiceGrpc.VersionServiceBlockingStub stub = VersionServiceGrpc.newBlockingStub(channel);

        Version.GetNewVersionRequest request = Version.GetNewVersionRequest.newBuilder()
            .setVerId(Version.VersionId.newBuilder().setId(1).setType(Version.VersionType.TABLE).build())
            .setVersion(6)
            .build();
        //stub.getNewVersion(request).get()
        //System.out.println(stub.getNewVersion(request).get());
        long start = System.currentTimeMillis();
        Version.GetNewVersionResponse res = stub.getNewVersion(request);
        System.out.println(System.currentTimeMillis() - start + "   " +res.getVersion());
        //System.out.println(res.getVersion());
        //future.addListener(() -> {
        //  try {
        //      System.out.println(System.currentTimeMillis() - start);
        //    System.out.println(future.get());
        //  } catch (InterruptedException e) {
        //    throw new RuntimeException(e);
        //  } catch (ExecutionException e) {
        //    throw new RuntimeException(e);
        //  }
        //}, Executors.newCachedThreadPool());
        //LockSupport.park();
        channel.shutdown();
        channel.awaitTermination(1, TimeUnit.SECONDS);
        return 0;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        for (int i = 0; i < 10000; i++) {
            executo
            new VersionService(null).getNewVersion(Version.VersionType.TABLE, 1, 1);
        }
    }

}

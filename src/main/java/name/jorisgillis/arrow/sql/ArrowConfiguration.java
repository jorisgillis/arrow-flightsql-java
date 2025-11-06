/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package name.jorisgillis.arrow.sql;

import name.jorisgillis.arrow.sql.server.Producer;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.RootAllocator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.URISyntaxException;

@Configuration
public class ArrowConfiguration {
    private static final long MAX_MEMORY_SIZE_BYTES = 200 * 1024 * 1024L; // 200 MB
    private static final int MAX_MESSAGE_SIZE = 10 * 1024 * 1024; // 10 MB

    @Bean
    public Location location() throws URISyntaxException {
        return Location.forGrpcInsecure("0.0.0.0", 33333);
    }

    @Bean
    public RootAllocator rootAllocator() {
        return new RootAllocator(MAX_MEMORY_SIZE_BYTES);
    }

    @Bean
    public FlightSqlProducer flightSqlProducer(
            RootAllocator allocator,
            Location location,
            @Value("${application.data.dir}") String pathToDataDir)
            throws IOException {
        return new Producer(allocator, location, pathToDataDir);
    }

    @Bean
    public FlightServer flightServer(
            RootAllocator allocator, Location location, FlightSqlProducer producer) {
        return FlightServer.builder(allocator, location, producer)
                .maxInboundMessageSize(MAX_MESSAGE_SIZE)
                .build();
    }

    @Bean
    public FlightSqlClient flightSqlClient(RootAllocator allocator, Location location) {
        return new FlightSqlClient(FlightClient.builder(allocator, location).build());
    }
}

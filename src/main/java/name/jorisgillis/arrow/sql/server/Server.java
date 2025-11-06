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

package name.jorisgillis.arrow.sql.server;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class Server {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private final FlightServer flightServer;
    private final Location location;

    public Server(FlightServer flightServer, Location location) {
        this.flightServer = flightServer;
        this.location = location;
    }

    @PostConstruct
    public void start() throws IOException {
        LOGGER.info("Starting Flight SQL server ...");
        flightServer.start();
        LOGGER.info("Flight SQL server started at {}", location.getUri());
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        LOGGER.info("Stopping Flight SQL server ...");
        flightServer.shutdown();
        flightServer.awaitTermination(1, TimeUnit.HOURS);
    }
}

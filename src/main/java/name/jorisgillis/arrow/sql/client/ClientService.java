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

package name.jorisgillis.arrow.sql.client;

import name.jorisgillis.arrow.sql.client.exception.FlightStreamException;
import name.jorisgillis.arrow.sql.client.exception.FlightTicketException;
import name.jorisgillis.arrow.sql.random_data.RandomDataGenerator;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ClientService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientService.class);

    private final FlightSqlClient client;
    private final RootAllocator allocator;
    private final RandomDataGenerator randomDataGenerator;

    @Autowired
    public ClientService(
            FlightSqlClient client,
            RootAllocator allocator,
            RandomDataGenerator randomDataGenerator) {
        this.client = client;
        this.allocator = allocator;
        this.randomDataGenerator = randomDataGenerator;
    }

    private static FlightSql.CommandStatementIngest.TableDefinitionOptions
            createDefinitionOptions() {
        return FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder()
                .setIfNotExist(
                        FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption
                                .TABLE_NOT_EXIST_OPTION_CREATE)
                .setIfExists(
                        FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption
                                .TABLE_EXISTS_OPTION_FAIL)
                .build();
    }

    private static FlightSqlClient.ExecuteIngestOptions createIngestExecutionOptions(
            String tableName) {
        return new FlightSqlClient.ExecuteIngestOptions(
                tableName, createDefinitionOptions(), "default", "default", Map.of());
    }

    /**
     * List all tables in the database.
     *
     * <p>Gets the ticket(s) to fetch stream(s) of table names and schemas. Parsing them into the
     * list of table names.
     *
     * @return List of table names.
     */
    public List<Table> listTables() {
        var tables = new ArrayList<Table>();

        FlightInfo flightInfo = client.getTables("default", "*", "*", List.of(), true);

        for (Ticket ticket :
                flightInfo.getEndpoints().stream().map(FlightEndpoint::getTicket).toList()) {
            try (FlightStream stream = client.getStream(ticket)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    VarCharVector nameVector = (VarCharVector) root.getVector("table_name");
                    VarBinaryVector schemaVector = (VarBinaryVector) root.getVector("table_schema");

                    for (int i = 0; i < nameVector.getValueCount(); i++) {
                        tables.add(
                                new Table(
                                        new String(nameVector.get(i)),
                                        Schema.deserializeMessage(
                                                ByteBuffer.wrap(schemaVector.get(i)))));
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error fetching stream", e);
                throw new FlightStreamException(e);
            }
        }

        return tables;
    }

    /**
     * Generates random data and Writes generated data to the specified table.
     *
     * @param tableName Name of the table to write to
     */
    public void writeGeneratedData(String tableName) {
        try (IntVector idVector = new IntVector("id", allocator);
                VarCharVector nameVector =
                        new VarCharVector(
                                "name", FieldType.notNullable(new ArrowType.Utf8()), allocator);
                Float4Vector balanceVector = new Float4Vector("balance", allocator)) {

            var vectors = List.of(idVector, nameVector, balanceVector);
            Schema schema = new Schema(vectors.stream().map(FieldVector::getField).toList());

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                FlightSqlClient.ExecuteIngestOptions executeIngestOptions =
                        createIngestExecutionOptions(tableName);
                randomDataGenerator.generate(root, 10_000);
                client.executeIngest(root, executeIngestOptions);
            }
        }
    }

    /**
     * Executing a passed in SQL query.
     *
     * @param query The query to execute on the DataFusion engine, with the created Arrow file
     *     tables.
     * @return Result of the query converted to TSV format.
     */
    public String executeQuery(String query) {
        FlightInfo flightInfo = client.execute(query);

        if (flightInfo.getEndpoints().isEmpty()) {
            throw new FlightTicketException("No endpoint found for executing query");
        }
        if (flightInfo.getEndpoints().getFirst().getTicket() == null) {
            throw new FlightTicketException("No ticket for fetching query results");
        }

        StringBuilder result = new StringBuilder();

        List<Ticket> tickets =
                flightInfo.getEndpoints().stream().map(FlightEndpoint::getTicket).toList();
        for (Ticket ticket : tickets) {
            try (var stream = client.getStream(ticket)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    result.append(root.contentToTSVString());
                }
            } catch (Exception e) {
                LOGGER.error("Error while executing query", e);
                throw new FlightStreamException("Error = %s".formatted(e));
            }
        }

        return result.toString();
    }
}

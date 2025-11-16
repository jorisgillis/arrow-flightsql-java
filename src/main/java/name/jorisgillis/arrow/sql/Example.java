package name.jorisgillis.arrow.sql;

import com.google.protobuf.Any;
import com.google.protobuf.Message;

import org.apache.arrow.datafusion.ArrowFormat;
import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.ListingOptions;
import org.apache.arrow.datafusion.ListingTable;
import org.apache.arrow.datafusion.ListingTableConfig;
import org.apache.arrow.datafusion.SessionContexts;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Pattern;

/// This code is in a weird format, because it needs to fit in the format of the Arrow Java
/// Cookbook. The Cookbook has a specific format for code examples. The code examples are run as
/// tests and their output is compared to the expected output, defined in the ReStructured Text
/// format.
///
/// All the code from the cookbook example will be pasted into a main function of a class called
/// Example. I've recreated these conditions here to test the code before pasting it into the
/// Cookbook.
public class Example {
    public static void main(String[] args) {
        class FlightSqlExample {

            public FlightSqlExample() {}

            private void createATableWithPredefinedData(
                    RootAllocator rootAllocator, FlightSqlClient client) {
                Field idField = Field.notNullable("id", new ArrowType.Int(32, false));
                Field nameField = Field.notNullable("name", new ArrowType.Utf8());
                Field createdField =
                        Field.notNullable(
                                "ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));

                IntVector idVector = new IntVector(idField, rootAllocator);
                VarCharVector nameVector = new VarCharVector(nameField, rootAllocator);
                TimeStampMilliTZVector createdVector =
                        new TimeStampMilliTZVector(createdField, rootAllocator);
                try (VectorSchemaRoot ingestData =
                        VectorSchemaRoot.of(idVector, nameVector, createdVector)) {
                    ingestData.allocateNew();
                    int numberOfRows = 5;
                    for (int i = 0; i < numberOfRows; i++) {
                        idVector.set(i, i + 1);
                        nameVector.set(i, ("name" + i).getBytes(StandardCharsets.UTF_8));
                        createdVector.set(
                                i, Instant.parse("2026-01-01T00:00:00Z").toEpochMilli() + i);
                    }
                    ingestData.setRowCount(numberOfRows);

                    FlightSqlClient.ExecuteIngestOptions ingestOptions =
                            new FlightSqlClient.ExecuteIngestOptions(
                                    "writing_test",
                                    FlightSql.CommandStatementIngest.TableDefinitionOptions
                                            .newBuilder()
                                            .setIfExists(
                                                    FlightSql.CommandStatementIngest
                                                            .TableDefinitionOptions
                                                            .TableExistsOption
                                                            .TABLE_EXISTS_OPTION_FAIL)
                                            .build(),
                                    "default",
                                    "public",
                                    Map.of());
                    client.executeIngest(ingestData, ingestOptions);
                }
            }

            private void stoppingServer(FlightServer server, Thread serverThread)
                    throws InterruptedException {
                System.out.println("RUNNER: Ending server");
                server.shutdown();
                server.awaitTermination();
                serverThread.join();
                System.out.println("RUNNER: Server stopped");
            }

            private Thread startingServer(FlightServer server) throws InterruptedException {
                Thread serverThread =
                        new Thread(
                                () -> {
                                    try {
                                        server.start();
                                        System.out.println("RUNNER: Server running!");
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                serverThread.start();

                // Waiting for the server to start
                Thread.sleep(1000L);
                return serverThread;
            }

            private void listTablesAvailableOnServer(FlightSqlClient client) {
                FlightInfo listTablesInfo =
                        client.getTables("default", "public", ".*", List.of(), true);
                try (FlightStream listTablesStream =
                        client.getStream(listTablesInfo.getEndpoints().get(0).getTicket())) {
                    while (listTablesStream.next()) {
                        VectorSchemaRoot root = listTablesStream.getRoot();
                        System.out.println("CLIENT: Tables = \n" + root.getVector("table_name"));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            private void executeQueryPrintResults(FlightSqlClient client) throws Exception {
                FlightInfo executeInfo = client.execute("SELECT * FROM writing_test");
                try (FlightStream executeStream =
                        client.getStream(executeInfo.getEndpoints().get(0).getTicket())) {
                    while (executeStream.next()) {
                        VectorSchemaRoot root = executeStream.getRoot();
                        System.out.println("CLIENT: Query result = \n" + root.contentToTSVString());
                    }
                }
            }

            // =================================================================================================================
            // Main function: to start server and drive the client.
            // =================================================================================================================
            void runExample() {
                var producer = new ExampleProducer();
                try (var rootAllocator = new RootAllocator(1024 * 1024 * 100);
                        FlightServer server =
                                FlightServer.builder()
                                        .location(Location.forGrpcInsecure("0.0.0.0", 33333))
                                        .allocator(rootAllocator)
                                        .producer(producer)
                                        .build()) {
                    // =========================================================================================================
                    // Starting server in a virtual thread
                    // =========================================================================================================
                    Thread serverThread = startingServer(server);

                    // =========================================================================================================
                    // Creating client and executing commands on the server.
                    // =========================================================================================================
                    try (var flightClient =
                            FlightClient.builder()
                                    .location(Location.forGrpcInsecure("0.0.0.0", 33333))
                                    .allocator(rootAllocator)
                                    .build()) {
                        var client = new FlightSqlClient(flightClient);

                        System.out.println("CLIENT: Creating a table!");
                        createATableWithPredefinedData(rootAllocator, client);

                        System.out.println("CLIENT: Listing all tables");
                        listTablesAvailableOnServer(client);

                        System.out.println("CLIENT: Querying the table");
                        executeQueryPrintResults(client);

                        System.out.println("CLIENT: Ending client run.");
                    } catch (Exception e) {
                        System.out.println("CLIENT: Error running:\n");
                        e.printStackTrace(System.out);
                    }

                    // =========================================================================================================
                    // Shutdown the server
                    // =========================================================================================================
                    stoppingServer(server, serverThread);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // =================================================================================================================
            // FlightSqlProducer: the code responding on the Flight SQL Server side.
            // =================================================================================================================

            /**
             * A Producer is the heart of the Flight SQL Server.
             *
             * <p>It is responsible for handling all the requests from the client. From FlightInfo
             * (generating tickets) to the schema of the data and streaming the actual data.
             *
             * <p>This example does not showcase all possibilities of the Producer, but it does
             * showcase the most important ones.
             */
            private class ExampleProducer extends BasicFlightSqlProducer {
                private final Map<String, Path> tablePaths = new HashMap<>();

                private final RootAllocator rootAllocator =
                        new RootAllocator(1024 * 1024 * 200); // 200 MB
                private final Map<String, VectorSchemaRoot> results =
                        Collections.synchronizedMap(new WeakHashMap<>(100));

                private long getSizeInBytes(VectorSchemaRoot vectorSchemaRoot) {
                    long totalSize = 0;
                    for (ValueVector vector : vectorSchemaRoot.getFieldVectors()) {
                        totalSize += vector.getBufferSize();
                    }
                    return totalSize;
                }

                // =============================================================================================================
                // TABLES
                // =============================================================================================================
                @Override
                public FlightInfo getFlightInfoTables(
                        FlightSql.CommandGetTables request,
                        CallContext context,
                        FlightDescriptor descriptor) {
                    if (!request.getCatalog().equals("default")
                            || !request.getDbSchemaFilterPattern().equals("public")) {
                        throw new RuntimeException(
                                "Only supporting `default` (catalog) and `public` (database schema).");
                    }

                    return super.getFlightInfoTables(request, context, descriptor);
                }

                @Override
                public void getStreamTables(
                        FlightSql.CommandGetTables command,
                        CallContext context,
                        ServerStreamListener listener) {
                    try (VectorSchemaRoot tableRoot =
                            VectorSchemaRoot.create(Schemas.GET_TABLES_SCHEMA, rootAllocator)) {
                        VarCharVector nameVector =
                                (VarCharVector) tableRoot.getVector("table_name");
                        nameVector.allocateNewSafe();
                        VarCharVector typeVector =
                                (VarCharVector) tableRoot.getVector("table_type");
                        typeVector.allocateNewSafe();
                        VarBinaryVector schemaVector =
                                (VarBinaryVector) tableRoot.getVector("table_schema");
                        schemaVector.allocateNewSafe();

                        Pattern tableNamePattern = Pattern.compile(".*");
                        if (command.hasTableNameFilterPattern()) {
                            tableNamePattern = Pattern.compile(command.getTableNameFilterPattern());
                        }
                        int index = 0;
                        for (String tableName : tablePaths.keySet()) {
                            if (tableNamePattern.matcher(tableName).matches()) {
                                nameVector.set(index, tableName.getBytes(StandardCharsets.UTF_8));
                                typeVector.set(index, "table".getBytes(StandardCharsets.UTF_8));
                                schemaVector.set(index, "schema".getBytes(StandardCharsets.UTF_8));
                                index++;
                            }
                        }
                        tableRoot.setRowCount(index);

                        listener.start(tableRoot);
                        listener.putNext();
                        listener.completed();
                    }
                }

                // =============================================================================================================
                // QUERYING
                // =============================================================================================================
                @Override
                public FlightInfo getFlightInfoStatement(
                        FlightSql.CommandStatementQuery command,
                        CallContext context,
                        FlightDescriptor descriptor) {
                    try (var ctx = SessionContexts.create()) {
                        for (Map.Entry<String, Path> tableEntry : tablePaths.entrySet()) {
                            ctx.registerTable(
                                    tableEntry.getKey(),
                                    new ListingTable(
                                            new ListingTableConfig.Builder(
                                                            tableEntry.getValue().toString())
                                                    .withListingOptions(
                                                            ListingOptions.builder(
                                                                            new ArrowFormat())
                                                                    .build())
                                                    .build(ctx)
                                                    .get()));
                        }
                        DataFrame dataFrame = ctx.sql(command.getQuery()).get();

                        try (ArrowReader reader = dataFrame.collect(rootAllocator).get()) {
                            try {
                                VectorSchemaRoot collectedData =
                                        VectorSchemaRoot.create(
                                                reader.getVectorSchemaRoot().getSchema(),
                                                rootAllocator);
                                collectedData.allocateNew();
                                while (reader.loadNextBatch()) {
                                    VectorSchemaRootAppender.append(
                                            collectedData, reader.getVectorSchemaRoot());
                                }

                                results.put(command.getQuery(), collectedData);

                                /*
                                 * Very important to return a FlightSql.TicketStatementQuery as the Ticket in endpoints,
                                 * because this is used for routing to the getStreamStatement function in the getStream
                                 * function of FlightSqlProducer.
                                 */
                                FlightSql.TicketStatementQuery ticketStatementQuery =
                                        FlightSql.TicketStatementQuery.newBuilder()
                                                .setStatementHandle(command.getQueryBytes())
                                                .build();
                                byte[] statementQuerySerialized =
                                        Any.pack(ticketStatementQuery).toByteArray();
                                Ticket ticket = new Ticket(statementQuerySerialized);
                                FlightEndpoint endpoint = FlightEndpoint.builder(ticket).build();
                                return FlightInfo.builder(
                                                new Schema(List.of()),
                                                descriptor,
                                                List.of(endpoint))
                                        .setBytes(getSizeInBytes(collectedData))
                                        .setRecords(collectedData.getRowCount())
                                        .build();
                            } catch (Exception e) {
                                System.out.println("SERVER: Error while saving query result");
                                throw new RuntimeException(e);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("SERVER: Error running query: " + command.getQuery());
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void getStreamStatement(
                        FlightSql.TicketStatementQuery ticket,
                        CallContext context,
                        ServerStreamListener listener) {
                    var resultKey = ticket.getStatementHandle().toStringUtf8();
                    if (results.containsKey(resultKey)) {
                        VectorSchemaRoot root = results.get(resultKey);
                        listener.start(root);
                        listener.putNext();
                        listener.completed();
                        root.close();
                    } else {
                        listener.error(new RuntimeException("Could not find query result"));
                    }
                }

                // =============================================================================================================
                // WRITING
                // =============================================================================================================
                @Override
                public Runnable acceptPutStatementBulkIngest(
                        FlightSql.CommandStatementIngest command,
                        CallContext context,
                        FlightStream flightStream,
                        StreamListener<PutResult> ackStream) {
                    Path tablePath = Path.of(command.getTable() + ".arrow");
                    try (FileOutputStream fileWriter = new FileOutputStream(tablePath.toFile());
                            ArrowFileWriter arrowFileWriter =
                                    new ArrowFileWriter(
                                            flightStream.getRoot(),
                                            new DictionaryProvider.MapDictionaryProvider(),
                                            fileWriter.getChannel())) {
                        while (flightStream.next()) {
                            arrowFileWriter.writeBatch();
                        }
                        tablePaths.put(command.getTable(), tablePath);
                        ackStream.onNext(PutResult.empty());
                        return ackStream::onCompleted;
                    } catch (IOException e) {
                        return () -> ackStream.onError(e);
                    }
                }

                // =============================================================================================================
                // OTHER
                // =============================================================================================================
                @Override
                protected <T extends Message> List<FlightEndpoint> determineEndpoints(
                        T request, FlightDescriptor flightDescriptor, Schema schema) {
                    Ticket ticket = new Ticket(flightDescriptor.getCommand());
                    FlightEndpoint endpoint =
                            FlightEndpoint.builder(
                                            ticket, Location.forGrpcInsecure("0.0.0.0", 33333))
                                    .build();
                    return List.of(endpoint);
                }
            }
        }

        new FlightSqlExample().runExample();
    }
}

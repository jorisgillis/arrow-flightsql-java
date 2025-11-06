<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Arrow FlightSQL in Java

This is an example project on how to implement Apache Arrow FlightSQL in Java
with the Apache DataFusion query engine.

---

## How to run?

Prerequisites:

- Working Java environment
- Git to clone the project

To run this project, follow these steps:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/jorisgillis/arrow-flightsql-java
   cd arrow-flightsql-java
   ```

2. **Build the project:**
   Ensure you have Java and Maven installed. Then, build the project using:
   ```bash
   mvn clean install
   ```

3. **Run the application:**
   Execute the main class to start the server:
   ```bash
   JDK_JAVA_OPTIONS="--add-reads=org.apache.arrow.flight.core=ALL-UNNAMED --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED" \
   mvn spring-boot:run
   ```

   The `JDK_JAVA_OPTIONS` tell the JDK to open up some of the modules to all
   unnamed modules. This project is an unnamed module and needs access to
   `flight.core` and `arrow.memory`.


4. **Connect to the server:**
   A HTTP API is available on [http://localhost:8080](http://localhost:8080). Or
   you can connect with a FlightSQLClient from somewhere else.

The HTTP API has three endpoints:

| API call                            | Description                                  |
|-------------------------------------|----------------------------------------------|
| GET  http://localhost:8080/tables   | Lists all the tables in the database         |
| POST http://localhost:8080/generate | Generates a random table, table name in body |
| POST http://localhost:8080/query    | Executes the query as a string in the body   |

---

## How does Apache Arrow FlightSQL work?

[Apache Arrow Flight SQL](arrow-flight-sql-java) is pulls Arrow into the heart
of data systems. Building a generic way of interacting with a database based on
[Arrow](arrow) memory format and the [Arrow Flight](arrow-flight-java) protocol
for efficiently exchanging data over the network and between programming
languages.

I prefer to use the term **data system** over database, because notwithstanding
the SQL in *Flight SQL*, there is little in the protocol that limits it to being
SQL only. Except for the concept of *table*, there is little reference to
tabular data.

Flight SQL defines a *higher level interface for data systems* to implement and
for data clients to discover those data systems. The interface is organized
along the concepts of **catalog**, **database schema**, **table**, **table
type**, and **table schema**.

The implementing data system is free to interpret the concepts catalog and table
type to its convenience.

The Arrow Flight RPC functions form the foundation to fetch metadata and data
from the data system. A client explores the data system by requesting catalogs,
tables, etc. Queries are prepared or executed in the Arrow Flight way of
working: FlightInfo -> Endpoints & Tickets -> Fetching data streams.

### Very short introduction to Arrow Flight

Arrow Flight is a gRPC server-client protocol for fetching Arrow data in an
efficient, cross-platform, cross-programming language way. A conversation
between client and server typically follows this pattern:

1. Client requests **FlightInfo** from the server with a **FlightDescriptor**:
the flight info contains the *endpoints* and *tickets* that the client can use
to fetch data from the server(s). Indeed, the endpoints can point to different
and/or mulitple servers.

2. The client requests a **stream** based on the *endpoint (where?)* and the *ticket
   (what?)•.

3. The server sends a **stream of VectorSchemaRoots** (in Java; in Rust and
Python this would be RecordBatches), which the client accepts.

A client can send data to the server using a **FlightDescriptor** and the
**acceptPut** function on the server. The descriptor describes the stream that
will be uploaded to the server.

Finally, a server can expose an arbitrary set of **Actions** to the client. With
the **listActions** call the client learns about the available actions and with
the **doAction** call a client can execute actions on a server.

### Arrow Flight SQL

Arrow Flight SQL builds on top of the Flight protocol but standardizing a set of
generic actions and flows for getting and putting data. Through *dynamic
dispatching on the primitives of the Flight*, a higher level API is exposed to
the server and client.

The core Flight functions are:

- `getFlightInfo`: get the where (endpoints) and what (tickets) of a data set
- `getSchema`: get the schema of a data set
- `getStream`: stream a data set
- `acceptPut`: upload data the server
- `doAction`: execute an action on the server

Flight SQL adds variants to each of those functions for the following Flight SQL
concepts:

- `Statement`: A query, typically a SQL query, but in essence this is an
  arbitrary string

- `SubstraitPlan`: A query in the standardized Substrait format.
  [Substrait](substrait) is a uniform way of representing relational
  transformations (aka queries). Substrait is to queries, what Arrow is to data
  formats.

- `PreparedStatement`: A prepared statement, allows the data system to compile a
  plan with parameter place holders, avoiding repeated recompilation. Also
  offers protection against SQL injection, by containerizing user inputs from
  the query structure.

- `SqlInfo`: Information about the SQL capabilities of the data system.

- `TypeInfo`: Information about the data types supported by the data system.

- `Catalogs`: The catalogs in the data system. In a relational database, a
  catalog is a collection of metadata that describes the structure and
  organization of the database. Varies by implementation.

- `Schemas`: Grouping of tables. Varies by implementation.

- `Tables`: The tables in a catalog, schema, and/or filtered by name.

- `TableTypes`: The types of tables. Varies by implementation.

- `PrimaryKeys`: The primary keys of a table.

- `ExportedKeys`: The list of foreign keys that reference the primary key of a
  given table.

- `ImportedKeys`: The list of of foreign keys in a given table.

- `CrossReference`: The list of foreign keys in a given source table that
  reference columns in a given target table.

For the reading part this results in this set of functions.

<table>
  <tr>
    <th>Flight function</th>
    <th>Flight SQL function</th>
  </tr>
  <tr>
    <th rowspan="5">getFlightInfo</th>
    <td><code>getFlightInfoStatement</code></td>
  </tr>
  <tr>
    <td><code>getFlightInfoPreparedStatement</code></td>
  </tr>
  <tr>
    <td><code>getFlightInfoCatalogs</code></td>
  </tr>
  <tr>
    <td><code>getFlightInfoSchemas</code></td>
  </tr>
  <tr>
    <td><code>getFlightInfoTables</code></td>
  </tr>
  <tr>
    <th rowspan="5">getSchema</th>
    <td><code>getSchemaStatement</code></td>
  </tr>
  <tr>
    <td><code>getSchemaPreparedStatement</code></td>
  </tr>
  <tr>
    <td>Catalogs: default schema handled by <code>getSchema</code></td>
  </tr>
  <tr>
    <td>Schemas: default schema handled by <code>getSchema</code></td>
  </tr>
  <tr>
    <td>Tables: default schema handled by <code>getSchema</code></td>
  </tr>
  <tr>
    <th rowspan="5">getStream</th>
    <td><code>getStreamStatement</code></td>
  </tr>
  <tr>
    <td><code>getStreamPreparedStatement</code></td>
  </tr>
  <tr>
    <td><code>getStreamCatalogs</code></td>
  </tr>
  <tr>
    <td><code>getStreamSchemas</code></td>
  </tr>
  <tr>
    <td><code>getStreamTables</code></td>
  </tr>
</table>

Similarly, for the other concepts there is dispatching fromg the Flight
functions to the Flight SQL functions. For more information see the interface
`FlightSqlProducer`.

The **actions** are a special case in the Flight SQL domain. There is a fixed
set of actions related to sessions, transactions, prepared statements,
cancellation, and save points.

List of actions are routed to the corresponding functions:

- `FLIGHT_SQL_BEGIN_SAVEPOINT`: `beginSavepoint`
- `FLIGHT_SQL_BEGIN_TRANSACTION`: `beginTransaction`
- `FLIGHT_SQL_CREATE_PREPARED_STATEMENT`: `createPreparedStatement`
- `FLIGHT_SQL_CLOSE_PREPARED_STATEMENT`: `closePreparedStatement`
- `FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN`: `createPreparedSubstraitPlan`
- `FLIGHT_SQL_CANCEL_QUERY`: `cancelQuery`
- `FLIGHT_SQL_END_SAVEPOINT`: `endSavepoint`
- `FLIGHT_SQL_END_TRANSACTION`: `endTransaction`

By handling the incoming requests over the Flight protocol and dispatching to
the corresponding higher level functions in the `FlightSqlProducer` interface,
the developer of a data system can focus on the specifics of the data system
once, and be sure that Flight SQL clients can all communicate with the system.

As a plus, there is a Flight SQL JDBC driver, thus any Java application can
directly make us of your data system.

### Beyond SQL

The strategy of Flight SQL is not unique or specific to SQL and can be applied
with a different set of concepts to other data models.

One could imagine a **Flight Key-Value** that standardizes over a key-value data
store. Allowing for key-range or bulk queries that return a big set of values.

A **Flight NoSQL** standardizing operations over wide-column store such as
Apache Cassandra.

Or a **Flight Search** for search engines such as Elastic or MeiliSearch.

### Implementation

How to implement a Flight SQL enabled data system?

The short answer: by implementing every function in the `NoOpFlightSqlProducer`
interface. The `FlightSqlProducer` handles the dispatching to the higher level
functions listed in `NoOpFlightSqlProducer`.

The `BasicFlightSqlProducer` offers an abstract implementation of the the
`(NoOp)FlightSqlProducer`, allowing you to only implement the following
functions, to get to a working Flight SQL server:

```java
/**
 * Return a list of FlightEndpoints for the given request and FlightDescriptor. This method should
 * validate that the request is supported by this FlightSqlProducer.
 */
 @Override
 protected <T extends Message> List<FlightEndpoint> determineEndpoints(
     T request,
     FlightDescriptor flightDescriptor,
     Schema schema
 ) {
     // This implementation converts a descriptor into a ticket that will then need to be
     // handled by the `getStream...` function.
     return List.of(
         FlightEndpoint.builder(
             new Ticket(flightDescriptor.getCommand()),
             location
         ).build()
     );
 }

  /**
   * Returns data for catalogs based data stream.
   *
   * @param context Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamCatalogs(CallContext context, ServerStreamListener listener);

  /**
   * Returns data for schemas based data stream.
   *
   * @param command The command to generate the data stream.
   * @param context Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamSchemas(
      CommandGetDbSchemas command, CallContext context, ServerStreamListener listener);

  /**
   * Returns data for tables based data stream.
   *
   * @param command The command to generate the data stream.
   * @param context Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamTables(
      CommandGetTables command, CallContext context, ServerStreamListener listener);

  /**
   * Returns data for a SQL query based data stream.
   *
   * @param ticket Ticket message containing the statement handle.
   * @param context Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamStatement(
      TicketStatementQuery ticket, CallContext context, ServerStreamListener listener);
```

This basic implementation does not support table types, sql info, prepared
statements or keys. If a client inquires after these, it will get back an
unimplemented failure for the `NoOpFlightSqlProducer` interface.

If you implement every `getFlightInfo...` function of `FlightSqlProducer`, you
can safely ignore the `determineEndpoints` functions from
`BasicFlightSqlProducer`.


---

## On this example implementation

This implemenation is a demonstration I've created to learn more about the
Flight SQL protocol in Java. It showcases how to implement a Flight SQL server
on top of the DataFusion query engine (written in Rust). The storage engine I've
implemented is extremely rudimentary. It expects each table to be written in a
single Arrow IPC file.

It has many caveats:

- The database cannot update or drop tables.

- A table can only be written to once: at the creation.

- Data is stored in the Arrow IPC format (which is not intended for this use
  case).

- It computes a query when a ticket is provided. Typically the query is computed
during the `getFlightInfoStatement` function is executed, given back endpoint(s)
and ticket(s) where the result set can be fetched.

- There are no transactions.

- ...

---

## Where to go next?

For more information, refer to the official documentation of [Apache
Arrow][arrow] and [DataFusion][datafusion].

Other implementations of the Flight SQL:

- [FlightSqlClientDemoApp.java](https://github.com/apache/arrow-java/blob/main/flight/flight-sql/src/main/java/org/apache/arrow/flight/sql/example/FlightSqlClientDemoApp.java)
- [FlightSqlScenarioProducer.java](https://github.com/apache/arrow-java/blob/main/flight/flight-integration-tests/src/main/java/org/apache/arrow/flight/integration/tests/FlightSqlScenarioProducer.java#L731)
- Time series database [InfluxDB3](https://docs.influxdata.com/influxdb3/core/reference/client-libraries/flight/java-flightsql/) implements Flight SQL server (in Rust).
- [SQLite wrapper in C++](https://github.com/voltrondata/sqlflite)
- [DuckDB wrapper in C++](https://github.com/ClearTax/flight-duckdb-server)

---

## References

- [Apache Arrow](arrow)
- [Apache Arrow Flight - Java](arrow-flight-java)
- [Apache Arrow Flight SQL - Java](arrow-flight-sql-java)
- [Apache DataFusion](datafusion)
- [Substrait](substrait)

[arrow]: https://arrow.apache.org/

[arrow-flight-java]: https://arrow.apache.org/docs/java/flight.html

[arrow-flight-sql-java]: https://arrow.apache.org/docs/java/flight_sql.html

[datafusion]: https://arrow.apache.org/datafusion/

[substrait]: https://substrait.io/

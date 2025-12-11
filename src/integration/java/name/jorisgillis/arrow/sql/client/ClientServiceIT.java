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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.File;
import java.io.IOException;
import java.util.List;

@SpringBootTest
@ActiveProfiles("test")
public class ClientServiceIT {

    public static final Schema RANDOM_DATA_TABLE_SCHEMA =
            new Schema(
                    List.of(
                            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                            new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                            new Field(
                                    "balance",
                                    FieldType.nullable(
                                            new ArrowType.FloatingPoint(
                                                    FloatingPointPrecision.SINGLE)),
                                    null)));
    @Autowired private ClientService clientService;

    @BeforeAll
    static void setup() throws IOException {
        File dataDir = new File("test-data");

        if (dataDir.exists()) {
            FileUtils.deleteDirectory(dataDir);
        }
    }

    @Test
    void writingTables() {
        Table table1 = new Table("test-001", RANDOM_DATA_TABLE_SCHEMA);
        Table table2 = new Table("test-002", RANDOM_DATA_TABLE_SCHEMA);
        Table table3 = new Table("test-003", RANDOM_DATA_TABLE_SCHEMA);
        List<Table> tables = clientService.listTables();

        assertThat(tables).doesNotContain(table1, table2, table3);

        clientService.writeGeneratedData("test-001");

        tables = clientService.listTables();
        assertThat(tables).contains(table1);

        clientService.writeGeneratedData("test-002");
        clientService.writeGeneratedData("test-003");

        tables = clientService.listTables();
        assertThat(tables).contains(table1, table2, table3);
    }

    @Test
    void whenQueryingAnExistingTable_returnsTheResult() {
        clientService.writeGeneratedData("query-001");

        String result = clientService.executeQuery("SELECT COUNT(*) FROM 'query-001'");

        assertThat(result).isEqualTo("COUNT(UInt8(1))\n" + "10000");
    }
}

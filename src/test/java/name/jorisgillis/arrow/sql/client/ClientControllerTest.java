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

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import name.jorisgillis.arrow.sql.client.exception.FlightStreamException;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

@WebMvcTest(ClientController.class)
class ClientControllerTest {

    @Autowired private MockMvc mockMvc;

    @MockitoBean private ClientService clientService;

    private static Schema createABSchema() {
        return new Schema(
                List.of(
                        new Field("A", FieldType.nullable(new ArrowType.Int(32, true)), null),
                        new Field(
                                "B",
                                FieldType.nullable(
                                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                                null)));
    }

    @Test
    void whenListingTables_callsClientAndReturnsListOfTableName() throws Exception {
        Schema schemaTable1 = createABSchema();
        when(clientService.listTables())
                .thenReturn(List.of(new Table("table1", schemaTable1), new Table("table2", null)));

        mockMvc.perform(get("/tables"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].name", is("table1")))
                .andExpect(jsonPath("$[0].schema.fields[0].name", is("A")))
                .andExpect(jsonPath("$[0].schema.fields[1].name", is("B")))
                .andExpect(jsonPath("$[1].name", is("table2")));
    }

    @Test
    void whenListingTables_throwsFlightStreamException_returnsInternalServerError()
            throws Exception {
        when(clientService.listTables()).thenThrow(new FlightStreamException(new Exception()));

        mockMvc.perform(get("/tables")).andExpect(status().isInternalServerError());
    }

    @Test
    void whenGeneratingAndWritingATable_returnsANoContentResponse() throws Exception {
        mockMvc.perform(post("/generate").contentType(MediaType.APPLICATION_JSON).content("table1"))
                .andExpect(status().isAccepted());
    }

    @Test
    void whenGeneratingAndWritingATable_throwsFlightStreamException_returnsInternalServerError()
            throws Exception {
        doThrow(new FlightStreamException(new Exception()))
                .when(clientService)
                .writeGeneratedData("table1");

        mockMvc.perform(post("/generate").contentType(MediaType.APPLICATION_JSON).content("table1"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void whenExecutingQuery_returnsQueryResult() throws Exception {
        when(clientService.executeQuery("SELECT * FROM table1")).thenReturn("The Result!");

        mockMvc.perform(
                        post("/query")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("SELECT * FROM table1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.TEXT_PLAIN + ";charset=UTF-8"))
                .andExpect(content().string("The Result!"));
    }

    @Test
    void whenExecutingQuery_throwsFlightStreamException_returnsInternalServerError()
            throws Exception {
        when(clientService.executeQuery("SELECT * FROM table1"))
                .thenThrow(new FlightStreamException("Something went wrong!"));

        mockMvc.perform(
                        post("/query")
                                .contentType(MediaType.TEXT_PLAIN)
                                .content("SELECT * FROM table1"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void whenExecutingQuery_withNoQueryGiven_returnsBadRequest() throws Exception {
        mockMvc.perform(post("/query")).andExpect(status().isBadRequest());
    }
}

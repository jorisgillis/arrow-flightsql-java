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

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
public class ClientController {
  private final ClientService clientService;

  @Autowired
  public ClientController(ClientService clientService) {
    this.clientService = clientService;
  }

  @GetMapping(path = "/tables", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<Table>> listTables() {
    return ResponseEntity.ok(clientService.listTables());
  }

  @PostMapping(path = "/generate", produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> generate(@RequestBody String tableName) {
    clientService.writeGeneratedData(tableName);
    return ResponseEntity.accepted().build();
  }

  @PostMapping(path = "/query", produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> executeQuery(@RequestBody String query) {
    return ResponseEntity.ok().body(clientService.executeQuery(query));
  }
}

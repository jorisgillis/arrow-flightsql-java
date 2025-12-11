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

package name.jorisgillis.arrow.sql.random_data;

import org.apache.arrow.vector.*;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class RandomDataGenerator {

    public void generate(VectorSchemaRoot root, int rowCount) {
        for (FieldVector vector : root.getFieldVectors()) {
            if (vector instanceof IntVector) {
                generateIntData((IntVector) vector, rowCount);
            } else if (vector instanceof Float4Vector) {
                generateFloatData((Float4Vector) vector, rowCount);
            } else if (vector instanceof Float8Vector) {
                generateDoubleData((Float8Vector) vector, rowCount);
            } else if (vector instanceof VarCharVector) {
                generateVarCharData((VarCharVector) vector, rowCount);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported vector type: " + vector.getClass().getName());
            }
        }

        root.setRowCount(rowCount);
    }

    private void generateIntData(IntVector vector, int rowCount) {
        vector.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            vector.set(i, ThreadLocalRandom.current().nextInt());
        }
        vector.setValueCount(rowCount);
    }

    private void generateFloatData(Float4Vector vector, int rowCount) {
        vector.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            vector.set(i, ThreadLocalRandom.current().nextFloat());
        }
        vector.setValueCount(rowCount);
    }

    private void generateDoubleData(Float8Vector vector, int rowCount) {
        vector.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            vector.set(i, ThreadLocalRandom.current().nextDouble());
        }
        vector.setValueCount(rowCount);
    }

    private void generateVarCharData(VarCharVector vector, int rowCount) {
        vector.allocateNew();
        for (int i = 0; i < rowCount; i++) {
            String randomString = generateRandomString();
            vector.setSafe(i, randomString.getBytes());
        }
        vector.setValueCount(rowCount);
    }

    private String generateRandomString() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        return ThreadLocalRandom.current()
                .ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}

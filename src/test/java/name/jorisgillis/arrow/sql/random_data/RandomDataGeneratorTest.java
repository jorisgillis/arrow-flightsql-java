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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.junit.jupiter.api.Test;

import java.util.List;

class RandomDataGeneratorTest {

    @Test
    void generatesCorrectAmountOfSamples() {
        try (var allocator = new RootAllocator();
                var IntVector = new IntVector("int", allocator)) {
            var root = new VectorSchemaRoot(List.of(IntVector));

            new RandomDataGenerator().generate(root, 100);

            assertThat(root.getRowCount()).isEqualTo(100);
        }
    }

    @Test
    void generatesIntegers_Floats_andStrings() {
        try (var allocator = new RootAllocator();
                var IntVector = new IntVector("int", allocator);
                var float4Vector = new Float4Vector("float", allocator);
                var float8Vector = new Float8Vector("double", allocator);
                var stringVector = new VarCharVector("string", allocator)) {
            var root =
                    new VectorSchemaRoot(
                            List.of(IntVector, float4Vector, float8Vector, stringVector));

            new RandomDataGenerator().generate(root, 10);

            assertThat(root.getRowCount()).isEqualTo(10);
            assertThat(root.getVector("int")).isInstanceOf(IntVector.class);
            assertThat(((IntVector) root.getVector("int")).get(0)).isInstanceOf(Integer.class);
            assertThat(root.getVector("float")).isInstanceOf(Float4Vector.class);
            assertThat(((Float4Vector) root.getVector("float")).get(0)).isInstanceOf(Float.class);
            assertThat(root.getVector("double")).isInstanceOf(Float8Vector.class);
            assertThat(((Float8Vector) root.getVector("double")).get(0)).isInstanceOf(Double.class);
            assertThat(root.getVector("string")).isInstanceOf(VarCharVector.class);
            assertThat(((VarCharVector) root.getVector("string")).get(0))
                    .isInstanceOf(byte[].class);
        }
    }
}

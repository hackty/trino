/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.environment.EnvMultinodeMinioDataLake;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeDeltaLakeKerberizedHdfs;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeDeltaLakeOss;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class SuiteDeltaLake
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvMultinodeMinioDataLake.class)
                        .withGroups("configured_features", "delta-lake-minio")
                        .build(),

                testOnEnvironment(EnvSinglenodeDeltaLakeKerberizedHdfs.class)
                        .withGroups("configured_features", "delta-lake-hdfs")
                        .build(),
                //TODO enable the product tests against Databricks testing environment
//                testOnEnvironment(EnvSinglenodeDeltaLakeDatabricks.class)
//                        .withGroups("configured_features", "delta-lake-databricks")
//                        .withExcludedGroups("delta-lake-exclude-73")
//                        .build(),
//
//                testOnEnvironment(EnvSinglenodeDeltaLakeDatabricks91.class)
//                        .withGroups("configured_features", "delta-lake-databricks")
//                        .build(),

                testOnEnvironment(EnvSinglenodeDeltaLakeOss.class)
                        // TODO: make the list of tests run here as close to those run on SinglenodeDeltaLakeDatabricks
                        //  e.g. replace `delta-lake-oss` group with `delta-lake-databricks` + any exclusions, of needed
                        .withGroups("configured_features", "delta-lake-oss")
                        .build());
    }
}

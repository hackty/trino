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
package io.trino.plugin.teradata;

import io.airlift.configuration.Config;

public class TeradataConfig {

    private String user;

    private String password;

    private String url;

    @Config("teradata.user")
    public TeradataConfig setUser(String user) {
        this.user = user;
        return this;
    }

    @Config("teradata.password")
    public TeradataConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    @Config("teradata.url")
    public TeradataConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }
}

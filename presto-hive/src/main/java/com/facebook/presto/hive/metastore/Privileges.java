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
package com.facebook.presto.hive.metastore;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

public class Privileges
{
    private SetMultimap<String, HivePrivilegeInfo> userPrivileges;
    private SetMultimap<String, HivePrivilegeInfo> groupPrivileges;
    private SetMultimap<String, HivePrivilegeInfo> rolePrivileges;

    public Privileges(SetMultimap<String, HivePrivilegeInfo> userPrivileges, SetMultimap<String, HivePrivilegeInfo> groupPrivileges, SetMultimap<String, HivePrivilegeInfo> rolePrivileges)
    {
        this.userPrivileges = ImmutableSetMultimap.copyOf(userPrivileges);
        this.groupPrivileges = ImmutableSetMultimap.copyOf(groupPrivileges);
        this.rolePrivileges = ImmutableSetMultimap.copyOf(rolePrivileges);
    }

    public SetMultimap<String, HivePrivilegeInfo> getUserPrivileges()
    {
        return userPrivileges;
    }

    public SetMultimap<String, HivePrivilegeInfo> getGroupPrivileges()
    {
        return groupPrivileges;
    }

    public SetMultimap<String, HivePrivilegeInfo> getRolePrivileges()
    {
        return rolePrivileges;
    }
}

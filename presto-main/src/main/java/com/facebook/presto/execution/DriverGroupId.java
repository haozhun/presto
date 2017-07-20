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

package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;

public class DriverGroupId
{
    private final boolean grouped;
    private final int groupId;

    public static DriverGroupId empty()
    {
        return new DriverGroupId(false, 0);
    }

    public static DriverGroupId of(int id)
    {
        return new DriverGroupId(true, id);
    }

    @JsonCreator
    public static DriverGroupId of(OptionalInt id)
    {
        if (id.isPresent()) {
            return of(id.getAsInt());
        }
        else {
            return empty();
        }
    }

    public DriverGroupId(boolean grouped, int groupId)
    {
        this.grouped = grouped;
        this.groupId = groupId;
    }

    public boolean isPresent()
    {
        return grouped;
    }

    public int get()
    {
        checkState(grouped);
        return groupId;
    }

    @Override
    public String toString()
    {
        if (grouped) {
            return "Group" + groupId;
        }
        else {
            return "NotGrouped";
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DriverGroupId that = (DriverGroupId) o;
        return grouped == that.grouped &&
                groupId == that.groupId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(grouped, groupId);
    }

    @JsonValue
    public OptionalInt toOptionalInt()
    {
        return grouped ? OptionalInt.of(groupId) : OptionalInt.empty();
    }
}

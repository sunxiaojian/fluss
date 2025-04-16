---
sidebar_label: Default
sidebar_position: 2
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Default Merge Engine

## Overview

The **Default Merge Engine** in Fluss retains the latest record for a given primary key. It supports the following operations: `INSERT`, `UPDATE`, `DELETE`.
The Default Merge Engine is the default behavior in Fluss. It is enabled by default and does not require any additional configuration.

## Example

```sql title="Flink SQL"
CREATE TABLE T (
    k  INT,
    v1 DOUBLE,
    v2 STRING,
    PRIMARY KEY (k) NOT ENFORCED
);

-- Insert
INSERT INTO T VALUES (1, 1.0, 't1');
INSERT INTO T VALUES (1, 1.0, 't2');
INSERT INTO T VALUES (2, 2.0, 't3');
SELECT * FROM T WHERE k = 1;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 1  | 1.0 | t2 |
+----+-----+----+

-- Update
UPDATE T SET v1 = 4.0 WHERE k = 2;
SELECT * FROM T WHERE k = 2;
 -- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 2  | 2.0 | t3 |
+----+-----+----+
 
-- Delete
DELETE FROM T WHERE k = 2;
SELECT * FROM T;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 1  | 1.0 | t2 |
+----+-----+----+
```
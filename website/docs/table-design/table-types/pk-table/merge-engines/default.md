---
sidebar_label: Default (LastRow)
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
Fluss also supports partial column updates, allowing you to write only a subset of columns to incrementally update the data and ultimately achieve complete data. Note that the columns being written must include the primary key column.
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
INSERT INTO T(k, v1, v2) VALUES (1, 1.0, 't1');
INSERT INTO T(k, v1, v2) VALUES (1, 1.0, 't2');
SELECT * FROM T WHERE k = 1;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 1  | 1.0 | t2 |
+----+-----+----+

-- Update
INSERT INTO T(k, v1, v2) VALUES (2, 2.0, 't2');
UPDATE T SET v1 = 4.0 WHERE k = 2;
SELECT * FROM T WHERE k = 2;
 -- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 2  | 4.0 | t2 |
+----+-----+----+


-- Partial Update
INSERT INTO T(k, v1) VALUES (3, 3.0); -- set v1 to 3.0
SELECT * FROM T WHERE k = 3;
-- Output:
+----+-----+------+
| k  | v1  | v2   |
+----+-----+------+
| 3  | 3.0 | null |
+----+-----+------+
INSERT INTO T(k, v2) VALUES (3, 't3'); -- set v2 to 't3'
SELECT * FROM T WHERE k = 3;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 3  | 3.0 | t3 |
+----+-----+----+
 
-- Delete
DELETE FROM T WHERE k > 2;
SELECT * FROM T;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 1  | 1.0 | t2 |
+----+-----+----+
```
---
sidebar_label: Versioned
sidebar_position: 3
---

# Versioned Merge Engine

By specifying `'table.merge-engine' = 'versioned'`, users can update data based on the configured version column. Updates will be carried out when the latest value of the specified field is greater than the stored value. If it is less than or empty, no update will be made.

:::note
When using `versioned` merge engine, there are the following limits:
- `UPDATE` and `DELETE` statements are not supported
- Partial update is not supported
  :::

## Versioned Merge Column

:::note
The versioned merge column supports the following data types.
- INT
- BIGINT
- TIMESTAMP
- TIMESTAMP(p)
- TIMESTAMP_LTZ
- TIMESTAMP_LTZ(p)
  :::

example:
```sql title="versioned"

create table merge_engine_with_version (
    a int not null primary key not enforced,
    b string, 
    ts bigint
 ) with(
    'table.merge-engine' = 'versioned',
    'table.merge-engine.versioned.ver-column' = 'ts'
);
insert into merge_engine_with_version ( a,b,ts ) VALUES (1, 'v1', 1000);

-- insert data with ts < 1000, no update will be made
insert into merge_engine_with_version ( a,b,ts ) VALUES (1, 'v2', 999);
select * from merge_engine_with_version;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v1  | 1000 |
-- +---+-----+------+


-- insert data with ts > 1000, update will be made
insert into merge_engine_with_version ( a,b,ts ) VALUES (1, 'v2', 2000);
select * from merge_engine_with_version;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v2  | 2000 |

-- insert data with ts = null, no update will be made
nsert into merge_engine_with_version ( a,b,ts ) VALUES (1, 'v2', null);
select * from merge_engine_with_version;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v2  | 2000 |

```

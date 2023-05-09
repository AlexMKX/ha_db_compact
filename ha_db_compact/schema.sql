drop schema if exists ha_db_compact cascade;
create schema if not exists ha_db_compact;
create materialized view if not exists ha_db_compact.dupe_states as
(
with d as (SELECT state_id,
                  FIRST_VALUE(STATE_ID) OVER (PARTITION BY ENTITY_ID,
                      LAST_CHANGED_TS
                      ORDER BY last_updated_ts) AS FST,
                  last_value(state_id) OVER (PARTITION BY ENTITY_ID,
                      LAST_CHANGED_TS
                      ORDER BY last_changed_ts) AS LST
           FROM public.states
           WHERE to_timestamp(last_changed_ts) < (NOW() - interval '12 hours'))
select *,
       CASE when state_id != fst and state_id != lst then true else false end as to_delete
from d)
with no data;
create index if not exists dupes_state_id on ha_db_compact.dupe_states (state_id);
create index if not exists dupes_index_fst on ha_db_compact.dupe_states (fst);
create materialized view if not exists ha_db_compact.orphaned_attributes as
(
SELECT state_attributes.attributes_id
FROM state_attributes
WHERE NOT (EXISTS (SELECT states.attributes_id
                   FROM states
                   WHERE states.attributes_id = state_attributes.attributes_id))
  AND attributes_id < (SELECT max(states_1.attributes_id) AS max
                       FROM states states_1)
    )
with no data;
create index if not exists orphaned_attrs_idx on ha_db_compact.orphaned_attributes (attributes_id);

import psycopg2
import psycopg2.extensions
import time
import appdaemon.plugins.hass.hassapi as hass
import threading


class ha_db_compact(hass.Hass):
    # def __init__(self):
    #     self.handle = None
    #     self.host = None
    #     self.user = None
    #     self.dbname = None
    #     self.password = None
    #     super.__init__()
    dbname: str
    user: str
    password: str
    host: str
    mutex: threading.Lock

    def initialize(self):
        config = self.args.get('config', {})
        self.dbname = config['database']
        self.user = config['user']
        self.password = config['password']
        self.host = config['host']
        self.mutex = threading.Lock()
        # create_struct(cursor)
        # do_cleanup(cursor)

        self.run_every(self.cleanup, "now+300", 24*60*60)
        pass

    def cleanup(self, args):
        if self.mutex.locked():
            self.log("Already running")
            return
        try:
            self.mutex.acquire()
            conn = psycopg2.connect(
                **{"database": self.dbname, "user": self.user, "password": self.password, "host": self.host,
                   "application_name": "HA DB Cleanup"})
            self.log("Connected to database")
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self.create_struct(conn)
            self.log("Structures created")
            self.do_cleanup(conn)
            self.log("Cleanup complete")
        except Exception as e:
            self.log("Error: " + str(e))
        finally:
            self.mutex.release()

    def create_struct(self, conn: psycopg2.extensions.connection) -> None:
        cursor = conn.cursor()
        cursor.execute("drop materialized view if EXISTS cleanup_dupes;")
        cursor.execute("""create materialized view cleanup_dupes as
        (
        with d as (SELECT state_id,
                          FIRST_VALUE(STATE_ID) OVER (PARTITION BY ENTITY_ID,
                              LAST_CHANGED_TS
                              ORDER BY last_updated_ts) AS FST,
                          last_value(state_id) OVER (PARTITION BY ENTITY_ID,
                              LAST_CHANGED_TS
                              ORDER BY last_changed_ts) AS LST
                   FROM PUBLIC.STATES
                   WHERE to_timestamp(last_changed_ts) < (NOW() - interval '12 hours'))
        select *,
               CASE when state_id != fst and state_id != lst then true else false end as to_delete
        from d)
        with no data;""")
        cursor.execute(
            "create index dupes_state_id on cleanup_dupes (state_id); "
            "create index dupes_index_fst on cleanup_dupes (fst);")
        cursor.execute("refresh materialized view cleanup_dupes;")
        cursor.execute("drop materialized view if EXISTS orphaned_attributes;")
        cursor.execute("""create materialized view orphaned_attributes as
        (
        select attributes_id
        from public.state_attributes
        where not exists (select attributes_id
                          from public.states
                          where states.attributes_id = state_attributes.attributes_id)
        ) with no data;""")

    def do_cleanup(self, conn: psycopg2.extensions.connection) -> None:
        cursor = conn.cursor()
        cursor.execute("""UPDATE public.states
        SET OLD_STATE_ID = cleanup_dupes.FST
        FROM cleanup_dupes
        WHERE cleanup_dupes.STATE_ID = STATES.STATE_ID
          AND cleanup_dupes.FST != STATES.STATE_ID
          and states.old_state_id is not NULL;
        """)
        cursor.execute ("materialized view orphaned_attributes;")
        prev_rows: int
        curr_rows = 0
        while True:
            try:
                r = cursor.execute("""delete
                from state_attributes
                where exists (select attributes_id
                              from orphaned_attributes tablesample system(10)
                              where orphaned_attributes.attributes_id = state_attributes.attributes_id)
    
                """)
                prev_rows = curr_rows
                curr_rows = cursor.rowcount
                self.log(f'Rows deleted: {curr_rows} Prev: {prev_rows}')
                if curr_rows == 0:
                    break
                time.sleep(30)
                pass
            except Exception as e:
                self.error("Error: " + str(e))
                pass

        cursor.execute("drop materialized view if EXISTS orphaned_attributes;")
        cursor.execute("drop materialized view if EXISTS cleanup_dupes;")
        self.log("Starting vacuum")
        cursor.execute('vacuum full analyze;')
        self.log("Vacuum complete")

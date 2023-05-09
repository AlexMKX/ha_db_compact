import psycopg2
import psycopg2.errors
import psycopg2.extensions
import time
import appdaemon.plugins.hass.hassapi as hass
import threading

import os
import sys
from appdaemon.__main__ import main

if __name__ == '__main__':
    sys.argv.extend(['-c', './', '-t', '100'])
    sys.exit(main())


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

        self.run_every(self.cleanup, "now+300", 24 * 60 * 60)
        pass

    def cleanup(self, args) -> None:
        """
        scheduler function, Cleanup the database
        :param args:
        :return:
        """
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
            try:
                self.do_cleanup(conn)
            except Exception as e:
                self.log(f'Error: {str(e)} probably there is no structure, creating it')
                self.create_struct(conn)
                self.log(f'Created structure, retrying cleanup')
                self.do_cleanup(conn)
            self.log("Cleanup complete")
        except Exception as e:
            self.log("Error: " + str(e))
        finally:
            self.mutex.release()

    def create_struct(self, conn: psycopg2.extensions.connection) -> None:
        """
        Create the structures needed for the cleanup
        :param conn:  connection to the database
        """
        cursor = conn.cursor()
        with open(os.path.join(self.app_dir, "schema.sql"), "r") as f:
            cursor.execute(f.read())

    def do_cleanup(self, conn: psycopg2.extensions.connection) -> None:
        """
        Do the actual cleanup
        :param conn:
        """
        self.log("Starting cleanup")
        cursor = conn.cursor()
        cursor.execute("refresh materialized view ha_db_compact.dupe_states;")
        cursor.execute("""
            UPDATE public.states
            SET old_state_id = ha_db_compact.dupe_states.fst
            FROM ha_db_compact.dupe_states
            WHERE ha_db_compact.dupe_states.state_id = public.states.state_id
              AND ha_db_compact.dupe_states.fst != public.states.state_id
              and states.old_state_id is not NULL;
        """)
        self.log("States updated")
        cursor.execute("""
            delete from public.states
            where exists(select state_id
                         from ha_db_compact.dupe_states
                         where public.states.state_id = ha_db_compact.dupe_states.state_id
                           and ha_db_compact.dupe_states.to_delete = true);
                           """)
        self.log("States deleted")
        cursor.execute("refresh materialized view ha_db_compact.orphaned_attributes;")
        self.log("Orphaned attributes refreshed")
        prev_rows: int
        curr_rows = 0
        while True:
            try:
                r = cursor.execute("""
                delete
                    from public.state_attributes
                    where exists (
                    select attributes_id
                    from ha_db_compact.orphaned_attributes
                    where ha_db_compact.orphaned_attributes.attributes_id = public.state_attributes.attributes_id
                    )
                    """)
                prev_rows = curr_rows
                curr_rows = cursor.rowcount
                self.log(f'Rows deleted: {curr_rows} Prev: {prev_rows}')
                if curr_rows == 0:
                    break
                time.sleep(30)
                pass
            except psycopg2.errors.ForeignKeyViolation as e:
                self.error("Error: " + str(e))
                pass
        self.log("Starting vacuum")
        cursor.execute('vacuum full analyze;')
        self.log("Vacuum complete")

import os
from pathlib import Path
import subprocess
import logging

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor


class SurfaceDatabase:
    """Database class to handle database setup and data processing"""

    def __init__(
        self, dbname, dbuser, dbhost, dbpassword, pbf_path=None, alt_road_network=None
    ):
        """Initializes the database class

        Args:
            dbname (str): name of the database
            dbuser (str): name of the database user
            dbhost (str): database host
            dbpassword (str): database password
            pbf_path (str, optional): path to the pbf file for the OSM road network. If provided, alt_road_network is ignored. Defaults to None.
            alt_road_network (str, optional): Alternative road network to OSM. If pbf_path is None, required. Defaults to None.
        """
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.dbpassword = dbpassword
        self.pbf_path = pbf_path
        self.alt_road_network = alt_road_network

        self.setup_database(dbname, pbf_path, alt_road_network)

    def __repr__(self):
        return f"Database(name={self.dbname}, tables={self.get_table_names()}, input_road_network={self.pbf_path})"

    def get_table_names(self):
        # TODO
        return []

    def setup_database(self, dbname, pbf_path, alt_road_network=None):
        """
        Setup the database with the provided pbf file
        """
        res = subprocess.run(
            r"psql -lqt | cut -d \| -f 1 | grep -w " + dbname,
            shell=True,
            executable="/bin/bash",
        )
        if res.returncode == 0:
            logging.info(f"database already exists. Skip DB creation step.")
        else:
            logging.info(
                "setup database. Depending on the pbf_file size this might take a while"
            )
            osmosis_scheme_file = os.path.join(
                Path(os.path.dirname(__file__)).parent, "pgsnapshot_schema_0.6.sql"
            )
            subprocess.run(f"createdb {dbname}", shell=True, executable="/bin/bash")
            subprocess.run(
                f"psql  -d {dbname} -c 'CREATE EXTENSION postgis;'",
                shell=True,
                executable="/bin/bash",
            )
            subprocess.run(
                f"psql  -d {dbname} -c 'CREATE EXTENSION hstore;'",
                shell=True,
                executable="/bin/bash",
            )
            subprocess.run(
                f"psql -d {dbname} -f {osmosis_scheme_file}",
                shell=True,
                executable="/bin/bash",
            )
            subprocess.run(
                f"""osmosis --read-pbf {pbf_path} --tf accept-ways 'highway=*' --used-node --tf reject-relations --log-progress --write-pgsql database={dbname}""",
                shell=True,
                executable="/bin/bash",
            )
            logging.info("database setup complete")

        # TODO: implement alt_road_network alternative

    def create_dbconnection(self):
        """Create a connection to the database

        Returns:
            psycopg2.connection: a database connection
        """
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.dbuser,
            host=self.dbhost,
            password=self.dbpassword,
        )

    def execute_sql_query(self, query, params, is_file=True):
        """Execute a sql query

        Args:
            query (str): sql query to execute
            params (dict): parameters to pass to the query
            is_file (bool, optional): If sql query parameter is path to a file, then true. If the query is the query String, then False. Defaults to True.
        """
        # create table with sample data points
        conn = self.create_dbconnection()

        if is_file:
            with open(query, "r") as file:
                query = file.read()
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(sql.SQL(query.format(**params)))
            conn.commit()

        conn.close()

    def execute_many_sql_query(self, query, value_list):
        conn = self.create_dbconnection()

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.executemany(query, value_list)
            conn.commit()
        conn.close()


    # TODO: fix function
    def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = {table_name});"

        try:
            self.execute_sql_query(query, {"table_name": table_name}, is_file=False)
            return True
        except Exception as e:
            return False

    def table_to_shapefile(self, table_name, output_file):
        """Write a database geodata table to a shapefile

        Args:
            table_name (str): which table to write to shapefile
            output_file (str): where to write the shapefile to
        """
        subprocess.run(
            f'pgsql2shp -f "{output_file}" {self.dbname} "select * from {table_name}"',
            shell=True,
            executable="/bin/bash",
        )
        logging.info(f"table {table_name} writtena as shp file to {output_file}")

    def img_ids_from_dbtable(self, db_table):
        conn = self.create_dbconnection()

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            img_ids = cursor.execute(sql.SQL(f"SELECT img_id FROM {db_table}"))
            img_ids = cursor.fetchall()
            img_ids = [img_id[0] for img_id in img_ids]
        conn.close()
        return img_ids

    def add_rows_to_table(self, table_name, header, rows):
        # TODO: validate that header matches with table
        columns = ", ".join(header)
        placeholders = ", ".join(["%s"] * len(header))
        flattened_rows = [tuple(row) for row in rows]
        query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders});'
        self.execute_many_sql_query(query, flattened_rows)
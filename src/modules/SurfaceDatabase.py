import logging
import os
import subprocess
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, execute_batch


class SurfaceDatabase:
    """Database class to handle database setup and data processing"""

    def __init__(
        self, dbname, dbuser, dbpassword, dbhost, dbport, pbf_path=None, road_network_path=None, sql_custom_way_prep=None
    ):
        """Initializes the database class

        Args:
            dbname (str): name of the database
            dbuser (str): name of the database user
            dbpassword (str): database password
            dbhost (str): database host
            dbport (str): database port
            pbf_path (str, optional): path to the pbf file for the OSM road network. If provided, road_network_path is ignored. Defaults to None.
            road_network_path (str, optional): Alternative road network to OSM. If pbf_path is None, required. Defaults to None.
        """
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.dbport = dbport
        self.dbpassword = dbpassword
        self.pbf_path = pbf_path
        self.road_network_path = road_network_path
        self.sql_custom_way_prep = sql_custom_way_prep

        self.setup_database()

    def __repr__(self):
        return f"Database(name={self.dbname}, tables={self.get_table_names()}, input_road_network={self.pbf_path})"

    def setup_database(self):
        """
        Setup the database with the provided pbf file
        """
        res = subprocess.run(
            r"psql -lqt | cut -d \| -f 1 | grep -w " + self.dbname,
            shell=True,
            executable="/bin/bash",
        )
        if res.returncode == 0:
            logging.info("database already exists. Skip DB creation step.")
        else:
            logging.info(
                "Setup database"
            )
            subprocess.run(
                f"createdb {self.dbname} -h {self.dbhost} -p {self.dbport} -U {self.dbuser}",
                shell=True,
                executable="/bin/bash",
            )
            subprocess.run(
                f"psql  -d {self.dbname} -c 'CREATE EXTENSION postgis;'",
                shell=True,
                executable="/bin/bash",
            )
            if self.pbf_path:
                    logging.info(f"using pbf file {self.pbf_path}. Depending on the file size this might take a while")
                    subprocess.run(
                        f"psql  -d {self.dbname} -c 'CREATE EXTENSION hstore;'",
                        shell=True,
                        executable="/bin/bash",
                    )
                    osmosis_scheme_file = (
                        Path(os.path.dirname(__file__)).parent.parent / "pgsnapshot_schema_0.6.sql"
                    )
                    subprocess.run(
                        f"psql -d {self.dbname} -f {osmosis_scheme_file}",
                        shell=True,
                        executable="/bin/bash",
                    )
                    subprocess.run(
                        f"""osmosis --read-pbf {self.pbf_path} --tf accept-ways 'highway=*' --used-node --tf reject-relations --log-progress --write-pgsql database={self.dbname} user={self.dbuser} password={self.dbpassword}""",
                        shell=True,
                        executable="/bin/bash",
                    )
            elif self.road_network_path:
                subprocess.run(
                    f'ogr2ogr -f "PostgreSQL" PG:"dbname={self.dbname} user={self.dbuser} password={self.dbpassword} host={self.dbhost} port={self.dbport}" "{self.road_network_path}" -nln ways -overwrite',
                    shell=True,
                    executable="/bin/bash",
                )
                logging.info(f"Road network added to database from {self.road_network_path}")
                self.execute_sql_query(self.sql_custom_way_prep, {})

            else:
                logging.error("No road network provided")
            logging.info("database setup complete")

    def create_dbconnection(self):
        """Create a connection to the database

        Returns:
            psycopg2.connection: a database connection
        """
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.dbuser,
            host=self.dbhost,
            port=self.dbport,
            password=self.dbpassword,
        )

    def execute_sql_query(self, query, params, is_file=True, get_response=False):
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
            if get_response:
                res = cursor.fetchall()
            conn.commit()

        conn.close()
        if get_response:
            return res

    def execute_many_sql_query(self, query, value_list, params={}, is_file=True):
        conn = self.create_dbconnection()

        if is_file:
            with open(query, "r") as file:
                query = file.read()

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            execute_batch(cursor, sql.SQL(query.format(**params)), value_list)
            conn.commit()
        conn.close()

    def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}');"
        res = self.execute_sql_query(query, {"table_name": table_name}, is_file=False, get_response=True)
        return res[0][0]

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
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
        self.execute_many_sql_query(query, flattened_rows, is_file=False)


    def remove_aoi_tables(self, aoi_name):
        self.execute_sql_query(
                f"""DROP TABLE IF EXISTS {aoi_name}_eval_groups,
                                         {aoi_name}_group_predictions, 
                                         {aoi_name}_partitions,
                                         {aoi_name}_segmented_ways,
                                         {aoi_name}_way_selection,
                                         {aoi_name}_img_metadata,
                                         {aoi_name}_img_classifications,
                                         {aoi_name}_img_selection;
                """
            , {}, is_file=False)
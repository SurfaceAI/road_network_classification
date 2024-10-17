import logging
import os
import subprocess
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, execute_batch
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

import fnmatch
from pydriosm.downloader import GeofabrikDownloader
#from pydriosm.ios import PostgresOSM

class SurfaceDatabase:
    """Database class to handle database setup and data processing"""

    def __init__(
        self, dbname, dbuser, dbpassword, dbhost, dbport, pbf_folder=None, osm_region=None, 
        road_network_path=None, sql_custom_way_prep=None
    ):
        """Initializes the database class

        Args:
            dbname (str): name of the database
            dbuser (str): name of the database user
            dbpassword (str): database password
            dbhost (str): database host
            dbport (str): database port
            pbf_folder (str, optional): folder where OSM pbf files are stored
            osm_region (str, optional): path to the pbf file for the OSM road network. If provided, road_network_path is ignored. Defaults to None.
            road_network_path (str, optional): Alternative road network to OSM. If osm_region is None, required. Defaults to None.
        """
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.dbhost = dbhost if dbhost is not None else "localhost"
        self.dbport = dbport if dbport is not None else 5432
        self.pbf_folder = pbf_folder
        self.osm_region = osm_region
        self.road_network_path = road_network_path
        self.sql_custom_way_prep = sql_custom_way_prep

        self.setup_database()

    def __repr__(self):
        return f"Database(name={self.dbname}, tables={self.get_table_names()}, input_road_network={self.osm_region})"

    def _database_exists(self):
        query = f"SELECT 1 FROM pg_database WHERE datname = '{self.dbname}'"
        res = self.execute_sql_query(query, is_file=False, get_response= True, postgres_default=True)
        return len(res) > 0
        
    def setup_database(self):
        """
        Setup the database with the provided pbf file
        """

        if self._database_exists():
            logging.info(f"Database {self.dbname} already exists. Skip DB creation step.")
        else:
            logging.info(
                "Setup database."
            )
            try:
                self.execute_sql_query(f'CREATE DATABASE "{self.dbname}"', is_file=False, postgres_default=True, set_isolation_level=True)
                self.execute_sql_query('CREATE EXTENSION postgis;', is_file=False)
                if self.osm_region:
                    self._init_osm_data()

                elif self.road_network_path:
                    # TODO: refine subprocess call
                    subprocess.run(
                        f'ogr2ogr -f "PostgreSQL" PG:"dbname={self.dbname} user={self.dbuser} password={self.dbpassword} host={self.dbhost} port={self.dbport}" "{self.road_network_path}" -nln ways -overwrite',
                        shell=True,
                        executable="/bin/bash",
                    )
                    logging.info(f"Road network added to database from {self.road_network_path}")
                    self.execute_sql_query(self.sql_custom_way_prep)

                else:
                    logging.error("No road network provided")
            except Exception as e:

                # drop incorrectly initialized database
                self.execute_sql_query(f'DROP DATABASE "{self.dbname}"', is_file=False, postgres_default=True, set_isolation_level=True)
                logging.error(f"Error setting up database.")
                raise e
            logging.info("Database setup complete.")

    def _init_osm_data(self):
        self.execute_sql_query('CREATE EXTENSION hstore;', is_file=False)

        osmosis_scheme_file = Path(os.path.dirname(__file__)).parent.parent / "pgsnapshot_schema_0.6.sql"
        self.execute_sql_query(osmosis_scheme_file)

        if not os.path.exists(self.pbf_folder):
            os.makedirs(self.pbf_folder)
            is_download = True
        else:
            for root,_,files in os.walk(self.pbf_folder):
                pbf_files = fnmatch.filter(files, f"{self.osm_region}*.osm.pbf") 
                if len(pbf_files) > 0:
                    pbf_file = Path(root) / pbf_files[0]
                    is_download = False
                else: 
                    is_download = True
        if is_download:
            logging.info(f"PBF file for {self.osm_region} does not exist. It will be downloaded from Geofabrik.")
            gfd = GeofabrikDownloader()
            pbf_file = gfd.download_osm_data(self.osm_region, "pbf", 
                                            download_dir= self.pbf_folder,
                                            confirmation_required=False, 
                                            ret_download_path=True,
                                            verbose=True)
            logging.info(f"PBF file downloaded to {pbf_file}")
        
        logging.info(f"******LOAD OSM DATA FROM {pbf_file} TO DATABASE******")
        logging.info(f"Depending on the file size, this may take a while.")

        # TODO: use pydriosm instead of subprocess osmosis
        #  PostgresOSM(host=self.dbhost, port=self.dbport, username=self.dbuser, password=self.dbpassword, database_name=self.dbname, data_dir=pbf_file)
        subprocess.run(
            f"""osmosis --read-pbf {pbf_file} --tf accept-ways 'highway=*' --used-node --tf reject-relations --log-progress --write-pgsql database={self.dbname} user={self.dbuser}""",
            shell=True,
            executable="/bin/bash",
        )
        if is_download:
            logging.info(f"Remove downloaded pbf file {pbf_file}.")
            os.remove(pbf_file)

    def _create_dbconnection(self, postgres_default=False):
        """Create a connection to the database

        Returns:
            psycopg2.connection: a database connection
        """
        dbname = 'postgres' if postgres_default else self.dbname
        return psycopg2.connect(
            dbname=dbname,
            user=self.dbuser,
            host=self.dbhost,
            port=self.dbport,
            password=self.dbpassword,
        )

    def execute_sql_query(self, query, params={}, is_file=True, postgres_default= False, get_response=False, set_isolation_level=False):
        """Execute a sql query

        Args:
            query (str): sql query to execute
            params (dict): parameters to pass to the query
            is_file (bool, optional): If sql query parameter is path to a file, then true. If the query is the query String, then False. Defaults to True.
            postgres_default (bool, optional): If the query is to be executed on the default postgres database. Defaults to False.
            get_response (bool, optional): If the response is to be fetched. Defaults to False.
            set_isolation_level (bool, optional): If the isolation level is to be set. Defaults to False.
        """
        # create table with sample data points
        conn = self._create_dbconnection(postgres_default)
        if set_isolation_level:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

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
        conn = self._create_dbconnection()

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
        logging.info(f"Output written as shapefile to {output_file}.")

    def img_ids_from_dbtable(self, db_table):
        conn = self._create_dbconnection()

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

    def remove_temp_tables(self, aoi_name):
        self.execute_sql_query(
                f"""DROP TABLE IF EXISTS {aoi_name}_eval_groups,
                                         {aoi_name}_partitions,
                                         {aoi_name}_segmented_ways,
                                         {aoi_name}_way_selection,
                                         {aoi_name}_img_selection;
                """
            , is_file=False)
import click
import logging
import sys
from tabulate import tabulate
from rubrik_rsc_oracle.common import connection
from rubrik_rsc_oracle.common.oracle_database import OracleDatabase


@click.command()
@click.option('--database_name', '-d', type=str, required=False,  help='The database name')
@click.option('--host_name', '-h', type=str, required=False,  help='The database host or RAC cluster')
@click.option('--keyfile', '-k', type=str, required=False,  help='The connection keyfile path')
@click.option('--insecure', is_flag=True,  help='Flag to use insecure connection')
@click.option('--debug_level', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(database_name, host_name, keyfile, insecure, debug_level):
    """
    Displays information about the Oracle database object, the available snapshots, and recovery ranges.
    If no source_host_db is supplied, all non-relic Oracle databases will be listed.
    Recommended console line size is 120 characters.
    """
    numeric_level = getattr(logging, debug_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(debug_level))
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(numeric_level)
    console_formatter = logging.Formatter('%(asctime)s: %(message)s')
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

    rubrik = connection.RubrikConnection(keyfile, insecure)
    if database_name:
        database = OracleDatabase(rubrik, database_name, host_name)
        logger.debug("Database ID: {}".format(database.id))
        database.get_details()
    else:
        databases = OracleDatabase.get_oracle_databases(rubrik)['oracleDatabases']['nodes']
        db_data = []
        db_headers = ["Database", "DB Unique Name", "Role", "DG_Group", "Host/Cluster", "Instances", "CDM Cluster", "SLA", "Assignment"]
        for db in databases:
            if not db['isLiveMount']:
                db_element = [''] * 9
                db_element[0] = db['name'].lower()
                for path in db['physicalPath']:
                    if path['objectType'] == 'OracleRac' or path['objectType'] == 'OracleHost':
                        db_element[4] = path['name']
                db_element[5] = db['numInstances']
                if db['dataGuardType'] == 'DATA_GUARD_MEMBER':
                    db_element[1] = db['dbUniqueName']
                    db_element[2] = db['dbRole']
                    db_element[3] = db['dataGuardGroup']['name']
                db_element[6] = db['cluster']['name']
                db_element[7] = db['effectiveSlaDomain']['name']
                db_element[8] = db['slaAssignment']
                db_data.append(db_element)
        db_data.sort(key=lambda x: (x[0], x[1]))
        print("-" * 130)
        print(tabulate(db_data, headers=db_headers))
        print("-" * 130)
    rubrik.delete_session()
    return


class RubrikOracleBackupInfoError(connection.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

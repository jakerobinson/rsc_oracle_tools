import click
import logging
import sys
from tabulate import tabulate
from rsc_oracle.common import connection
from rsc_oracle.common import oracle_database


@click.command()
@click.option('--database_name', '-d', type=str, required=False,  help='The database name')
@click.option('--host_name', '-h', type=str, required=False,  help='The database host or RAC cluster')
@click.option('--mounted_host', '-m', type=str, required=False,  help='The host with the live mount to remove')
@click.option('--keyfile', '-k', type=str, required=False,  help='The connection keyfile path')
@click.option('--insecure', is_flag=True,  help='Flag to use insecure connection')
@click.option('--debug', is_flag=True,  help='Flag to enable debug mode')

def cli(database_name, host_name, mounted_host, keyfile, insecure, debug):
    """
    This will print the information about a Rubrik live mount using the database name and the live mount host.

\b
    Returns:
        
    """
    if debug:
        debug_level = "DEBUG"
    else:
        debug_level = "Warning"
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
        pass
    # if source_host_db and mounted_host:
    #     source_host_db = source_host_db.split(":")
    #     mount = rbs_oracle_common.RubrikRbsOracleMount(rubrik, source_host_db[1], source_host_db[0], mounted_host)
    #     live_mount_ids = mount.get_oracle_live_mount_id()
    #     if not live_mount_ids:
    #         raise RubrikOracleMountInfoError("No live mounts found for {} live mounted on {}. ".format(source_host_db[1], mounted_host))
    #     else:
    #         print("Live mounts of {} mounted on {}:".format(source_host_db[1], mounted_host))
    #         for live_mount_id in live_mount_ids:
    #             logger.info("Getting info for mount with id: {}.".format(live_mount_id))
    #             mount_information = mount.get_live_mount_info(live_mount_id)
    #             logger.debug("mount_info: {0}".format(mount_information))
    #             print("Source DB: {}  Source Host: {}  Mounted Host: {}  Owner: {}  Created: {}  Status: {}  id: {}".format(
    #                 source_host_db[1], source_host_db[0], mounted_host, mount_information.get('ownerName', 'None'),
    #                 mount_information['creationDate'], mount_information['status'], mount_information['id']))
    #         rubrik.delete_session()
    #         return
    else:
        logger.debug("Source and target host not supplied. Getting full list of mounts")

        oracle_live_mounts = oracle_database.OracleDatabase.get_oracle_mounts(rubrik)
        logger.debug("All mounts: {}".format(oracle_live_mounts))
        live_mounts = []
        live_mount_headers = ["Cluster", "Source DB", "Mounted Host", "Files Only", "Status", "Created"]
        for mount in oracle_live_mounts:
            db_element = [''] * 6
            db_element[0] = mount.get('cluster', "NA").get('name')
            db_element[1] = mount.get('sourceDatabase', "NA").get('name')
            if mount.get('targetOracleHost'):
                db_element[2] = mount.get('targetOracleHost', "NA").get('name')
            else:
                db_element[2] = mount.get('targetOracleRac', "NA").get('name')
            db_element[3] = mount.get('isFilesOnlyMount', "NA")
            db_element[4] = mount.get('status', "NA")
            db_element[5] = mount.get('creationDate', "NA")
            live_mounts.append(db_element)
        live_mounts.sort(key=lambda x: (x[0], x[1]))
        print("*" * 100)
        print(tabulate(live_mounts, headers=live_mount_headers))
        print("*" * 100)
        rubrik.delete_session()
        return


class RubrikOracleMountInfoError(connection.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

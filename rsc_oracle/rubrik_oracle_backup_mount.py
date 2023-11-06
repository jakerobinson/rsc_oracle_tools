
import click
import logging
import sys
import datetime
import pytz
from rsc_oracle.common import connection, oracle_database, oracle_target


@click.command()
@click.option('--database_name', '-d', type=str, required=True,  help='The database name')
@click.option('--host', '-h', type=str, required=False,  help='The database host or RAC cluster')
@click.option('-cluster_name', '-c', type=str, required=False,  help='The cluster with the backup')
@click.option('--path', '-p', type=str, required=True, help='The path used to mount the backup files')
@click.option('--restore_time', '-r', type=str, help='Point in time to mount the DB, format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--target', '-t', type=str, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--timeout', type=int, default=12, help='Time to wait for mount operation to complete in minutes before script timeouts. Mount will still continue after timeout.')
@click.option('--no_wait', is_flag=True, help='Queue Live Mount and exit.')
@click.option('--keyfile', '-k', type=str, required=False,  help='The connection keyfile path')
@click.option('--insecure', is_flag=True,  help='Flag to use insecure connection')
@click.option('--debug', is_flag=True,  help='Flag to enable debug mode')
def cli(database_name, host, cluster_name, path, restore_time, target, timeout, no_wait, keyfile, insecure, debug):
    """
    This will mount the requested Rubrik Oracle backup set on the provided path.

\b
    The source database is specified as a db or and db name along with a host name. The mount path is required. If the
     restore time is not provided the most recent recoverable time will be used. The host for the mount can be specified
     if it is not it will be mounted on the source host.
    Returns:
        live_mount_info (dict): The information about the requested files only mount returned from the Rubrik CDM.
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
    database = oracle_database.OracleDatabase(rubrik, database_name, host, cluster_name)
    logger.debug(f"Database ID: {database.id}, Cluster ID: {database.cluster_id}")
    database_details = database.get_details()
    logger.debug(f"DB Details: {database_details}")
    timezone = database_details['cluster']['timezone']
    host_type = "None"
    if database.dataguard:
        for node in database_details['descendantConnection']['nodes']:
            for path in node['physicalPath']:
                if path['objectType'] == 'OracleHost':
                    host_type = "standAlone"
                    # host = path['name']
                elif path['objectType'] == 'OracleRac':
                    host_type = "RAC"
                    # host = path['name']
    else:
        if database_details['physicalPath'][0]['objectType'] == 'OracleRac':
            host_type = 'RAC'
        else:
            host_type = "standAlone"
    logger.debug(f"Source database type: {host_type}")
    rac = False
    if host_type == 'RAC':
        rac = True
    host = oracle_target.OracleTarget(rubrik, target, database.cluster_id, rac=rac)
    logger.debug(f"Target name: {host.rac_name}, ID: {host.id}")
    rubrik.delete_session()
    exit(20)

    if host and not target:
        target = host
    target_id = database.get_target_id(rubrik.cluster_id, target)
    if restore_time:
        time_ms = database.epoch_time(restore_time, timezone)
        logger.warning("Mounting backup pieces for a point in time restore to time: {}.".format(time_restore))
    else:
        logger.warning("Using most recent recovery point for mount.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], timezone)
    logger.warning("Starting the mount of the requested {} backup pieces on {}.".format(source_host_db[1], host_target))
    live_mount_info = database.live_mount(target_id, time_ms, files_only=True, mount_path=path)
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(
        cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    logger.info("Live mount requested at {}.".format(start_time.strftime(fmt)))
    logger.info("No wait flag is set to {}.".format(no_wait))
    if no_wait:
        logger.warning("Live mount id: {} Mount status: {}.".format(live_mount_info['id'], live_mount_info['status']))
        rubrik.delete_session()
        return live_mount_info
    else:
        live_mount_info = database.async_requests_wait(live_mount_info['id'], timeout)
        logger.warning("Async request completed with status: {}".format(live_mount_info['status']))
        if live_mount_info['status'] != "SUCCEEDED":
            raise RubrikOracleBackupMountError(
                "Mount of backup files did not complete successfully. Mount ended with status {}".format(
                    live_mount_info['status']))

    rubrik.delete_session()
    return


class RubrikOracleBackupMountError(connection.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

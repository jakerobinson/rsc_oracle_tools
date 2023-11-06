
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
@click.option('--restore_time', '-r', type=str, help='Point in time to mount the DB, format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--target', '-t', type=str, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--pfile', '-p', type=str, help='Custom Pfile path (on target host)')
@click.option('--aco_file_path', '-a', type=str, help='ACO file path for parameter changes')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME on destination host. Required as option or in ACO File if source is a Data Guard Group.')
@click.option('--timeout', type=int, default=12, help='Time to wait for mount operation to complete in minutes before script timeouts. Mount will still continue after timeout.')
@click.option('--no_wait', is_flag=True, help='Queue Live Mount and exit.')
@click.option('--keyfile', '-k', type=str, required=False,  help='The connection keyfile path')
@click.option('--insecure', is_flag=True,  help='Flag to use insecure connection')
@click.option('--debug', is_flag=True,  help='Flag to enable debug mode')
def cli(database_name, host, cluster_name, restore_time, target, pfile, aco_file_path, oracle_home, timeout, no_wait, keyfile, insecure, debug):
    """Live mount a Rubrik Oracle Backup.

\b
    Gets the backup for the Oracle database on the Oracle database host and will live mount it on the host provided.

\b
    Returns:
        live_mount_info (json); JSON text file with the Rubrik cluster response to the live mount request
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

    rubrik = rbs_oracle_common.RubrikConnection(keyfile, insecure)
    source_host_db = source_host_db.split(":")
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0], 180)
    oracle_db_info = database.get_oracle_db_info()
    logger.debug(oracle_db_info)
    # If source DB is RAC then the target for the live mount must be a RAC cluster
    host_id = None
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            host_id = database.get_rac_id(rubrik.cluster_id, host_target)
    else:
        host_id = database.get_host_id(rubrik.cluster_id, host_target)
    if time_restore:
        time_ms = database.epoch_time(time_restore, rubrik.timezone)
        logger.warning("Using {} for mount.". format(time_restore))
    else:
        logger.warning("Using most recent recovery point for mount.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], rubrik.timezone)
    aco_config_map = None
    aco_parameters = []
    if aco_file_path:
        logger.warning("Using ACO File: {}".format(aco_file_path))
        try:
            with open(aco_file_path) as f:
                for curline in f:
                    curline = curline.strip()
                    if not curline.startswith("#") and curline != '':
                        curline = curline.replace("'", '')
                        curline = curline.replace('"', '')
                        aco_parameters.append(curline.split("=",1))
                        logger.debug("aco_file line: {}".format(curline))
        except IOError as e:
            rubrik.delete_session()
            raise RubrikOracleDBMountError("I/O error({0}): {1}".format(e.errno, e.strerror))
        except Exception:
            rubrik.delete_session()
            raise RubrikOracleDBMountError("Unexpected error: {}".format(sys.exc_info()[0]))
        aco_config_map = {}
        for config in aco_parameters:
            aco_config_map[config[0]] = config[1]
        logger.debug(aco_config_map)
    if pfile:
        logger.warning("Using custom PFILE File: {}.".format(pfile))
        if aco_parameters:
            logger.debug("Using ACO file with PFILE.")
            for config in aco_parameters:
                logger.debug(config)
                if config[0].upper() != 'ORACLE_HOME' and config[0].upper() != 'SPFILE_LOCATION' and config[0][:-1].upper() != 'DB_CREATE_ONLINE_LOG_DEST_':
                    rubrik.delete_session()
                    raise RubrikOracleDBMountError("When using a custom PFILE the only parameters allowed in the ACO file are ORACLE_HOME, SPFILE_LOCATION and DB_CREATE_ONLINE_LOG_DEST_*.")
    logger.debug("dataGuardType is {0}".format(oracle_db_info['dataGuardType']))
    # If source is a Data Guard Group, check to be sure an ORACLE_HOME is provided
    if oracle_db_info['dataGuardType'] == 'DataGuardGroup':
        if oracle_home:
            logger.debug("DG GROUP USING ORACLE_HOME OPTION")
        elif aco_config_map:
            logger.debug("Source is a DG Group and ACO File is being used. Checking for ORACLE_HOME...")
            if 'ORACLE_HOME' in aco_config_map:
                logger.debug("ORACLE_HOME: {0} is present in the ACO File.".format(aco_config_map['ORACLE_HOME']))
            else:
                logger.warning("ORACLE_HOME is not set in the ACO File: {0} or provided as an option.".format(aco_file_path))
                rubrik.delete_session()
                raise RubrikOracleDBMountError("When cloning a DG Group database, the ORACLE_HOME must be provided")
        else:
            logger.warning("ORACLE_HOME must be specified for a DG Group.")
            rubrik.delete_session()
            raise RubrikOracleDBMountError("When cloning a DG Group database, the ORACLE_HOME must be provided")
    if oracle_home and database.v6:
        logger.debug("Post 6.0 CDM ORACLE_HOME is supported")
    elif oracle_home:
        rubrik.delete_session()
        raise RubrikOracleDBMountError("The Oracle Home parameter is not supported with pre 6.0 CDM.")
    logger.warning("Starting Live Mount of {0} on {1}.".format(source_host_db[1], host_target))
    logger.debug("db_clone parameters host_id={0}, time_ms={1}, pfile={2}, aco_config_map={3}, oracle_home={4}".format(host_id, time_ms, pfile, aco_config_map, oracle_home))
    live_mount_info = database.live_mount(host_id=host_id, time_ms=time_ms, pfile=pfile, aco_config_map=aco_config_map, oracle_home=oracle_home)
    logger.debug(live_mount_info)
    # Set the time format for the printed result
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    logger.debug("Live mount status: {0}, Started at {1}.".format(live_mount_info['status'], start_time.strftime(fmt)))
    if no_wait:
        rubrik.delete_session()
        return live_mount_info
    else:
        live_mount_info = database.async_requests_wait(live_mount_info['id'], timeout)
        logger.warning("Async request completed with status: {}".format(live_mount_info['status']))
        if live_mount_info['status'] != "SUCCEEDED":
            rubrik.delete_session()
            raise RubrikOracleDBMountError(
                "Mount of Oracle DB did not complete successfully. Mount ended with status {}".format(
                    live_mount_info['status']))
        logger.warning("Live mount of the backup files completed.")
        rubrik.delete_session()
        return live_mount_info


class RubrikOracleDBMountError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

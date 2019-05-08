import csv
import mysql.connector
import logging
import argparse
import sys
import fnmatch
import os
import datetime
import time


def setup_logger():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    # logger.setLevel(logging.DEBUG)

    return logger


def process_args():
    parser = argparse.ArgumentParser(description='location of csv files.')

    parser.add_argument('-hs', '--host', metavar='MySql Host',
                        help='MySql Host', default="localhost")

    parser.add_argument('-u', '--user', metavar='MySql User',
                        help='MySql User', default="root")

    parser.add_argument('-p', '--password', metavar='MySql Password',
                        help='MySql Password', default="")

    parser.add_argument('-d', '--date', metavar='date for which backfill runs',
                        help='date for which backfill runs in yyyy-mm-dd format', default=None)

    return parser.parse_args()


def rollback(date):
    rollback_files = get_files_for_date(date)

    for rollback_file in rollback_files:
        start = datetime.datetime.now()
        log.info("Rolling back " + rollback_file)
        process_rollback_file(rollback_file)
        mydb.commit()
        os.remove("rollback/"+rollback_file)
        time_taken = (datetime.datetime.now() - start).seconds
        log.info("Rollback completed, deleted file "+rollback_file)
        sleep_for = 1 if time_taken == 0 else time_taken * 2
        log.info("sleeping for {} secs".format(sleep_for))
        time.sleep(sleep_for)


def process_rollback_file(rollback_file):
    f2 = open("rollback/"+rollback_file)
    reader = csv.reader(f2, dialect='excel')
    try:
        for row in reader:
            delete(row)
    except Exception as e:
        log.error('Could not read fee File: {}. Exiting!!\n{}'.format(file, e))
        sys.exit(1)
    finally:
        f2.close()


def get_files_for_date(date):
    rollback_files = fnmatch.filter(os.listdir('rollback'), date + "*.csv")

    if rollback_files is None:
        log.error("Rollback file not found for date " + date)
        sys.exit(1)

    return rollback_files


def delete(row):
    cursor = mydb.cursor()
    cursor.execute(row[0])


if __name__ == '__main__':
    log = setup_logger()

    my_args = process_args()

    mydb = mysql.connector.connect(
        host=my_args.host,
        user=my_args.user,
        password=my_args.password
    )

    log.info("MySql Connection Successful - {}".format(mydb))

    rollback(my_args.date)
    log.info("Done Processing")

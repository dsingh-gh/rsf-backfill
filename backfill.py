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

    parser.add_argument('-from', '--fromDate', metavar='From date for which backfill runs',
                        help='From date for which backfill runs in yyyy-mm-dd format', default=None)

    parser.add_argument('-to', '--toDate', metavar='To date for which backfill runs',
                        help='To date for which backfill runs in yyyy-mm-dd format', default=None)

    parser.add_argument('-bs', '--batchSize', metavar='size for MySql insert batch, defaulted to 2500',
                        help='size for MySql insert batch, defaulted to 2500', default=2500, type=int)

    parser.add_argument('-sp', '--sleepPeriod', metavar='sleep period b/w batches defaulted to 5 secs',
                        help='sleep period', default=5, type=int)

    return parser.parse_args()


def backfill(date, batch_size):
    #batch_num = 1
    #completed = 0

    fee_file, tax_file, adj_file = get_files_for_date(date)

    log.info("Reading {}, {}, {}".format(fee_file, tax_file, adj_file))
    service_fee_tax_by_transaction_id_dict = map_service_fee_tax_by_transaction_id(tax_file)
    adjustments_by_order_id_dict = map_adjustments_by_order_id(adj_file)
    log.info("Done Reading files")

    process_service_fee_file_v2(fee_file, service_fee_tax_by_transaction_id_dict, adjustments_by_order_id_dict, batch_size, date)

    # while completed == 0:
    #     log.info("Starting batch {}".format(batch_num))
    #     start = datetime.datetime.now()
    #     line_item_ids = process_service_fee_file(fee_file, service_fee_tax_by_transaction_id_dict, adjustments_by_order_id_dict, batch_num, batch_size)
    #     if len(line_item_ids) == 0:
    #         completed = 1
    #     else:
    #         create_rollback(line_item_ids, date, batch_num)
    #         # mydb.commit()
    #         time_taken = (datetime.datetime.now() - start).seconds
    #         log.info("Processed batch {} in {} secs".format(batch_num, time_taken))
    #         sleep_for = 1 if time_taken == 0 else time_taken * 2
    #         log.info("sleeping for {} secs".format(sleep_for))
    #         time.sleep(sleep_for)
    #         batch_num = batch_num + 1


def process_service_fee_file_v2(fee_file, service_fee_tax_by_transaction_id_dict, adjustments_by_order_id_dict, batch_size, date):
    log.info("Enter process_service_fee_file")
    batch_line_num = 0
    batch_num = 1
    f2 = open(fee_file)
    reader = csv.reader(f2, dialect='excel')
    insert_params = []
    try:
        for row in reader:
            if batch_line_num <= batch_size:
                batch_line_num = batch_line_num + 1
                process_row(adjustments_by_order_id_dict, insert_params, row, service_fee_tax_by_transaction_id_dict)
            else:
                process_row(adjustments_by_order_id_dict, insert_params, row, service_fee_tax_by_transaction_id_dict)
                line_item_ids = insert_batch(insert_params, batch_num)
                create_rollback(line_item_ids, date, batch_num)
                batch_line_num = 0
                insert_params = []
                batch_num = batch_num + 1

        # last batch
        if len(insert_params) > 0:
            line_item_ids = insert_batch(insert_params, batch_num)
            create_rollback(line_item_ids, date, batch_num)

    except Exception as e:
        log.error('Could not read fee File: {}. Exiting!!\n{}'.format(file, e))
        sys.exit(1)
    finally:
        f2.close()

    log.info("Exit process_service_fee_file")


def process_row(adjustments_by_order_id_dict, insert_params, row, service_fee_tax_by_transaction_id_dict):
    insert_params.append((row[2], "RESTAURANT_SERVICE_FEE", row[5]))
    if row[2] in service_fee_tax_by_transaction_id_dict.keys():
        rsf_tax_row = service_fee_tax_by_transaction_id_dict[row[2]]
        insert_params.append((rsf_tax_row[2], "RESTAURANT_SERVICE_FEE_TAX", rsf_tax_row[5]))
    if row[1] in adjustments_by_order_id_dict.keys():
        service_fee_tax_row = service_fee_tax_by_transaction_id_dict[row[2]] if (
                    row[2] in service_fee_tax_by_transaction_id_dict.keys()) else None
        insert_refund_line_items(insert_params, row, service_fee_tax_row, adjustments_by_order_id_dict[row[1]])


def process_service_fee_file(fee_file, service_fee_tax_by_transaction_id_dict, adjustments_by_order_id_dict, batch_num, batch_size):
    log.info("Enter process_service_fee_file")
    line_num = 1
    f2 = open(fee_file)
    reader = csv.reader(f2, dialect='excel')
    try:
        insert_params = []
        for row in reader:
            if (batch_num - 1) * batch_size < line_num <= batch_num * batch_size:
                #insert(line_item_ids, row, "RESTAURANT_SERVICE_FEE")
                insert_params.append((row[2], "RESTAURANT_SERVICE_FEE", row[5]))
                if row[2] in service_fee_tax_by_transaction_id_dict.keys():
                    #insert(line_item_ids, service_fee_tax_by_transaction_id_dict[row[2]], "RESTAURANT_SERVICE_FEE_TAX")
                    rsf_tax_row = service_fee_tax_by_transaction_id_dict[row[2]]
                    insert_params.append((rsf_tax_row[2], "RESTAURANT_SERVICE_FEE_TAX", rsf_tax_row[5]))
                if row[1] in adjustments_by_order_id_dict.keys():
                    service_fee_tax_row = service_fee_tax_by_transaction_id_dict[row[2]] if (row[2] in service_fee_tax_by_transaction_id_dict.keys()) else None
                    #insert_line_items_for_service_fee_refund(line_item_ids, row, service_fee_tax_row, adjustments_by_order_id_dict[row[1]])
                    insert_refund_line_items(insert_params, row, service_fee_tax_row, adjustments_by_order_id_dict[row[1]])
            elif line_num > batch_num * batch_size:
                break

            line_num = line_num + 1
    except Exception as e:
        log.error('Could not read fee File: {}. Exiting!!\n{}'.format(file, e))
        sys.exit(1)
    finally:
        f2.close()

    line_item_ids = insert_batch(insert_params, batch_num)
    log.info("Exit process_service_fee_file")
    return line_item_ids


def insert_batch(insert_params, batch_num):
    line_item_ids = set()
    if len(insert_params) > 0:
        log.info("Inserting batch {}".format(batch_num))
        sql = "INSERT INTO grubhub.transaction_line_item (transaction_id, line_item_type, amount) " \
              "VALUES (%s, %s, %s)"

        cursor = mydb.cursor(prepared=True)
        cursor.executemany(sql, insert_params)
        mydb.commit()

        row_count = cursor.rowcount
        last_id = cursor.lastrowid
        first_id = last_id - row_count + 1

        log.info("Inserted batch {}, row count {} and last Id {}".format(batch_num, row_count, last_id))

        while first_id <= last_id:
            line_item_ids.add(first_id)
            first_id = first_id + 1

    log.info("Sleeping for {} secs".format(sleepPeriod))
    time.sleep(sleepPeriod)
    return line_item_ids


def get_files_for_date(date):
    tax_file = None
    fee_file = None
    adj_file = None

    files = fnmatch.filter(os.listdir('data'), '*' + date + ".csv")
    for fileName in files:
        if fileName.find("tax") > -1:
            tax_file = "data/" + fileName
        elif fileName.find("adjustments") > -1:
            adj_file = "data/" + fileName
        else:
            fee_file = "data/" + fileName

    if fee_file is None or tax_file is None:
        log.error("Fee or Tax file not found for date " + date)
        sys.exit(1)

    return fee_file, tax_file, adj_file


def map_service_fee_tax_by_transaction_id(tax_file):
    service_fee_tax_by_transaction_id_dict = {}

    f = open(tax_file)
    reader = csv.reader(f, dialect='excel')
    try:
        for row in reader:
            service_fee_tax_by_transaction_id_dict[row[2]] = row

    except Exception as e:
        log.error('Could not read tax File: {}. Exiting!!\n{}'.format(file, e))
        sys.exit(1)
    finally:
        f.close()

    return service_fee_tax_by_transaction_id_dict


def map_adjustments_by_order_id(adj_file):
    adjustments_by_order_id_dict = {}

    if adj_file is not None:
        f = open(adj_file)
        reader = csv.reader(f, dialect='excel')
        try:
            for row in reader:
                adjustments_by_order_id_dict[row[1]] = row

        except Exception as e:
            log.error('Could not read adjustments File: {}. Exiting!!\n{}'.format(file, e))
            sys.exit(1)
        finally:
            f.close()

    return adjustments_by_order_id_dict


def insert(item_ids, row, line_item_type):
    try:
        sql = "INSERT INTO grubhub.transaction_line_item (transaction_id, line_item_type, amount) " \
              "VALUES (" + row[2] + ", '" + line_item_type + "', " + row[5] + ")"
        cursor = mydb.cursor()
        cursor.execute(sql)
    except Exception as e:
        log.error("Insert sql failed - "+sql)
        log.error(e.message)
        sys.exit(1)

    item_ids.add(cursor.lastrowid)

def insert_refund_line_items(insert_params, service_fee_row, service_fee_tax_row, adjustment_row):
    # if net_amount on original & adjustment transaction are same
    if service_fee_row[3] == adjustment_row[3]:
        try:
            insert_params.append((adjustment_row[2], "RESTAURANT_SERVICE_FEE", service_fee_row[5]))
            log.info("Inserted refund for transaction Id {}".format(adjustment_row[2]))

            if service_fee_tax_row is not None:
                insert_params.append((adjustment_row[2], "RESTAURANT_SERVICE_FEE_TAX", service_fee_tax_row[5]))

        except Exception as e:
            log.error(e.message)
            sys.exit(1)


def insert_line_items_for_service_fee_refund(item_ids, service_fee_row, service_fee_tax_row, adjustment_row):

    # if net_amount on original & adjustment transaction are same
    if service_fee_row[3] == adjustment_row[3]:
        try:
            cursor = mydb.cursor()

            sql1 = "INSERT INTO grubhub.transaction_line_item (transaction_id, line_item_type, amount) " \
                  "VALUES (" + adjustment_row[2] + ", 'RESTAURANT_SERVICE_FEE', " + service_fee_row[5] + ")"
            cursor.execute(sql1)
            log.info("Inserted refund transaction {}".format(sql1))
            item_ids.add(cursor.lastrowid)

            if service_fee_tax_row is not None:
                sql2 = "INSERT INTO grubhub.transaction_line_item (transaction_id, line_item_type, amount) " \
                       "VALUES (" + adjustment_row[2] + ", 'RESTAURANT_SERVICE_FEE_TAX', " + service_fee_tax_row[5] + ")"
                cursor.execute(sql2)
                item_ids.add(cursor.lastrowid)

        except Exception as e:
            log.error("Insert sql for refund line items failed - "+sql1+" & "+sql2)
            log.error(e.message)
            sys.exit(1)


def create_rollback(item_ids, date, batch_num):
    log.info("Enter create_rollback")
    log.info("Creating rollback file for batch {}".format(batch_num))
    file_name = "rollback/" + date + "-{}.csv".format(batch_num)
    with open(file_name, 'w') as rollback_file:
        writer = csv.writer(rollback_file)

        for item_id in item_ids:
            delete_sql = "DELETE FROM grubhub.transaction_line_item WHERE transaction_line_item_id = {}".format(
                item_id)
            writer.writerow([delete_sql])

    rollback_file.close()
    log.info("Exit create_rollback")


def validate_date(date):
    f = open("status.csv")
    reader = csv.reader(f, dialect='excel')
    try:
        for row in reader:
            if row[0] == date:
                log.error(
                    "Script already executed for {}, to re-run make sure to first rollback for this date and clear this date from status.csv file".format(
                        date))
                log.info("Rollback cmd: python3 ./rollback.py --date {}".format(date))
                sys.exit(1)
    finally:
        f.close()

    with open("status.csv", 'a') as status_file:
        writer = csv.writer(status_file)
        writer.writerow([date])
    status_file.close()


if __name__ == '__main__':
    log = setup_logger()
    my_args = process_args()

    mydb = mysql.connector.connect(
        host=my_args.host,
        user=my_args.user,
        password=my_args.password
    )

    log.info("MySql Connection Successful - {}".format(mydb))
    log.info("Using batch Size {}".format(my_args.batchSize))

    fromDate = datetime.datetime.strptime(my_args.fromDate, '%Y-%m-%d').date()
    toDate = datetime.datetime.strptime(my_args.toDate, '%Y-%m-%d').date()
    sleepPeriod = my_args.sleepPeriod

    while fromDate <= toDate:
        cmd = input("Do you want to continue for {} ? ".format(fromDate))
        if cmd == 'y':
            log.info("Processing orders for {}".format(fromDate))
            validate_date(fromDate.strftime("%Y-%m-%d"))
            backfill(fromDate.strftime("%Y-%m-%d"), my_args.batchSize)
            fromDate = fromDate + datetime.timedelta(days=1)
        else:
            break

    log.info("Done Processing")

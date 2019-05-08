import csv
import logging
import argparse
from itertools import groupby

# awk -F ',' '{ print >> ("data/service_fee-" $1 ".csv"); close("data/service_fee-" $1 ".csv") }' prod\ data/service_fee.csv                                                     (master✱)
# awk -F ',' '{ print >> ("data/service_fee_tax-" $1 ".csv"); close("data/service_fee_tax-" $1 ".csv") }' prod\ data/service_fee_tax.csv                                         (master✱)
# awk -F ',' '{ print >> ("data/adjustments-" $1 ".csv"); close("data/adjustments-" $1 ".csv") }' prod\ data/adjustments.csv
# awk '!seen[$0]++' adjustments.csv > adjustments_fix.csv


def setup_logger():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    # logger.setLevel(logging.DEBUG)

    return logger


def split_file(file):
    i = 1
    for key, rows in groupby(csv.reader(open(file)), lambda row: row[0]):
        file_name = "batches/service_fee_{}".format(i)
        with open(file_name, 'w') as split_file:
            writer = csv.writer(split_file)
            i = i + 1
            for row in rows:
                writer.writerow(row)
        split_file.close()


def process_args():
    parser = argparse.ArgumentParser(description='location of csv files.')
    parser.add_argument('-p', '--serviceFeeFile', metavar='file containing service fee records',
                        help='Service fee file', default=None)

    return parser.parse_args()


if __name__ == '__main__':
    log = setup_logger()

    my_args = process_args()
    service_fee_file = my_args.serviceFeeFile

    split_file(service_fee_file)
    log.info("Processing Done")

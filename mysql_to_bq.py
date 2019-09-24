import MySQLdb
from google.cloud import bigquery
import mysql.connector
import logging
import os
from MySQLdb.converters import conversions
import click
import MySQLdb.cursors
from google.cloud.exceptions import ServiceUnavailable
import sys

bqTypeDict = {'int': 'INTEGER',
              'varchar': 'STRING',
              'double': 'FLOAT',
              'tinyint': 'INTEGER',
              'decimal': 'FLOAT',
              'text': 'STRING',
              'smallint': 'INTEGER',
              'char': 'STRING',
              'bigint': 'INTEGER',
              'float': 'FLOAT',
              'longtext': 'STRING',
              'datetime': 'TIMESTAMP'
              }


def conv_date_to_timestamp(str_date):
    import time
    import datetime

    date_time = MySQLdb.times.DateTime_or_None(str_date)
    unix_timestamp = (date_time - datetime.datetime(1970, 1, 1)).total_seconds()

    return unix_timestamp


def Connect(host, database, user, password):
    return mysql.connector.connect(host='tempus-qa.hashmapinc.com',
                                    port='30656',
                                    database='recommendation_spark',
                                    user='root',
                                    password='docker')


def BuildSchema(host, database, user, password, table):
    logging.debug('build schema for table %s in database %s' % (table, database))
    conn = Connect(host, database, user, password)
    cursor = conn.cursor()
    cursor.execute("DESCRIBE %s;" % table)

    tableDecorator = cursor.fetchall()
    schema = []

    for col in tableDecorator:
        colType = col[1].split("(")[0]
        if colType not in bqTypeDict:
            logging.warning("Unknown type detected, using string: %s", str(col[1]))

        field_mode = "NULLABLE" if col[2] == "YES" else "REQUIRED"
        field = bigquery.SchemaField(col[0], bqTypeDict.get(colType, "STRING"), mode=field_mode)

        schema.append(field)

    return tuple(schema)


def bq_load(table, data, max_retries=5):
    logging.info("Sending request")
    uploaded_successfully = False
    num_tries = 0

    while not uploaded_successfully and num_tries < max_retries:
        try:
            insertResponse = table.insert_data(data)

            for row in insertResponse:
                if 'errors' in row:
                    logging.error('not able to upload data: %s', row['errors'])

            uploaded_successfully = True
        except ServiceUnavailable as e:
            num_tries += 1
            logging.error('insert failed with exception trying again retry %d', num_tries)
        except Exception as e:
            num_tries += 1
            logging.error('not able to upload data: %s', str(e))


@click.command()
@click.option('-h', '--host', default='tempus-qa.hashmapinc.com', help='MySQL hostname')
@click.option('-d', '--database', required=True, help='MySQL database')
@click.option('-u', '--user', default='root', help='MySQL user')
@click.option('-p', '--password', default='docker', help='MySQL password')
@click.option('-t', '--table', required=True, help='MySQL table')
@click.option('-i', '--projectid', required=True, help='Google BigQuery Project ID')
@click.option('-n', '--dataset', required=True, help='Google BigQuery Dataset name')
@click.option('-l', '--limit', default=0, help='max num of rows to load')
@click.option('-s', '--batch_size', default=1000, help='max num of rows to load')
@click.option('-k', '--key', default='key.json',help='Location of google service account key (relative to current working dir)')
@click.option('-v', '--verbose', default=0, count=True, help='verbose')
def SQLToBQBatch(host, database, user, password, table, projectid, dataset, limit, batch_size, key, verbose):
    # set to max verbose level
    verbose = verbose if verbose < 3 else 3
    loglevel = logging.ERROR - (10 * verbose)

    logging.basicConfig(level=loglevel)

    logging.info("Starting SQLToBQBatch. Got: Table: %s, Limit: %i", table, limit)
    ## set env key to authenticate application

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.getcwd(), key)
    print('file found')
    # Instantiates a client
    bigquery_client = bigquery.Client()
    print('Project id created')

    try:

        bq_dataset = bigquery_client.dataset(dataset)
        bq_dataset.create()
        logging.info("Added Dataset")
    except Exception as e:
        if ("Already Exists: " in str(e)):
            logging.info("Dataset already exists")
        else:
            logging.error("Error creating dataset: %s Error", str(e))



    bq_table = bq_dataset.table(table)
    bq_table.schema = BuildSchema(host, database, user, password, table)
    print('Creating schema using build schema')
    bq_table.create()
    logging.info("Added Table %s", table)

    conn = Connect(host, database, user, password)
    cursor = conn.cursor()

    logging.info("Starting load loop")
    cursor.execute("SELECT * FROM %s" % (table))

    cur_batch = []
    count = 0

    for row in cursor:
        count += 1

        if limit != 0 and count >= limit:
            logging.info("limit of %d rows reached", limit)
            break

        cur_batch.append(row)

        if count % batch_size == 0 and count != 0:
            bq_load(bq_table, cur_batch)

            cur_batch = []
            logging.info("processed %i rows", count)

    # send last elements
    bq_load(bq_table, cur_batch)
    logging.info("Finished (%i total)", count)
    print("table created")


if __name__ == '__main__':
    # run the command
    SQLToBQBatch()
    # python mysql_to_bq.py -d 'recommendation_spark' -t temp_market_store -i 'inductive-cocoa-250507' -n 'inductive-cocoa-250507:practice123' -k key.json

# -*- coding: UTF-8 -*-

"""
title: csv2mongodb.py
description: A quick ( and arguably dirty ) bit of code to parse a csv file
and import it into a mongodb collection. Could use a lot of improvment! Always
open to suggestions and *contructive* feedback.
author: Frank I. Emmons
email: frank.i.emmons@gmail.com
version: 0.01
usage: python csv2mongodb.py <options>
python_version: 2.7.11
"""

# Import the required modules
import csv,json,sys,os,argparse,re,pymongo,time
from collections import OrderedDict

# set up our argument parser
parser = argparse.ArgumentParser(description = 'Converts a given csv file and '
                                 'converts into json ready to be loaded into '
                                 'mongodb' )
parser.add_argument('--csv', required=True, \
                    help = 'The full path of the csv file to be loaded.')
parser.add_argument('--with-header', required=True, \
                    help = 'Comma separated list of header columns.')
parser.add_argument('--database', required=True, \
                    help = 'The mongo database to add these records to.')
parser.add_argument('--collection', required=True, \
                    help = 'The collection to add records to.')
parser.add_argument('--field-delimiter', \
                    help = 'The field delimiter in the csv. Will default to'
                    'comma.')
parser.add_argument('--host', \
                    help = 'Hostname or ip address of the mondodb server.')
parser.add_argument('--port', \
                    help = 'Port that the mongodb server is listening on.')
parser.add_argument('--user', help = 'Mongodb username.')
parser.add_argument('--password', help = 'Mongodb password.')


def get_conn_vals(args):

    """
    Parses given arguments to then return  connection value string that can be
    used to connect to mongodb with.
    Args:
        args(obj): object returned by parser.parse_args()
    """

    vals_string = 'mongodb://'
    vals = {
        'host': args.host,
        'database': args.database
    }

    if args.user:
        vals['username'] = args.user
    if args.password:
        vals['password'] = args.password
    if args.port:
        vals['port'] = args.port

    if args.user and args.password:
        vals_string += '{user}:{password}@'

    vals_string += '{host}/{database}'

    if args.port:
        vals_string += ':{port}'

    return vals_string.format(**vals)

# parsing our command line arguments
args = parser.parse_args()

# Initializing some default vars
conn = None
headers = None
delim = ','

# Setting what the csv field delimiter should be, if it was provided
if args.field_delimiter:
    delim = args.field_delimiter

# Split up the comma separated headers
if args.with_header:
    headers = args.with_header.split(',')

# attempt to connect to mongodb - if there was a problem, we should just exit
try:
    conn = pymongo.MongoClient(get_conn_vals(args)) # connection
    db = conn[args.database] # database
    collection = db[args.collection] # collection
except Exception as e:
    print "Error:",e
    sys.exit(0)

# Opening the csv, loop through the records and inserting them into mongodb
with open(args.csv, 'r') as fh:
    fhreader = csv.reader(fh, delimiter=delim)
    for row in fhreader:
        # If the record we have just read is not the headers row,
        # then insert it into the collection.
        if headers and cmp( row, headers ) < 0:
            # turn row into a dictionary
            record = OrderedDict( zip( headers, row ) )
            # attempt to insert the record now
            try:
                collection.insert(record)
            except pymongo.errors.AutoReconnect, e:
                print 'Warning', e
                time.sleep(3)
            except pymongo.errors.DuplicateKeyError:
                # already have this record - no need to duplicate it!
                #Let's just ...
                break
    else:
        raise Exception('Could not loop through csv data! '
                        'At the end or empty file?')
        sys.exit(0)

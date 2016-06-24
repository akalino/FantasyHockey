"""
Project Name: WebScrapers 
@author: alkal 
Created on: 6/5/16 at  12:04 PM 
"""

import csv
import datetime
import argparse
import subprocess


def create_dates(_start, _n):
    _dl = []
    for i in range(_n):
        start = datetime.datetime.strptime(_start, '%Y-%m-%d')
        d = start - datetime.timedelta(days=i)
        d = d.strftime('%Y-%m-%d')
        _dl.append(d)
    return _dl


def attempt_scrape(_in_list, _out_list):
    for d in _in_list:
        print '*** Attempting to process for {d} ***'.format(d=d)
        try:
            subprocess.call('/home/alkal/anaconda/bin/python '
                            '/home/alkal/Documents/FantasyHockey/WebScrapers/backpopulate_daily_scores.py'
                            ' -d {t}'.format(t=d), shell=True)
            # Failed sites are written to a file for cleanup job to be run later
            _in_list.remove(d)
        except:
            _out_list.append(d)

    return _out_list


if __name__ == "__main__":
    # Put the argparser here
    parser = argparse.ArgumentParser(prog='backfill',
                                     description='Pulls in stats for a given day')
    parser.add_argument('-s', '--start', nargs='?', type=str, required=True,
                        help='The start date to pull stats for',
                        dest='start')
    parser.add_argument('-n', '--ndays', nargs='?', type=int, required=True,
                        help='The number of days forward from start date to populate',
                        dest='n')
    args = parser.parse_args()
    # Create the list of dates that need to be backfilled
    date_list = create_dates(args.start, args.n)
    out = []
    output = attempt_scrape(date_list, out)
    print('*** Finished backfill for {st} and prior {n} days ***'.format(st=args.start, n=args.n))

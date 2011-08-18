#!/usr/bin/env python
'''
Created on Jun 22, 2011

Script for checking PostgreSQL 9 streaming replication lag.
Contains Checker class, which perform all checking routine.
If used as script, provides command line interface (use turnipp.py -h).

Requires psycopg2 (easy_install psycopg2)

MIT licensed:

Copyright (c) 2011 Lev Orekhov. All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''
import psycopg2
import argparse

__version__ = '1.0'

class Checker(object):
    '''
    Checks for replication lag between two postgresql 9.0 server.
    
    @ivar master: connection object to master server
    @ivar slave: connection object to slave server  
    '''
    def __init__(self, m, s):
        '''
        @param m: connection string to master ("host=foo user=bar")
        @param s: connection string to slave ("host=foo user=bar")
        '''
        self.master = m
        self.slave = s
    
    def check(self):
        '''
        Checks for lag.
        If all is OK, lag in Kb returns, if all is VERY BAD, -1 returns.
        '''
        try:
            m_con = psycopg2.connect(self.master)
            s_con = psycopg2.connect(self.slave)
            m = m_con.cursor()
            m.execute('SELECT pg_current_xlog_location()')
            m_current = self.__in_bytes(m.fetchone()[0])
            m_con.commit()
            m_con.close()
            
            s = s_con.cursor()
            s.execute('SELECT pg_last_xlog_replay_location()')
            s_current = self.__in_bytes(s.fetchone()[0])
            s_con.commit()
            s_con.close()
        except:
            return -1
        return (m_current - s_current) / 1024
    
    def __in_bytes(self, xlog):
        logid, offset = xlog.split('/')
        return (int('ffffffff', 16) * int(logid, 16)) + int(offset, 16)
                                                                
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check replication.')
    parser.add_argument('-m', '--master', metavar='DSN',
                        help='Master server DSN (e.g. "host=foo user=bar")')
    parser.add_argument('-s', '--slave', metavar='DSN',
                        help='Slave server DSN (e.g. "host=foo user=bar")')
    
    args = parser.parse_args()
    checker = Checker(args.master, args.slave)
    print checker.check()


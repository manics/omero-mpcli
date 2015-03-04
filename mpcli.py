#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright (C) 2015 University of Dundee & Open Microscopy Environment.
# All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Client side parallel OMERO commands.
"""

import argparse
import datetime
import getpass
import logging
import multiprocessing
import pickle

import omero
import omero.cli
import omero.gateway


log = logging.getLogger('mpomero')
log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)


class MpOmeroCliException(Exception):

    def __init__(self, msg):
        super(MpOmeroCliException, self).__init__(msg)


class MpOmeroCli(object):

    def __init__(self, host=None, port=None, user=None, password=None,
                 sessionid=None, groupid=-1, detach=True):
        if not host:
            host = 'localhost'
        if not port:
            port = 4064
        if not sessionid:
            if not user:
                user = raw_input('User: ')
            if not password:
                password = getpass.getpass()

        self.client = omero.client(host, port)
        if sessionid:
            self.session = self.client.joinSession(sessionid)
            log.info('Joined session as: %s', self.session)
        else:
            self.session = self.client.createSession(user, password)
            log.info('Created session: %s', self.session)
        self.client.enableKeepAlive(60)
        self.conn = omero.gateway.BlitzGateway(client_obj=self.client)
        self.conn.SERVICE_OPTS.setOmeroGroup(groupid)
        self.detach = detach

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self.detach:
            try:
                log.info('Detaching session: %s', self.session)
                self.session.detachOnDestroy()
            except Exception as e:
                log.error(e)
        else:
            log.info('Closing session: %s', self.session)
        self.client.closeSession()


def get_params_list(opts, common, params, groupsize):
    for i in xrange(0, len(params), groupsize):
        yield (opts, common, params[i:i + groupsize])


def invokecli(args, dryrun=False):
    opts, common, params = args
    cmdline = common[:1]
    if opts['login']:
        cmdline += ['-s', opts['host'], '-p', opts['port'], '-k',
                    opts['session']]
    cmdline += common[1:] + params

    if dryrun:
        log.info('CLI (dryrun): %s', cmdline)
        return

    cli = omero.cli.CLI()
    cli.loadplugins()

    tries = opts['tries']
    for n in xrange(tries):
        log.info('CLI (attempt %d): %s', n, cmdline)
        cli.invoke(cmdline)
        if cli.rv == 0:
            return 0
    return [cli.rv]


def runscript(args, mode='process', dryrun=False):
    opts, common, params = args
    scriptname = common[0]
    commonargs = common[1:]

    log.info('Loading script: %s', scriptname)
    env = {}
    execfile(scriptname, env)

    with MpOmeroCli(opts['host'], int(opts['port']),
                    sessionid=opts['session']) as c:

        if mode == 'get':
            log.info('Running get()')
            rs = env['get'](c.client, commonargs)
            log.info('Script (get): %d results', len(rs))
            return rs

        if dryrun:
            log.info('Script (dryrun): %s %s', commonargs, params)
            return

        rs = env['process'](c.client, commonargs, params)
        return rs


def main(args, common, params):
    logging.basicConfig(format='%(asctime)s %(levelname)-5.5s %(message)s')

    log.info(args)

    out = 'out-%s.pkl' % (datetime.datetime.strftime(
        datetime.datetime.now(), '%Y%m%d-%H%M%S'))

    with MpOmeroCli(args.server, args.port, args.user, password=args.password,
                    detach=False) as c:
        sessionid = c.client.getSessionId()
        log.info('common: %s', common)

        opts = {}
        opts['host'] = c.client.getProperty('omero.host')
        opts['port'] = c.client.getProperty('omero.port')
        opts['session'] = sessionid
        opts['tries'] = args.tries
        opts['login'] = args.login

        if args.mode == 'cli':
            paramslist = get_params_list(opts, common, params, args.groupsize)
            func = invokecli
        else:
            assert not params
            params = runscript((opts, common, []), 'get')
            paramslist = get_params_list(opts, common, params, args.groupsize)
            func = runscript

        if args.dry_run:
            for ocp in paramslist:
                func(ocp, dryrun=True)
            return

        log.info('Creating pool of %d threads', args.threads)
        pool = multiprocessing.Pool(args.threads)
        results = []
        for r in pool.imap(func, paramslist):
            results.extend(r)

        log.info('Saving results to: %s', out)
        with open(out, 'wb') as f:
            pickle.dump(results, f)
        log.info('pool.map results length: [%d]', len(results))

    log.info('Main thread exiting')


def parse_args(args=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--server', default=None)
    parser.add_argument('--port', default=None, type=int)
    parser.add_argument('--user', default=None)
    parser.add_argument('--group', default=None, type=int)
    parser.add_argument('--password', default=None)

    parser.add_argument(
        '--threads', default=multiprocessing.cpu_count(), type=int)
    parser.add_argument('--tries', default=1, type=int,
                        help='Retry on failure')
    parser.add_argument('--groupsize', default=1, type=int,
                        help='Pass mapargs in groups of this size')
    parser.add_argument('--login', action='store_true',
                        help='Include session login arguments in command')

    parser.add_argument('-n', '--dry-run', action='store_true')

    parser.add_argument('mode', choices=['cli', 'script'],
                        help='Invoke an OMERO CLI command, or run a script')
    parser.add_argument('other', nargs=argparse.REMAINDER, help=(
        'CLI or script arguments in the form'
        '"arg1 arg2 ... -- maparg1 maparg2 ..."'))

    args = parser.parse_args(args)
    common = []
    params = []

    gotsep = False
    for arg in args.other:
        if gotsep:
            params.append(arg)
        elif arg == '--':
            gotsep = True
        else:
            common.append(arg)

    return args, common, params


if __name__ == '__main__':
    # Don't run if called inside ipython
    try:
        __IPYTHON__
    except NameError:
        allargs = parse_args()
        main(*allargs)

# Example:
# mpcli.py --server server --user user --password password --groupsize 3 \
#   --login --tries 3 cli import -d 1 -- a.img b.img c.img ...

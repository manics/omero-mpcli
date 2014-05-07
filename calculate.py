#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright (C) 2014 University of Dundee & Open Microscopy Environment.
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
Client side calculation of features, stored locally.
Uses multiprocessing to parallelise calculations on the local machine.
"""

import argparse
import datetime
import errno
import getpass
import logging
import multiprocessing
import numpy
import os
import pickle

import omero
import omero.gateway
import portalocker

import pychrm
from pychrm.FeatureSet import Signatures
from pychrm.PyImageMatrix import PyImageMatrix


log = logging.getLogger('MultiCalc')
log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)


class CalculationException(Exception):

    def __init__(self, msg):
        super(CalculationException, self).__init__(msg)


class FeatureFileAlreadyExists(CalculationException):
    pass


class Calculator(object):

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
        else:
            self.session = self.client.createSession(user, password)
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

    def imageGenerator(self, objs):
        for o in objs:
            log.info(o)
            if isinstance(o, omero.gateway._ImageWrapper):
                yield o
            else:
                children = o.listChildren()
                for im in self.imageGenerator(children):
                    yield im

    def genObjects(self, typeids):
        for (t, i) in typeids:
            o = self.conn.getObject(t, i)
            if not o:
                raise CalculationException(
                    'Unable to get object: %s %d' % (t, i))
            yield o

    def genComputationList(self, typeids):
        """
        typeids: A list of (TypeName, Id) pairs where TypeName is Image,
        Dataset or Project
        """
        def planeParamsGen(im):
            for c in xrange(im.getSizeC()):
                for z in xrange(im.getSizeZ()):
                    for t in xrange(im.getSizeT()):
                        # yield (im.id, c, z, t, im.getSizeX(), im.getSizeY())
                        yield (im.id, c, z, t)

        objGen = self.genObjects(typeids)
        for im in self.imageGenerator(objGen):
            for params in planeParamsGen(im):
                yield params


def extractFeaturesPychrmSmall(conn, iid, c, z, t):
    """
    Calculate features for a single image plane. Note this takes in an
    image id instead of an image object to support batch jobs where a list
    of parameter sets can be provided.
    """
    im = conn.getObject('Image', iid)
    if not im:
        raise CalculationException('Image id not found: %d' % iid)

    # Calculate features for a single plane (c/z/t)
    pychrm_matrix = PyImageMatrix()
    pychrm_matrix.allocate(im.getSizeX(), im.getSizeY())
    numpy_matrix = pychrm_matrix.as_ndarray()

    numpy_matrix[:] = im.getPrimaryPixels().getPlane(
        theZ=z, theC=c, theT=t)
    feature_plan = pychrm.StdFeatureComputationPlans.getFeatureSet()
    options = ""  # Wnd-charm options
    fts = Signatures.NewFromFeatureComputationPlan(
        pychrm_matrix, feature_plan, options)

    ft = {
        'names': fts.names,
        'values': fts.values,
        'version': fts.version
    }
    return ft


def meanIntensity(conn, iid, c, z, t):
    """
    Calculate features for a single image plane. Note this takes in an
    image id instead of an image object to support batch jobs where a list
    of parameter sets can be provided.
    """
    im = conn.getObject('Image', iid)
    if not im:
        raise CalculationException('Image id not found: %d' % iid)

    # Calculate features for a single plane (c/z/t)
    m = im.getPrimaryPixels().getPlane(theZ=z, theC=c, theT=t)
    ft = {
        'names': ['min', 'max', 'mean'],
        'values': [m.min(), m.max(), m.mean()],
        'version': '0'
    }
    return ft


class FeatureFile(object):
    """
    A context manager to handle multiple concurrent feature calculations, and
    saving features.

    On entering the context manager an exclusive lockfile named after the image
    parameters is created.
    Features can be written to this file.
    On exit the lockfile is renamed to the final output filename.
    """

    def __init__(self, iid, c, z, t):
        self.dir = 'SmallFeatureSet'
        self.dir = os.path.join(self.dir, 'image%08d' % iid)
        self.filename = 'image%08d-c%04d-z%04d-t%04d' % (iid, c, z, t)
        self.npy = os.path.join(self.dir, self.filename + '.npy')
        self.saved = False

        try:
            os.makedirs(self.dir)
        except OSError as e:
            if e.errno != errno.EEXIST or not os.path.isdir(self.dir):
                raise

    def __enter__(self):
        """
        Succeeds if an exclusive lock was obtained and the final output file
        does not exist or is empty, otherwise raises an exception.
        """
        lock = os.path.join(self.dir, self.filename + '.tmp')
        log.debug('Locking: %s', lock)
        self.fh = open(lock, 'w+')
        try:
            portalocker.lock(self.fh, portalocker.LOCK_EX)
            # Check whether the final output file has already been created
            try:
                log.debug('Checking: %s', self.npy)
                # Using with open seems to trigger an ipython bug
                # with open(self.npy, 'r') as f:
                f = open(self.npy, 'r')
                try:
                    f.seek(0, 2)
                    if f.tell() > 0:
                        raise FeatureFileAlreadyExists(
                            'Feature file already exists: %s (%d B)' % (
                                self.npy, f.tell()))
                finally:
                    f.close()
            except IOError:
                # Assume this is file not found
                pass
            except:
                os.unlink(self.fh.name)
                raise
        except:
            self.fh.close()
            raise
        return self

    def save(self, a):
        log.debug('Saving: %s', self.fh.name)
        numpy.save(self.fh, a)
        self.saved = True

    def __exit__(self, type, value, traceback):
        if self.saved:
            log.debug('Renaming: %s->%s', self.fh.name, self.npy)
            os.rename(self.fh.name, self.npy)
            log.info('Saved: %s', self.npy)
        else:
            log.debug('Empty, deleting: %s', self.fh.name)
            os.unlink(self.fh.name)
        self.fh.close()


class FeatureFileZ(FeatureFile):
    """
    FeatureFile that saves to numpy archives instead of single arrays
    """
    def __init__(self, iid, c, z, t):
        super(FeatureFileZ, self).__init__(iid, c, z, t)
        self.npy = os.path.join(self.dir, self.filename + '.npz')

    def save(self, **kwargs):
        log.debug('Saving: %s', self.fh.name)
        numpy.savez(self.fh, **kwargs)
        self.saved = True


# calculate = meanIntensity
calculate = extractFeaturesPychrmSmall
"""
calculate should be a function that calculates features given a single image
plane.
Note this takes in an image id instead of an image object to support batch
jobs where a list of parameter sets can be provided.

Function of the form r = func(conn, iid, c, z, t)
  conn: BlitzGateway object
  iid: Image ID
  c, z, t: Index of the C/Z/T plance
  r: A dict with fields names ([string]), values ([double]), and
    version (string)
"""


def run1(params):
    log.info('params: %s', params)
    try:
        ftparams = params.pop('ftparams')
        with Calculator(**params) as c:
            # with FeatureFile(*ftparams) as ff:
            with FeatureFileZ(*ftparams) as ff:
                log.info('Calculating features')
                feats = calculate(c.conn, *ftparams)
                log.debug(feats)
                # ff.save(feats['values'])
                ff.save(**feats)
                return 'Completed: %s' % str(ftparams)
    except FeatureFileAlreadyExists as e:
        log.error(e)
        return 'Already exists: %s' % e
    except Exception as e:
        log.error(e, exc_info=True)
        return 'Failed: %s' % e


def main(args):
    logging.basicConfig(format='%(asctime)s %(levelname)-5.5s %(message)s')

    log.info(args)

    out = 'out-%s.pkl' % (datetime.datetime.strftime(
        datetime.datetime.now(), '%Y%m%d-%H%M%S'))

    # items = [('Dataset', 1802), ('Project', 1014)]

    with Calculator(args.server, args.port, args.user, password=args.password,
                    detach=False) as c:
        # Remember complist is a generator
        complist = c.genComputationList(args.ordered_arguments)

        if args.dry_run:
            log.info('Computation list:')
            for p in complist:
                log.info('  %s', p)
            return

        sessionid = c.client.getSessionId()

        paramsets = [{'host': args.server, 'port': args.port,
                      'sessionid': sessionid, 'ftparams': p} for p in complist]
        log.debug('paramsets: %s', [p['ftparams'] for p in paramsets])

        log.info('Creating pool of %d threads', args.threads)
        pool = multiprocessing.Pool(args.threads)
        results = pool.map(run1, paramsets)

        log.info('Saving statuses to: %s', out)
        with open(out, 'wb') as f:
            pickle.dump(results, f)
        # log.info('pool.imap results: %s', results)
        log.info('pool.map results length: [%d]', len(results))

    log.info('Main thread exiting')


def parse_args(args=None):
    class StoreOrdered(argparse.Action):
        """
        Based on http://stackoverflow.com/a/9028031
        """
        ORDERED_DEST = 'ordered_arguments'

        def __call__(self, parser, namespace, values, option_string=None):
            v = getattr(namespace, self.ORDERED_DEST, [])
            v.append((self.dest, values))
            setattr(namespace, self.ORDERED_DEST, v)

    parser = argparse.ArgumentParser()

    parser.add_argument('--server', default=None)
    parser.add_argument('--port', default=None, type=int)
    parser.add_argument('--user', default=None)
    parser.add_argument('--group', default=None, type=int)
    parser.add_argument('--password', default=None)
    parser.add_argument(
        '--threads', default=multiprocessing.cpu_count(), type=int)

    parser.add_argument('-n', '--dry-run', action='store_true')

    parser.add_argument('-p', action=StoreOrdered, dest='Project', type=int)
    parser.add_argument('-d', action=StoreOrdered, dest='Dataset', type=int)
    parser.add_argument('-i', action=StoreOrdered, dest='Image', type=int)
    return parser.parse_args(args)


if __name__ == '__main__':
    # Don't run if called inside ipython
    try:
        __IPYTHON__
    except NameError:
        args = parse_args()
        main(args)

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
Add random MapAnnotation(s) to all images in a dataset
"""

from datetime import datetime
from random import randrange, sample
import omero
from omero.rtypes import wrap, unwrap


with open('examples/words.txt') as f:
    WORDS = f.read().splitlines()


def get(client, common):
    qs = client.getSession().getQueryService()
    params = omero.sys.ParametersI()
    params.addIds(common)
    rs = qs.projection(
        'SELECT link.child.id FROM DatasetImageLink link '
        'WHERE link.parent.id in (:ids)', params)
    return [unwrap(r[0]) for r in rs]


def process(client, common, params):
    us = client.getSession().getUpdateService()
    #dt = datetime.now().strftime('%Y-%m-%d-%H:%M:%S.%f')
    ns = datetime.now().strftime('%Y-%m-%d')
    rs = []

    for imageid in params:
        numma = randrange(0, 100)
        for n in xrange(numma):
            numnvs = randrange(0, 100)
            numwords = [randrange(0, 100) for r in xrange(numnvs)]
            r = add_map_annotation(us, ns, numwords, imageid)
        print 'Image ID:%s' % imageid
        rs.append(r)
    return rs


def add_map_annotation(us, ns, numwords, imageid):
    ma = omero.model.MapAnnotationI()
    nvs = []
    for i in xrange(len(numwords)):
        nv = omero.model.NamedValue(
            'key %03d' % i, ' '.join(sample(WORDS, numwords[i])))
        nvs.append(nv)
    ma.setMapValue(nvs)

    ma.setNs(wrap(ns))
    ma = us.saveAndReturnObject(ma)

    link = omero.model.ImageAnnotationLinkI()
    image = omero.model.ImageI(imageid, False)
    link.setParent(image)
    link.setChild(ma)
    r = unwrap(us.saveAndReturnObject(link).getId())
    return r

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
Add a comment to all images in a dataset
"""

from datetime import datetime
import omero
from omero.rtypes import wrap, unwrap


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
    dt = datetime.now().strftime('%Y-%m-%d-%H:%M:%S.%f')
    rs = []
    for imageid in params:
        comment = omero.model.CommentAnnotationI()
        comment.setTextValue(wrap(dt))
        comment.setNs(wrap('test'))
        link = omero.model.ImageAnnotationLinkI()
        image = omero.model.ImageI(imageid, False)
        link.setParent(image)
        link.setChild(comment)
        # r = imageid
        r = unwrap(us.saveAndReturnObject(link).getId())
        print 'Comment ID:%s Image ID:%s' % (unwrap(comment.getId()), imageid)
        rs.append(r)
    return rs

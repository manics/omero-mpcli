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
Template for a parallelisable OMERO Python script
"""


def get(client):
    """
    This method will be called at the start, and should return a list of
    objects that will later be passed to process(). These objects must be
    serialisable to work with multiprocessing.

    Ideally this method should be idempotent.

    @param common: Configuration parameters
    @param client: An OMERO client object
    @return A list of serialisable objects to be processed
    """
    pass


def process(client, common, params):
    """
    This method will be called once a list of results from get() has been
    obtained.

    @param client: An OMERO client object
    @param common: Configuration parameters
    @param params: A partial list of results from query
    @return A list of serialisable objects to be returned
    """
    pass

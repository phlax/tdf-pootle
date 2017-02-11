# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import os

from th_pootle.management.commands import MySqlCommand


class TDFMySqlCommand(MySqlCommand):

    def fpath_revisions(self, path, name):
        return os.path.join(
            path, "revisions.%s.txt" % name)

    def fpath_unit_revisions(self, path, name):
        return os.path.join(
            path, "unit-revisions.%s.txt" % name)

    def fpath_new_subs_data(self, path, name):
        return os.path.join(
            path, "new-subs-data.%s.txt" % name)

    def fpath_subs_by_unit(self, path, name):
        return os.path.join(
            path, "subs-by-unit.%s.txt" % name)

    def fpath_subs_data(self, path, name):
        return os.path.join(
            path, "subs-data.%s.txt" % name)

    def fpath_submission_meta(self, path, name):
        return os.path.join(
            path, "submission-meta.%s.txt" % name)

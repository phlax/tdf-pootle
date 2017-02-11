# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import logging
import os
import sys

from uuid import uuid4

reload(sys)
sys.setdefaultencoding('utf-8')
os.environ['DJANGO_SETTINGS_MODULE'] = 'pootle.settings'

from django.core.management.base import CommandError

from . import TDFMySqlCommand


logger = logging.getLogger(__name__)


class Command(TDFMySqlCommand):
    help = (
        "Perform `select into outfile` on mysql. This command will output "
        "the file on the server where the database is running and will "
        "required the necessary permissions.")

    def add_arguments(self, parser):
        parser.add_argument(
            '-p', '--path',
            action='store',
            default='/tmp')
        parser.add_argument(
            '-t', '--type',
            action='store')
        parser.add_argument(
            '-n', '--name',
            action='store')

    def handle(self, **options):
        fname = options["name"] or uuid4().hex[:10]
        data_type = options["type"]
        data_types = [
            "unit_revision", "subs_by_unit", "subs_data",
            "submission_meta"]

        if data_type and data_type not in data_types:
            raise CommandError(
                "Unrecognized data type '%s' - i know about: %s"
                % (data_type, ", ".join(data_types)))

        if not hasattr(self.cursor.db, "mysql_version"):
            raise CommandError("This tool assumes the database is mysql")

        logger.warn(
            "Using this tool to output and reload data requires that "
            "nothing else can access the database server between the "
            "output and reload")

        if not data_type or data_type == "unit_revision":
            self.handle_unit_revisions(options["path"], name=fname)
        if not data_type or data_type == "subs_by_unit":
            self.handle_subs_by_unit(options["path"], name=fname)
        if not data_type or data_type == "subs_data":
            self.handle_subs_data(options["path"], name=fname)
        if not data_type or data_type == "submission_meta":
            self.handle_submission_meta(options["path"], name=fname)
        self.stdout.write("Tables dumped as: %s" % fname)

    def handle_unit_revisions(self, path, name):
        self.stdout.write(
            "Table unit_revisions with %(rows)s rows "
            "dumped to '%(filepath)s' "
            "in %(timing)s seconds"
            % self.outfile.select(
                "pootle_store_unit",
                self.fpath_unit_revisions(path, name),
                ["id", "revision"]))

    def handle_subs_by_unit(self, path, name):
        self.stdout.write(
            "Table subs_by_unit with %(rows)s rows "
            "dumped to '%(filepath)s' "
            "in %(timing)s seconds"
            % self.outfile.select(
                "pootle_app_submission",
                self.fpath_subs_by_unit(path, name),
                ["unit_id", "creation_time", "id"],
                order_by=["unit_id", "-creation_time", "-id"]))

    def handle_subs_data(self, path, name):
        self.stdout.write(
            "Table subs_data with %(rows)s rows "
            "dumped to '%(filepath)s' "
            "in %(timing)s seconds"
            % self.outfile.select(
                "pootle_app_submission",
                self.fpath_subs_data(path, name)))

    def handle_submission_meta(self, path, name):
        self.stdout.write(
            "Table submission_meta with %(rows)s rows "
            "dumped to '%(filepath)s' "
            "in %(timing)s seconds"
            % self.outfile.select_schema(
                "pootle_app_submission",
                self.fpath_submission_meta(path, name),
                optionally_enclose=None))

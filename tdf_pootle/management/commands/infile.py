# -*- coding: utf-8 -*-
#
# Copyright (C) Pootle contributors.
#
# This file is a part of the Pootle project. It is distributed under the GPL3
# or later license. See the LICENSE file for a copy of the license and the
# AUTHORS file for copyright and authorship information.

import codecs
import csv
import cStringIO
import logging
import os
import sys
import time
import warnings

import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')
os.environ['DJANGO_SETTINGS_MODULE'] = 'pootle.settings'

from django.db.utils import InternalError, ProgrammingError
from django.core.management.base import CommandError

from pootle_statistics.models import Submission

from . import TDFMySqlCommand


logger = logging.getLogger(__name__)
# make warnings into errors
warnings.filterwarnings('error', category=MySQLdb.Warning)


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self


class MySqlWriter:

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        data = data.replace("☠\"", b'\\"')
        data = data.replace("☠", b'\N')
        data = data.replace("\\\n", "\n")
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class Command(TDFMySqlCommand):
    help = (
        "Perform `load from infile` on mysql. This command will look for "
        "the file on the server where the database is running unless "
        "otherwise flagged and will required the necessary permissions.")

    def add_arguments(self, parser):
        parser.add_argument(
            'name',
            action='store')
        parser.add_argument(
            '-p', '--path',
            action='store',
            default='/tmp')
        parser.add_argument(
            '-l', '--local',
            action='store_true',
            default=False)
        parser.add_argument(
            '-a', '--action',
            action='store')
        parser.add_argument(
            '-t', '--target',
            action='store')
        parser.add_argument(
            '-k', '--keep-zombies',
            action='store_true')
        parser.add_argument(
            '-f', '--force',
            action='store_true')

    def get_revisions(self, path, name):
        revisionfile = self.fpath_revisions(path, name)

        if not os.path.exists(revisionfile):
            raise CommandError(
                "Revision file has not been generated at '%s'"
                % revisionfile)
        revisions = {}
        with open(revisionfile, "rb") as f:
            revision_data = csv.reader(f)
            for unitid, revision, pk in revision_data:
                revisions[pk] = revision
        return revisions

    def get_submission_meta(self, path, name):
        metafile = self.fpath_submission_meta(path, name)
        if not os.path.exists(metafile):
            raise CommandError(
                "Missing submission_meta file '%s'"
                % metafile)
        with open(metafile, "rb") as f:
            lines = [l for l in f.read().split("\n") if l.strip()]
        return lines

    def generate_revisions(self, path, name, units):
        subsunitfile = self.fpath_subs_by_unit(path, name)

        class RevisionCounter(object):
            last_unitid = 0
        counter = RevisionCounter()

        if not os.path.exists(subsunitfile):
            raise CommandError(
                "Missing subs_by_unit file '%s'"
                % subsunitfile)

        def revision_mangler(unitid, creation_time, pk):
            if counter.last_unitid != unitid:
                revision = units.get(unitid, 0)
            elif revision == 0:
                revision = 0
            else:
                revision = None
            if revision is not None:
                return [unitid, pk, revision]
            counter.last_unitid = unitid

        return self.csv_mangler(
            subsunitfile,
            self.fpath_revisions(path, name)).mangle(revision_mangler)

    def get_indeces(self, source, fields):
        return {
            k: source.index(k)
            for k in fields
            if k in source}

    def generate_submissions(self, path, name, revisions,
                             submeta, keep_zombies=True):
        resultfile = self.fpath_new_subs_data(path, name)
        subsdatafile = self.fpath_subs_data(path, name)

        if not os.path.exists(subsdatafile):
            raise CommandError(
                "Missing subs_data file '%s'"
                % subsdatafile)
        indeces = self.get_indeces(
            submeta,
            ["id", "creation_time", "unit_id",
             "old_value", "new_value",
             "similarity", "mt_similarity", "revision"])
        removed_indeces = [
            indeces["similarity"],
            indeces["mt_similarity"]]
        string_indeces = [
            indeces["old_value"],
            indeces["new_value"]]

        def submission_mangler(*sub):
            assert len(sub) == len(submeta)
            if not keep_zombies and sub[indeces["unit_id"]] == "N":
                return
            subid = sub[indeces["id"]]
            revision = revisions.get(subid)
            if revision is not None:
                sub[indeces["revision"]] = revision
                _sub = []
            for i, col in enumerate(sub):
                if i in removed_indeces:
                    continue
                if col == "N" and i not in string_indeces:
                    _sub.append(u"☠")
                else:
                    _sub.append(col)
            _sub[indeces["creation_time"]] = (
                '"%s"'
                % _sub[indeces["creation_time"]])
            _sub[indeces["old_value"]] = (
                u'"%s"'
                % _sub[indeces["old_value"]].replace('"', u"☠\""))
            _sub[indeces["new_value"]] = (
                u'"%s"'
                % _sub[indeces["new_value"]].replace('"', u"☠\""))
            return _sub
        reader_kwargs = dict(
            lineterminator='\n',
            escapechar="\\",
            doublequote=False)
        writer_kwargs = dict(
            lineterminator='\n',
            escapechar="\\",
            quotechar="'",
            doublequote=False,
            quoting=csv.QUOTE_NONE)
        return self.csv_mangler(
            subsdatafile,
            resultfile,
            reader_class=UnicodeReader,
            reader_kwargs=reader_kwargs,
            writer_class=MySqlWriter,
            writer_kwargs=writer_kwargs).mangle(submission_mangler)

    def handle_generate_revisions(self, path, name):
        """Generate a file containing a list of submission, revision
        where this can be gleaned
        """
        subsunitfile = self.fpath_subs_by_unit(path, name)
        csv_loader = self.csv_loader(self.fpath_unit_revisions(path, name))

        class RevisionCounter(object):
            last_unitid = 0
        counter = RevisionCounter()

        unit_revisions = csv_loader.as_dict(
            loader=(
                lambda unit, revision: (
                    (unit, int(revision))
                    if (int(revision) > 0)
                    else None)))
        self.stdout.write(
            "Unit revisions loaded from '%(source)s' "
            "with %(count)s/%(total)s rows "
            "generated in %(timing)s seconds"
            % unit_revisions)
        units = unit_revisions["results"]

        if not os.path.exists(subsunitfile):
            raise CommandError(
                "Missing subs_by_unit file '%s'"
                % subsunitfile)

        def revision_mangler(unitid, creation_time, pk):
            """sets the revision for the last sub of each unit"""
            if counter.last_unitid != unitid:
                revision = units.get(unitid, 0)
            elif revision == 0:
                revision = 0
            else:
                revision = None
            if revision is not None:
                return [unitid, pk, revision]
            counter.last_unitid = unitid

        self.stdout.write(
            "Revision file '%(target)s' with %(count)s/%(total)s rows "
            "generated in %(timing)s seconds"
            % self.csv_mangler(
                subsunitfile,
                self.fpath_revisions(path, name)).mangle(revision_mangler))

    def handle_generate_submissions(self, path, name, keep_zombies=False):
        """Generate a file containing all submission data to be loaded
        """
        self.stdout.write(
            "Submission file '%s' with %s/%s rows generated in %s seconds"
            % self.generate_submissions(
                path, name,
                self.get_revisions(path, name),
                self.get_submission_meta(path, name),
                keep_zombies=keep_zombies))

    def rows_in_target(self, target):
        try:
            self.cursor.execute("select count(*) from %s" % target)
        except ProgrammingError:
            return None
        return self.cursor.fetchone()[0]

    def handle_validate_submissions(self, target, step=1000, check_count=True):
        start = time.time()
        subs = Submission.objects
        self.cursor.execute("select count(*) from %s" % target)
        count = self.cursor.fetchone()[0]
        if check_count:
            assert subs.count() == count
        i = 0
        while True:
            self.cursor.execute(
                "select id, old_value, new_value from %s limit %s offset %s"
                % (target, step, i + step))
            result = self.cursor.fetchall()
            existing = subs.filter(
                id__in=[
                    res[0]
                    for res
                    in result]).values_list("id", "old_value", "new_value")
            assert (
                {e[0]: e[1:] for e in existing}
                == {r[0]: r[1:] for r in result})
            i += step
            print "completed %s" % min(i, count)
            if i > count:
                break
        self.stdout.write(
            "Validated %s submissions from table '%s' in %s seconds"
            % (count, target, (time.time() - start)))

    def handle_load_submissions(self, path, name, target, local=False):
        subsfile = self.fpath_new_subs_data(path, name)
        if not os.path.exists(subsfile):
            raise CommandError(
                "Missing submissions file '%s'"
                % subsfile)
        try:
            result = self.infile.load(
                target,
                subsfile,
                local=local)
        except InternalError as e:
            if not local:
                raise CommandError(
                    "You probably need to specify `local`: %s" % e)
            raise e
        self.stdout.write(
            "Submissions loaded (%(rows)s) from '%(filepath)s' "
            "in %(timing)s seconds"
            % result)

    def handle(self, **options):
        name = options["name"]
        path = options["path"]
        actions = [
            "generate_revisions",
            "generate_submissions",
            "load_submissions",
            "validate_submissions"]
        action = options["action"]
        local = options["local"]
        keep_zombies = options["keep_zombies"]
        target = options["target"] or "pootle_app_submission"

        if action == "validate_submissions":
            self.handle_validate_submissions(target)
            return

        rowcount = self.rows_in_target(target)

        if rowcount is None:
            # table doesnt exist, create it
            self.manager.copy_table("pootle_app_submission", target)
        if action and action not in actions:
            raise CommandError(
                "Unrecognized action '%s' - i know about: %s"
                % (action, ", ".join(actions)))
        if not action or action == "generate_revisions":
            self.handle_generate_revisions(path, name)
        if not action or action == "generate_submissions":
            self.handle_generate_submissions(
                path, name, keep_zombies=keep_zombies)
        if not action or action == "load_submissions":
            if rowcount:
                if not options["force"]:
                    raise CommandError(
                        "Target table '%s' contains data and "
                        "force not specificed"
                        % target)
                else:
                    self.manager.truncate_table(target)
            self.handle_load_submissions(path, name, target, local)

#!/usr/bin/env python

import datetime
import unittest

import sqlalchemy

from timeturner import timeturner_app

ONE_MINUTE = datetime.timedelta(minutes=1)
ONE_DAY = datetime.timedelta(days=1)

class DatabaseTest(unittest.TestCase):
    def setUp(self):
        engine = sqlalchemy.create_engine('sqlite:///:memory:')
        timeturner_app.Base.metadata.create_all(engine)
        self.session = sqlalchemy.orm.sessionmaker(bind=engine)()
        self.datetime_now = datetime.datetime(2013, 9, 21, 1, 2, 3)

        self.database = timeturner_app.Database(self.session, lambda: self.datetime_now)

    def _add_snapshot(self, datetime=None, hostname='myhost', title='some stuff'):
        self.database.add_snapshot(datetime or self.datetime_now, hostname, title, 'hello world!')

    def test_add_and_fetch_snapshot(self):
        self._add_snapshot()
        self.session.flush()
        contents = self.database.get_snapshot_contents(self.datetime_now, 'myhost', 'some stuff')
        self.assertEqual('hello world!', contents)

    def _add_timestamp_test_snapshots(self):
        self._add_snapshot()
        self._add_snapshot(datetime=self.datetime_now - ONE_MINUTE)
        self._add_snapshot(self.datetime_now - ONE_DAY)

    def test_list_days(self):
        self._add_timestamp_test_snapshots()
        self.assertEqual(
            [self.datetime_now.date() - ONE_DAY, self.datetime_now.date()],
            self.database.get_all_days(),
        )

    def test_list_timestamps(self):
        self._add_timestamp_test_snapshots()

        minute_now = self.datetime_now.replace(second=0, microsecond=0)
        self.assertEqual(
            [minute_now - ONE_MINUTE, minute_now],
            list(self.database.get_timestamps(self.datetime_now.date())),
        )

        a_day_later = self.datetime_now.date() + ONE_DAY
        self.assertEqual([], list(self.database.get_timestamps(a_day_later)))

    def test_list_snapshots_at_time(self):
        self._add_snapshot()
        self._add_snapshot(hostname='otherhost')
        self._add_snapshot(title='other stuff')
        self._add_snapshot(datetime=self.datetime_now - ONE_MINUTE)

        self.assertEqual(
            [('myhost', 'other stuff'), ('myhost', 'some stuff'), ('otherhost', 'some stuff')],
            list(self.database.get_snapshot_info_at_time(self.datetime_now)),
        )

    def test_no_duplicate_snapshots(self):
        self._add_snapshot()
        self._add_snapshot(datetime=self.datetime_now - ONE_MINUTE)
        self._add_snapshot(hostname='otherhost')
        self._add_snapshot(title='other stuff')
        with self.assertRaises(timeturner_app.DuplicateSnapshotError):
            self._add_snapshot()

    def test_clean_old_snapshots(self):
        self._add_snapshot()
        self._add_snapshot(hostname='otherhost')
        self.assertEqual(2, self.session.query(timeturner_app.Snapshot).count())
        self.datetime_now += datetime.timedelta(days=100)
        self._add_snapshot()
        self.assertEqual(1, self.session.query(timeturner_app.Snapshot).count())

    def test_that_a_snapshot_with_non_ascii_data_gets_cleaned_when_added(self):
        self.database.add_snapshot(
            self.datetime_now,
            'myhost',
            'some stuff',
            '\xf0\x9f\x98\x88',
        )
        self.session.flush()
        contents = self.database.get_snapshot_contents(self.datetime_now, 'myhost', 'some stuff')
        self.assertEqual('&#128520;', contents)

if __name__ == '__main__':
    unittest.main()

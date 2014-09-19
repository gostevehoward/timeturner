#!/usr/bin/env python

import collections
import cStringIO
import csv
import datetime
import json
import logging
import os
import sqlite3

import sqlalchemy
import sqlalchemy.orm.exc
from sqlalchemy.ext import declarative

import jinja2
import werkzeug.exceptions
from werkzeug import routing
import werkzeug.serving
from werkzeug import wrappers
import werkzeug.utils

Base = declarative.declarative_base()

class DuplicateSnapshotError(Exception):
    def __init__(self, timestamp, hostname, title):
        super(DuplicateSnapshotError, self).__init__('{} {} {}'.format(timestamp, hostname, title))

class Snapshot(Base):
    __tablename__ = 'snapshot'
    __table_args__ = (
        sqlalchemy.UniqueConstraint('timestamp', 'hostname', 'title'),
    )

    snapshot_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime)
    hostname = sqlalchemy.Column(sqlalchemy.String)
    title = sqlalchemy.Column(sqlalchemy.String)
    contents = sqlalchemy.Column(sqlalchemy.Text)

class Database(object):
    def __init__(self, session, datetime_now):
        self._session = session
        self._datetime_now = datetime_now

    def _clean_old_snapshots(self, now=datetime.datetime.now):
        oldest_allowed_timestamp = self._datetime_now() - datetime.timedelta(days=14)
        query = self._session.query(Snapshot).filter(Snapshot.timestamp < oldest_allowed_timestamp)
        query.delete()

    def add_snapshot(self, timestamp, hostname, title, csv_contents):
        cleaned_csv_contents = (
            csv_contents.decode('utf-8', 'replace').encode('ascii', 'xmlcharrefreplace')
        )
        self._session.add(
            Snapshot(
                timestamp=timestamp,
                hostname=hostname,
                title=title,
                contents=cleaned_csv_contents,
            )
        )

        try:
            self._session.flush()
        except sqlalchemy.exc.IntegrityError:
            raise DuplicateSnapshotError(timestamp, hostname, title)

        self._clean_old_snapshots()

    def get_all_days(self):
        query = (
            self._session.query(Snapshot)
                .distinct(Snapshot.timestamp)
                .with_entities(Snapshot.timestamp)
        )
        return sorted(set(snapshot.timestamp.date() for snapshot in query))

    def get_timestamps(self, day):
        query = (
            self._session.query(Snapshot)
                .filter(day <= Snapshot.timestamp)
                .filter(Snapshot.timestamp < day + datetime.timedelta(days=1))
                .order_by(Snapshot.timestamp.asc())
                .distinct(Snapshot.timestamp)
                .with_entities(Snapshot.timestamp)
        )
        return sorted(
            set(snapshot.timestamp.replace(second=0, microsecond=0) for snapshot in query)
        )

    def get_snapshot_info_at_time(self, timestamp):
        query = (
            self._session.query(Snapshot)
                .filter(timestamp <= Snapshot.timestamp)
                .filter(Snapshot.timestamp < timestamp + datetime.timedelta(seconds=60))
                .order_by(Snapshot.hostname.asc(), Snapshot.title.asc())
                .with_entities(Snapshot.hostname, Snapshot.title)
        )
        return ((snapshot.hostname, snapshot.title) for snapshot in query)

    def get_snapshot_contents(self, timestamp, hostname, title):
        return (
            self._session.query(Snapshot)
                .filter(timestamp <= Snapshot.timestamp)
                .filter(Snapshot.timestamp < timestamp + datetime.timedelta(seconds=60))
                .filter_by(hostname=hostname, title=title)
                .one()
                .contents
        )

class JinjaWrapper(object):
    def __init__(self, jinja_environment, base_context=None):
        self._jinja_environment = jinja_environment
        self._base_context = base_context or {}

        self._jinja_environment.filters['format_datetime'] = self._format_datetime

    def _format_datetime(self, value, format='%Y-%m-%d %H:%M'):
        return value.strftime(format)

    def render_template(self, template_name, environment, mime_type='text/html'):
        template = self._jinja_environment.get_template(template_name)
        context = dict(self._base_context, **environment)
        return wrappers.Response(template.render(**context), mimetype=mime_type)

class RequestHandler(object):
    def __init__(self, request, urls, database, jinja_wrapper):
        self._request = request
        self._urls = urls
        self._database = database
        self._jinja_wrapper = jinja_wrapper

    def _parse_date(self, date_string):
        try:
            return datetime.datetime.strptime(date_string, '%Y%m%d').date()
        except ValueError:
            raise werkzeug.exceptions.BadRequest('Invalid date {}'.format(date_string))

    def _parse_datetime(self, date_string, time_string):
        try:
            time = datetime.datetime.strptime(time_string, '%H%M').time()
        except ValueError:
            try:
                time = datetime.datetime.strptime(time_string, '%H%M%S').time()
            except ValueError:
                raise werkzeug.exceptions.BadRequest('Invalid time {}'.format(time_string))

        return datetime.datetime.combine(self._parse_date(date_string), time)

    def list_days(self):
        return self._jinja_wrapper.render_template(
            'list_days.html',
            {'days': self._database.get_all_days()},
        )

    def list_times(self, date):
        date = self._parse_date(date)
        return self._jinja_wrapper.render_template(
            'list_times.html',
            {'day': date, 'timestamps': self._database.get_timestamps(date)},
        )

    def list_snapshots(self, date, time):
        timestamp = self._parse_datetime(date, time)
        snapshot_infos = self._database.get_snapshot_info_at_time(timestamp)
        data_map = collections.defaultdict(list)
        for hostname, title in snapshot_infos:
            data_map[hostname].append(title)
        return self._jinja_wrapper.render_template(
            'list_snapshots.html',
            {'timestamp': timestamp, 'data_map': dict(data_map)},
        )

    def view_or_add_snapshot(self, date, time, hostname, title):
        timestamp = self._parse_datetime(date, time)
        if self._request.method == 'GET':
            csv_contents = self._database.get_snapshot_contents(timestamp, hostname, title)
            rows = list(csv.reader(cStringIO.StringIO(csv_contents)))

            return self._jinja_wrapper.render_template(
                'view_snapshot.html',
                {
                    'timestamp': timestamp,
                    'hostname': hostname,
                    'title': title,
                    'columns': rows[0],
                    'data_rows': rows[1:],
                },
            )
        elif self._request.method == 'PUT':
            self._database.add_snapshot(timestamp, hostname, title, self._request.get_data())
            return wrappers.Response(status=201)
        else:
            raise AssertionError('Unexpected method {}'.format(self._request.method))

class TimeTurnerApp(object):
    def __init__(self, session_factory, jinja_environment):
        self._session_factory = session_factory
        self._jinja_environment = jinja_environment

        self._url_map = routing.Map(
            [
                routing.Rule('/', endpoint='list days', methods=['GET']),
                routing.Rule('/<date>/', endpoint='list times on day', methods=['GET']),
                routing.Rule('/<date>/<time>/', endpoint='list snapshots at time', methods=['GET']),
                routing.Rule('/<date>/<time>/<hostname>/<title>/', endpoint='snapshot',
                             methods=['GET', 'PUT']),
            ]
        )
        self._endpoints = {
            'list days': RequestHandler.list_days,
            'list times on day': RequestHandler.list_times,
            'list snapshots at time': RequestHandler.list_snapshots,
            'snapshot': RequestHandler.view_or_add_snapshot,
        }

    @wrappers.Request.application
    def __call__(self, request):
        session = self._session_factory()
        adapter = self._url_map.bind_to_environ(request.environ)
        jinja_wrapper = JinjaWrapper(self._jinja_environment, dict(urls=adapter))
        request_handler = RequestHandler(
            request,
            adapter,
            Database(session, datetime.datetime.now),
            jinja_wrapper,
        )

        try:
            endpoint, kwargs = adapter.match()
            handler_fn = self._endpoints[endpoint]
            response = handler_fn(request_handler, **kwargs)
            session.commit()
            return response
        except werkzeug.exceptions.HTTPException, exc:
            session.rollback()
            return exc
        except Exception:
            logging.exception('Exception during request:')
            session.rollback()
            raise

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    engine = sqlalchemy.create_engine('sqlite:///timeturner.sqlite', echo=True)
    Base.metadata.create_all(engine)
    session_factory = sqlalchemy.orm.sessionmaker(bind=engine)

    jinja = jinja2.Environment(loader=jinja2.PackageLoader('timeturner', 'templates'))
    app = TimeTurnerApp(session_factory, jinja)
    werkzeug.serving.run_simple(
        'localhost',
        8080,
        app,
        use_reloader=True,
        static_files={'/static': os.path.join(os.path.dirname(__file__), 'static')},
    )

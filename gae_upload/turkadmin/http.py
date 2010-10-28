from google.appengine.ext import webapp
from google.appengine.ext.webapp import template

from django.utils import simplejson as json

from StringIO import StringIO

import cgi, csv


class RequestHandler(webapp.RequestHandler):
  def assignment_approval_url(self, approval):
    return '%s/assignment/approval?key=%s' % (self.request.host_url, approval.key())

  def assignment_rejection_url(self, rejection):
    return '%s/assignment/rejection?key=%s' % (self.request.host_url, rejection.key())

  def worker_bonus_url(self, bonus):
    return '%s/worker/bonus?key=%s' % (self.request.host_url, bonus.key())

  def worker_notification_url(self, notification):
    return '%s/worker/notification?key=%s' % (self.request.host_url, notification.key())

  def csv_reader(self, name):
    return csv.reader(StringIO(self.request.get(name)))

  def write(self, data):
    self.response.out.write(data)

  def render(self, path, params):
    self.write(template.render(path, params))

  def inspect(self, obj):
    self.write(cgi.escape(repr(obj)))

  def reply(self, code, text):
    self.response.set_status(code)

    self.write(cgi.escape(text))

  def json(self, data):
    self.response.headers['Content-Type'] = 'application/json'

    self.write(json.dumps(data))

  def bad_request(self, text='Bad Request'):
    self.reply(400, text)

  def not_found(self, text='Not Found'):
    self.reply(404, text)

  def method_not_allowed(self, text='Method Not Allowed'):
    self.reply(405, text)

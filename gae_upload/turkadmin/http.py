from google.appengine.ext import webapp
from google.appengine.ext.webapp import template

from django.utils import simplejson as json

from StringIO import StringIO

import cgi, csv


class RequestHandler(webapp.RequestHandler):
  def action_url(self, action):
    return '%s/action?key=%s' % (self.request.host_url, action.key())

  def operation_url(self, operation):
    return '%s/operation?key=%s' % (self.request.host_url, operation.key())

  def operation_task_url(self, operation):
    return '%s/operation/task?key=%s' % (self.request.host_url, operation.key())

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

  def internal_server_error(self, text='Internal Server Error'):
    self.reply(500, text)

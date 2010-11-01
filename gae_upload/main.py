from google.appengine.ext import webapp
from google.appengine.ext import db as datastore
from google.appengine.ext.webapp.util import run_wsgi_app as run_wsgi
from google.appengine.api.labs import taskqueue

from turkadmin.http import RequestHandler
from turkadmin.mturk import Connection as MTurkConnection, get_assignments
from turkadmin.models import *

from boto.exception import BotoClientError, BotoServerError

from datetime import datetime


class Struct(object):
  def __init__(self, **kwargs):
    self._attrs = kwargs

  def __getattr__(self, attr):
    return self._attrs[attr]


def entity_required(model, attr):
  def _decorate(fn):
    def _wrapper_fn(self, *args, **kwargs):
      key = self.request.get('key', None)

      if key is None:
        self.bad_request('No key')
      else:
        try:
          setattr(self, attr, model.get(key))

          if getattr(self, attr) is None:
            self.not_found()
          else:
            return fn(self, *args, **kwargs)
        except datastore.BadKeyError:
          self.not_found()

    return _wrapper_fn
  return _decorate


def response_error(response):
  return '%s: %s' % (response.errors[0][0], response.errors[0][1])


def throws_boto_errors(fn):
  def _fn(self, *args, **kwargs):
    try:
      return fn(self, *args, **kwargs)
    except (BotoClientError, BotoServerError), response:
      self.internal_server_error(response_error(response))

  return _fn


def validates_posted_aws_params(fn):
  def _fn(self, *args, **kwargs):
    self.action = Action()
    self.action.aws_access_key_id = self.request.get('aws_access_key_id')
    self.action.aws_secret_access_key = self.request.get('aws_secret_access_key')
    self.action.aws_hostname = self.request.get('aws_hostname') or 'mechanicalturk.sandbox.amazonaws.com'

    return fn(self, *args, **kwargs)

  return _fn


def validates_posted_assignment_ids_param(fn):
  def _fn(self, *args, **kwargs):
    self.assignment_ids = set([row[0] for row in self.csv_reader('assignment_ids')])

    if len(self.assignment_ids) > 0:
      self.hit_id = self.request.get('hit_id')

      connection = MTurkConnection(self.action)

      hit_assignment_ids = get_assignments(connection, self.hit_id, lambda item: item.AssignmentId)

      invalid_ids = self.assignment_ids.difference(set(hit_assignment_ids))

      if len(invalid_ids) == 0:
        return fn(self, *args, **kwargs)
      else:
        self.bad_request('Bad assignment_ids: ' + repr(invalid_ids))
    else:
      self.bad_request('No assignment_ids')

  return _fn


def action_operations(action):
  return AbstractOperation.all().filter('action = ', action)


def operation_status(operation):
  if operation.completed:
    return 'Completed: %s' % operation.completed.strftime('%Y-%m-%d %H:%M')
  elif operation.error:
    return 'Error: %s' % operation.error
  else:
    return 'Pending'


def operation_execute(key, action):
  operation = datastore.get(key)

  if not operation.completed and not operation.error:
    connection = MTurkConnection(action)

    try:
      operation.execute(connection)

      operation.completed = datetime.now()
    except (BotoClientError, BotoServerError), response:
      operation.error = response_error(response)

    operation.put()


class ActionList(RequestHandler):
  def item(self, action):
    return Struct(url=self.action_url(action), action=action)

  def get(self):
    items = map(self.item, Action.all().order('-created'))

    if len(items) > 0:
      self.render('priv/action_list.html', {'items': items})
    else:
      self.render('priv/action_list_empty.html', {})


class ActionView(RequestHandler):
  def item(self, operation):
    return Struct(operation=operation, status=operation_status(operation), url=self.operation_url(operation))

  @entity_required(Action, 'action')
  def get(self):
    if self.action.confirmed:
      self.render('priv/action_results.html', {
        'action': self.action
      , 'operations': [self.item(operation) for operation in action_operations(self.action)]
      })
    else:
      self.render('priv/action_preview.html', {
        'action': self.action
      , 'operations': action_operations(self.action)
      , 'url': self.request.url
      })

  @entity_required(Action, 'action')
  def post(self):
    if self.action.confirmed:
      self.method_not_allowed()
    else:
      self.action.confirmed = datetime.now()
      self.action.put()

    for operation in action_operations(self.action):
      taskqueue.add(url='/operation/task', params={'key': operation.key()})

    self.redirect(self.request.url)


class OperationView(RequestHandler):
  @entity_required(AbstractOperation, 'operation')
  def get(self):
    properties = []

    properties.append(Struct(name='status', value=operation_status(self.operation)))

    properties.append(Struct(name='description', value=self.operation.description))

    for name in set(dir(self.operation.__class__)).difference(set(dir(AbstractOperation))):
      properties.append(Struct(name=name.replace('_', ' '), value=getattr(self.operation, name)))

    self.render('priv/operation_view.html', {
      'properties': properties
    })


class OperationTask(RequestHandler):
  @entity_required(AbstractOperation, 'operation')
  def post(self):
    datastore.run_in_transaction(operation_execute, self.operation.key(), self.operation.action)

    self.write('OK')


class AssignmentApprovalForm(RequestHandler):
  def get(self):
    self.render('priv/assignment_approval_form.html', {
      'url': self.request.url
    })

  @throws_boto_errors
  @validates_posted_aws_params
  @validates_posted_assignment_ids_param
  def post(self):
    self.action.put()

    for assignment_id in self.assignment_ids:
      operation = ApproveAssignmentOperation()
      operation.action = self.action
      operation.description = 'Approve assignment %s' % assignment_id
      operation.assignment_id = assignment_id
      operation.hit_id = self.hit_id
      operation.put()

    self.redirect(self.action_url(self.action))


class AssignmentRejectionForm(RequestHandler):
  def get(self):
    self.render('priv/assignment_rejection_form.html', {
      'url': self.request.url
    })

  @throws_boto_errors
  @validates_posted_aws_params
  @validates_posted_assignment_ids_param
  def post(self):
    self.action.put()

    for assignment_id in self.assignment_ids:
      operation = RejectAssignmentOperation()
      operation.action = self.action
      operation.description = 'Reject assignment %s' % assignment_id
      operation.assignment_id = assignment_id
      operation.hit_id = self.hit_id
      operation.reason = reason
      operation.put()

    self.redirect(self.action_url(self.action))


class WorkerBonusForm(RequestHandler):
  def get(self):
    self.render('priv/worker_bonus_form.html', {
      'url': self.request.url
    })

  @throws_boto_errors
  @validates_posted_aws_params
  def post(self):
    hit_id = self.request.get('hit_id')

    connection = MTurkConnection(self.action)

    assignment_ids = {}

    for item in get_assignments(connection, hit_id, status='Approved'):
      assignment_ids[item.WorkerId] = item.AssignmentId

    operations = []

    for row in self.csv_reader('parameters'):
      if assignment_ids.has_key(row[0]):
        if row[1] == assignment_ids[row[0]]:
          operation = GrantBonusOperation()
          operation.description = 'Grant bonus to worker %s' % row[0]
          operation.assignment_id = row[1]
          operation.worker_id = row[0]
          operation.hit_id = hit_id
          operation.amount = row[2]
          operation.reason = row[3]

          operations.append(operation)
        else:
          self.bad_request('Bad assignment id: ' + repr(row[1]))
      else:
        self.bad_request('Bad worker id: ' + repr(row[0]))

    if len(operations) > 0:
      self.action.put()

      for operation in operations:
        operation.action = self.action
        operation.put()

      self.redirect(self.action_url(self.action))
    else:
      self.bad_request('No parameters')


class WorkerNotificationForm(RequestHandler):
  def get(self):
    self.render('priv/worker_notification_form.html', {
      'url': self.request.url
    })

  @throws_boto_errors
  @validates_posted_aws_params
  def post(self):
    operations = []

    for row in self.csv_reader('parameters'):
      operation = NotifyWorkerOperation()
      operation.description = 'Notify worker %s' % row[0]
      operation.worker_id = row[0]
      operation.message_subject = row[1]
      operation.message_text = row[2]

    if len(operations) > 0:
      self.action.put()

      for operation in operations:
        operation.action = self.action
        operation.put()

      self.redirect(self.action_url(self.action))
    else:
      self.bad_request('No parameters')


def handlers():
  return [
    ('/', ActionList)
  , ('/action', ActionView)
  , ('/operation', OperationView)
  , ('/operation/task', OperationTask)
  , ('/assignment/approval/form', AssignmentApprovalForm)
  , ('/assignment/rejection/form', AssignmentRejectionForm)
  , ('/worker/bonus/form', WorkerBonusForm)
  , ('/worker/notification/form', WorkerNotificationForm)
  ]


def application():
  return webapp.WSGIApplication(handlers(), debug=True)


def main():
  run_wsgi(application())


if __name__ == '__main__':
  main()

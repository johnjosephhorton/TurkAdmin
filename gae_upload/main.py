from google.appengine.ext import webapp
from google.appengine.ext import db as datastore
from google.appengine.ext.webapp.util import run_wsgi_app as run_wsgi

from turkadmin.http import RequestHandler
from turkadmin.models import AssignmentApproval, AssignmentRejection, WorkerBonus, WorkerNotification
from turkadmin.mturk import Connection as MTurkConnection, invalid_assignment_ids, notify_workers

from boto.mturk.price import Price
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


def throws_boto_errors(fn):
  def _fn(self, *args, **kwargs):
    try:
      return fn(self, *args, **kwargs)
    except (BotoClientError, BotoServerError), response:
      self.internal_server_error('%s: %s' % (response.errors[0][0], response.errors[0][1]))

  return _fn


def operation_construct(operation, request):
  operation.aws_access_key_id = request.get('aws_access_key_id')
  operation.aws_secret_access_key = request.get('aws_secret_access_key')
  operation.aws_hostname = request.get('aws_hostname') or 'mechanicalturk.sandbox.amazonaws.com'
  return operation


class AssignmentStatusChangeView(RequestHandler):
  def results(self, entity):
    results = []

    for (assignment_id, status) in zip(entity.assignment_ids, entity.results):
      results.append(Struct(assignment_id=assignment_id, status=status))

    return results

  def confirm(self, entity):
    if entity.confirmed:
      self.method_not_allowed()
    else:
      entity.confirmed = datetime.now()
      entity.put()

      mturk = MTurkConnection(entity)

      results = []

      for assignment_id in entity.assignment_ids:
        try:
          results.append(self.status_change(mturk, assignment_id))
        except (BotoClientError, BotoServerError), response:
          message = 'Error: %s: %s' % (response.errors[0][0], response.errors[0][1])

          results.append(message)

          entity.results = results
          entity.put()

          self.internal_server_error(message)

      entity.results = results
      entity.put()

      self.redirect(self.request.url)


class Actions(RequestHandler):
  def get(self):
    self.render('priv/actions.html', {})


class AssignmentApprovalList(RequestHandler):
  def item(self, approval):
    return Struct(url=self.assignment_approval_url(approval), approval=approval)

  def get(self):
    self.render('priv/assignment_approval_list.html', {
      'items': map(self.item, AssignmentApproval.all())
    })


class AssignmentApprovalForm(RequestHandler):
  def get(self):
    self.render('priv/assignment_approval_form.html', {
      'action': self.request.url
    })

  @throws_boto_errors
  def post(self):
    approval = operation_construct(AssignmentApproval(), self.request)
    approval.assignment_ids = list(set([row[0] for row in self.csv_reader('assignment_ids')]))
    approval.hit_id = self.request.get('hit_id')

    if len(approval.assignment_ids) > 0:
      invalid_ids = invalid_assignment_ids(approval)

      if len(invalid_ids) == 0:
        approval.put()

        self.redirect(self.assignment_approval_url(approval))
      else:
        self.bad_request('Bad assignment_ids: ' + repr(invalid_ids))
    else:
      self.bad_request('No assignment_ids')


class AssignmentApprovalView(AssignmentStatusChangeView):
  @entity_required(AssignmentApproval, 'approval')
  def get(self):
    if self.approval.confirmed:
      self.render('priv/assignment_approval_results.html', {
        'approval': self.approval
      , 'results': self.results(self.approval)
      })
    else:
      self.render('priv/assignment_approval_preview.html', {
        'approval': self.approval
      , 'action': self.request.url
      })

  @entity_required(AssignmentApproval, 'approval')
  def post(self):
    self.confirm(self.approval)

  def status_change(self, mturk, assignment_id):
    mturk.approve_assignment(assignment_id)

    return 'Approved'


class AssignmentRejectionList(RequestHandler):
  def item(self, approval):
    return Struct(url=self.assignment_rejection_url(approval), approval=approval)

  def get(self):
    self.render('priv/assignment_rejection_list.html', {
      'items': map(self.item, AssignmentRejection.all())
    })


class AssignmentRejectionForm(RequestHandler):
  def get(self):
    self.render('priv/assignment_rejection_form.html', {
      'action': self.request.url
    })

  @throws_boto_errors
  def post(self):
    rejection = operation_construct(AssignmentRejection(), self.request)
    rejection.assignment_ids = list(set([row[0] for row in self.csv_reader('assignment_ids')]))
    rejection.hit_id = self.request.get('hit_id')
    rejection.reason = self.request.get('reason')

    if len(rejection.assignment_ids) > 0:
      invalid_ids = invalid_assignment_ids(rejection)

      if len(invalid_ids) == 0:
        rejection.put()

        self.redirect(self.assignment_rejection_url(rejection))
      else:
        self.bad_request('Bad assignment_ids: ' + repr(invalid_ids))
    else:
      self.bad_request('No assignment_ids')


class AssignmentRejectionView(AssignmentStatusChangeView):
  @entity_required(AssignmentRejection, 'rejection')
  def get(self):
    if self.rejection.confirmed:
      self.render('priv/assignment_rejection_results.html', {
        'rejection': self.rejection
      , 'results': self.results(self.rejection)
      })
    else:
      self.render('priv/assignment_rejection_preview.html', {
        'rejection': self.rejection
      , 'action': self.request.url
      })

  @entity_required(AssignmentRejection, 'rejection')
  def post(self):
    self.confirm(self.rejection)

  def status_change(self, mturk, assignment_id):
    mturk.reject_assignment(assignment_id, self.rejection.reason)

    return 'Rejected'


class WorkerBonusList(RequestHandler):
  def item(self, bonus):
    return Struct(url=self.worker_bonus_url(bonus), bonus=bonus)

  def get(self):
    self.render('priv/worker_bonus_list.html', {
      'items': map(self.item, WorkerBonus.all())
    })


class WorkerBonusForm(RequestHandler):
  def get(self):
    self.render('priv/worker_bonus_form.html', {
      'action': self.request.url
    })

  @throws_boto_errors
  def post(self):
    bonus = operation_construct(WorkerBonus(), self.request)
    bonus.hit_id = self.request.get('hit_id')
    bonus.assignment_ids = []
    bonus.worker_ids = []
    bonus.amount = self.request.get('amount')
    bonus.reason = self.request.get('reason')

    for row in self.csv_reader('worker_and_assignment_ids'):
      bonus.worker_ids.append(row[0])
      bonus.assignment_ids.append(row[1])

    if len(bonus.worker_ids) > 0:
      mturk = MTurkConnection(bonus)

      worker_and_assignment_ids, invalid_ids = {}, []

      for item in mturk.get_assignments(bonus.hit_id, status='Approved', page_size=50):
        worker_and_assignment_ids[item.WorkerId] = item.AssignmentId

      for (worker_id, assignment_id) in zip(bonus.worker_ids, bonus.assignment_ids):
        if worker_and_assignment_ids[worker_id] == assignment_id:
          pass
        else:
          invalid_ids.append(worker_id)

      if len(invalid_ids) == 0:
        bonus.put()

        self.redirect(self.worker_bonus_url(bonus))
      else:
        self.bad_request('Bad worker_and_assignment_ids: ' + repr(invalid_ids))
    else:
      self.bad_request('No worker_and_assignment_ids')


class WorkerBonusView(RequestHandler):
  @entity_required(WorkerBonus, 'bonus')
  def get(self):
    if self.bonus.confirmed:
      results = []

      for (worker_id, assignment_id, status) in zip(self.bonus.worker_ids, self.bonus.assignment_ids, self.bonus.results):
        results.append(Struct(worker_id=worker_id, assignment_id=assignment_id, status=status))

      self.render('priv/worker_bonus_results.html', {
        'bonus': self.bonus
      , 'results': results
      })
    else:
      worker_and_assignment_ids = []

      for (worker_id, assignment_id) in zip(self.bonus.worker_ids, self.bonus.assignment_ids):
        worker_and_assignment_ids.append(Struct(worker_id=worker_id, assignment_id=assignment_id))

      self.render('priv/worker_bonus_preview.html', {
        'worker_and_assignment_ids': worker_and_assignment_ids
      , 'bonus': self.bonus
      , 'action': self.request.url
      })

  @entity_required(WorkerBonus, 'bonus')
  def post(self):
    if self.bonus.confirmed:
      self.bonus.method_not_allowed()
    else:
      self.bonus.confirmed = datetime.now()
      self.bonus.put()

      mturk = MTurkConnection(self.bonus)

      results = []

      bonus_price = Price(self.bonus.amount)

      for (worker_id, assignment_id) in zip(self.bonus.worker_ids, self.bonus.assignment_ids):
        try:
          mturk.grant_bonus(worker_id, assignment_id, bonus_price, self.bonus.reason)

          results.append('Granted')
        except (BotoClientError, BotoServerError), response:
          message = 'Error: %s: %s' % (response.errors[0][0], response.errors[0][1])

          results.append(message)

          self.bonus.results = results
          self.bonus.put()

          self.internal_server_error(message)

      self.bonus.results = results
      self.bonus.put()

      self.redirect(self.request.url)


class WorkerNotificationList(RequestHandler):
  def item(self, notification):
    return Struct(url=self.worker_notification_url(notification), notification=notification)

  def get(self):
    self.render('priv/worker_notification_list.html', {
      'items': map(self.item, WorkerNotification.all())
    })


class WorkerNotificationForm(RequestHandler):
  def get(self):
    self.render('priv/worker_notification_form.html', {
      'action': self.request.url
    })

  def post(self):
    notification = operation_construct(WorkerNotification(), self.request)
    notification.worker_ids = list(set([row[0] for row in self.csv_reader('worker_ids')]))
    notification.message_subject = self.request.get('message_subject')
    notification.message_text = self.request.get('message_text')
    notification.put()

    self.redirect(self.worker_notification_url(notification))


class WorkerNotificationView(RequestHandler):
  @entity_required(WorkerNotification, 'notification')
  def get(self):
    if self.notification.confirmed:
      self.render('priv/worker_notification_results.html', {
        'notification': self.notification
      })
    else:
      self.render('priv/worker_notification_preview.html', {
        'notification': self.notification
      , 'action': self.request.url
      })

  @throws_boto_errors
  @entity_required(WorkerNotification, 'notification')
  def post(self):
    if self.notification.confirmed:
      self.method_not_allowed()
    else:
      self.notification.confirmed = datetime.now()
      self.notification.put()

      notify_workers(self.notification)

      self.redirect(self.request.url)


def handlers():
  return [
    ('/', Actions)
  , ('/assignment/approval/list', AssignmentApprovalList)
  , ('/assignment/approval/form', AssignmentApprovalForm)
  , ('/assignment/approval', AssignmentApprovalView)
  , ('/assignment/rejection/list', AssignmentRejectionList)
  , ('/assignment/rejection/form', AssignmentRejectionForm)
  , ('/assignment/rejection', AssignmentRejectionView)
  , ('/worker/bonus/list', WorkerBonusList)
  , ('/worker/bonus/form', WorkerBonusForm)
  , ('/worker/bonus', WorkerBonusView)
  , ('/worker/notification/list', WorkerNotificationList)
  , ('/worker/notification/form', WorkerNotificationForm)
  , ('/worker/notification', WorkerNotificationView)
  ]


def application():
  return webapp.WSGIApplication(handlers(), debug=True)


def main():
  run_wsgi(application())


if __name__ == '__main__':
  main()

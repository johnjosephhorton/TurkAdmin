from google.appengine.ext import db as datastore
from google.appengine.ext.db.polymodel import PolyModel

from boto.mturk.price import Price


class Action(datastore.Model):
  created = datastore.DateTimeProperty(auto_now_add=True)
  creator = datastore.UserProperty(auto_current_user_add=True)
  aws_access_key_id = datastore.StringProperty()
  aws_secret_access_key = datastore.StringProperty()
  aws_hostname = datastore.StringProperty()
  confirmed = datastore.DateTimeProperty(default=None)


class AbstractOperation(PolyModel):
  action = datastore.ReferenceProperty(Action)
  description = datastore.StringProperty()
  #tasked = datastore.DateTimeProperty()
  completed = datastore.DateTimeProperty()
  error = datastore.StringProperty()

  def execute(self): # NB: run me in a transaction
    pass


class ApproveAssignmentOperation(AbstractOperation):
  assignment_id = datastore.StringProperty()
  hit_id = datastore.StringProperty()

  def execute(self, connection):
    return connection.approve_assignment(self.assignment_id)


class RejectAssignmentOperation(AbstractOperation):
  assignment_id = datastore.StringProperty()
  hit_id = datastore.StringProperty()
  reason = datastore.StringProperty()

  def execute(self, connection):
    return connection.reject_assignment(self.assignment_id, self.reason)


class GrantBonusOperation(AbstractOperation):
  assignment_id = datastore.StringProperty()
  worker_id = datastore.StringProperty()
  hit_id = datastore.StringProperty()
  amount = datastore.StringProperty()
  reason = datastore.StringProperty()

  def execute(self, connection):
    return connection.grant_bonus(self.worker_id, self.assignment_id, Price(self.amount), self.reason)


class NotifyWorkerOperation(AbstractOperation):
  worker_id = datastore.StringProperty()
  message_subject = datastore.StringProperty()
  message_text = datastore.TextProperty()

  def execute(self, connection):
    params = {'WorkerId': self.worker_id, 'Subject': self.message_subject, 'MessageText': self.message_text}

    return connection._process_request('NotifyWorkers', params)

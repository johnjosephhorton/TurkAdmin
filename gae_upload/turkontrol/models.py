from google.appengine.ext import db as datastore
from google.appengine.ext.db.polymodel import PolyModel


class Operation(PolyModel):
  created = datastore.DateTimeProperty(auto_now_add=True)
  #creator = datastore.UserProperty(auto_current_user_add=True)
  aws_access_key_id = datastore.StringProperty()
  aws_secret_access_key = datastore.StringProperty()
  aws_hostname = datastore.StringProperty()
  confirmed = datastore.DateTimeProperty(default=None)


class AssignmentApproval(Operation):
  assignment_ids = datastore.StringListProperty()
  hit_id = datastore.StringProperty()
  results = datastore.StringListProperty()


class AssignmentRejection(Operation):
  assignment_ids = datastore.StringListProperty()
  hit_id = datastore.StringProperty()
  reason = datastore.StringProperty()
  results = datastore.StringListProperty()


class WorkerBonus(Operation):
  assignment_ids = datastore.StringListProperty()
  worker_ids = datastore.StringListProperty()
  hit_id = datastore.StringProperty()
  amount = datastore.StringProperty()
  reason = datastore.StringProperty()
  results = datastore.StringListProperty()


class WorkerNotification(Operation):
  worker_ids = datastore.StringListProperty()
  message_subject = datastore.StringProperty()
  message_text = datastore.TextProperty()

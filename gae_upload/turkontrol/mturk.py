from boto.mturk.connection import MTurkConnection


def Connection(obj):
  return MTurkConnection(
    aws_access_key_id=obj.aws_access_key_id
  , aws_secret_access_key=obj.aws_secret_access_key
  , host=obj.aws_hostname
  )


def invalid_assignment_ids(obj):
  mturk = Connection(obj)

  assignments = mturk.get_assignments(obj.hit_id, status='Submitted', page_size=50)

  assignment_ids = map(lambda item: item.AssignmentId, assignments)

  return set(obj.assignment_ids).difference(set(assignment_ids))


def notify_workers(notification):
  conn = Connection(notification)

  params = {
    'Subject': notification.message_subject
  , 'MessageText': notification.message_text
  }

  if len(notification.worker_ids) == 1:
    params['WorkerId'] = notification.worker_ids[0]
  else:
    for i in range(0, len(notification.worker_ids)):
      params['WorkerId.%d' % (i + 1)] = notification.worker_ids[i]

  return conn._process_request('NotifyWorkers', params)

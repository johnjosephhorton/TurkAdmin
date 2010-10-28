from boto.mturk.connection import MTurkConnection


def Connection(obj):
  return MTurkConnection(
    aws_access_key_id=obj.aws_access_key_id
  , aws_secret_access_key=obj.aws_secret_access_key
  , host=obj.aws_hostname
  )


def get_assignments(connection, hit_id, fn=lambda x: x, status='Submitted', page_size=50):
  page_number = 1

  response = connection.get_assignments(hit_id, status=status, page_size=page_size)

  items = map(fn, response)

  total_num_results = int(response.TotalNumResults)

  while total_num_results > len(items):
    page_number += 1

    response = connection.get_assignments(hit_id, status=status, page_size=page_size, page_number=page_number)

    for item in response:
      items.append(fn(item))

  return items

events = {
  'type': 'object',
  'additionalProperties': False,
  'properties': {
    'event.created_time': { 'type': ['null', 'string'], 'format': 'date-time' },
    'event.id': { 'type': ['null', 'string'] },
    'event.is_admin': { 'type': ['null', 'string'] },
    'event.is_api_call': { 'type': ['null', 'string'] },
    'event.name': { 'type': ['null', 'string'] },
    'user.email': { 'type': ['null', 'string' ]},
    'user.id': { 'type': ['null', 'string'] },
    'role.name': { 'type': ['null', 'string' ]}
  }
}

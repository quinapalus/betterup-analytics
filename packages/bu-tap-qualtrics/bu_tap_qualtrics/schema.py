logs = {
  'items': {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
      'id': { 'type': ['null', 'string'] },
      'timestamp': { 'type': ['null', 'string'], 'format': 'date-time' },
      'datacenter': { 'type': ['null', 'string'] },
      'source': { 'type': ['null', 'string'] },
      'descriptor': { 'type': ['object'] }

    }
  }
}

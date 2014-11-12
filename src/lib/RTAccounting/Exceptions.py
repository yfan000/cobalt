"""Exceptions for the real-time accounting interface

"""

class ConnectionFailure(Exception):
    '''Unable to reach backing store'''
    pass

class BadMessage(Exception):
    '''Accounting system was unable to parse message sent'''
    pass

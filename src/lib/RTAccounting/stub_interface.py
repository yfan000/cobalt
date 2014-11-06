"""Accounting Interface null implementation

This is a do-nothing stub useful for testing, or running against accounting systems that do not implement real-time accounting.


"""
import logging
import Cobalt.RTAccounting.Exceptions

STOCK_REASON = "using the stub implementation."

_logger = logging.getLogger(__name__)

def user_default_project(user):
    '''stub implementation for project support.'''
    pass

def project_user_list(project):
    '''do nothing stub for getting list of users associated with projects'''
    pass

def fetch_job_status(job_data):
    '''return that all job statuses are OK for this version'''
    ret_list = []
    for job in  job_data:
        ret_list.append({'jobid': job['jobid'],
                         'status': "OK",
                         'reason': STOCK_REASON,
                         })
    return ret_list

def verify_job(job_data):
    '''return that all jobs are accepted'''
    ret_list = []
    for job in job_data:
        ret_list.append({'jobid': job['jobid'],
                         'status': "ACCEPT",
                         'reason': STOCK_REASON,
                        })
    return ret_list

def update_job(job_data, timestamp=None):
    '''do nothing stub interface for job_updates'''
    _logger.debug("JOB UPDATED with %s %s", job_data, timestamp)
    pass

def verify_reservation(reservation_data):
    '''STUB: all reservations should be accepted.'''
    ret_list = []
    for res in reservation_data:
        ret_list.append({'jobid': res['resid'],
                         'status': "ACCEPT",
                         'reason': STOCK_REASON,
                        })
    return ret_list

def update_reservation(reservation_data, timestamp=None):
    '''STUB: do nothing for reservation updates'''
    _logger.debug("RESERVATION UPDATED with %s %s", reservation_data, timestamp)


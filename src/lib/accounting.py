"""Handle generation of accounting log records, also known as the PBS logs.

These follow the standard mesasges from the accounting logs in OpenPBS's documentation.
This is based on the PBS Professional Reference Guide, chapter 9.

"""

from datetime import datetime
from time import mktime
from logging.handlers import BaseRotatingHandler
import codecs
import os
from Cobalt.Util import get_config_option
from Cobalt.PathImporter import import_from_config


try:
    RealTimeAccounting = import_from_config('RTAccounting', 'path', 'module_name')
except ImportError:
    import Cobalt.RTAccounting.stub_interface as RealTimeAccounting

#print RealTimeAccounting.fetch_job_status([{'jobid': 12345}])
import inspect
from functools import wraps


RESOURCE_NAME = get_config_option('system', 'resource_name', 'default')

#decorators for job/res information intercepts.
def _gen_arg_dict(func, *args, **kwargs):
    '''generate the update dictionary from incoming data to accounting functions'''
    update_dict = dict(zip(inspect.getargspec(func).args, args))
    for key, val in kwargs.items():
        update_dict[key] = val
    #make sure all messages are tagged with appropriate resource
    update_dict['resource'] = RESOURCE_NAME
    return update_dict

def job_record(func):
    '''Send job data from tagged messages to the realtime accounting system interface.'''
    @wraps(func)
    def send_update(*args, **kwargs):
        '''send job update information based on function's arguments'''
        RealTimeAccounting.update_job(_gen_arg_dict(func, *args, **kwargs))
        return func(*args, **kwargs)
    return send_update

def reservation_record(func):
    '''Send reservation data from tagged messages to the realtime accounting system interface.'''
    @wraps(func)
    def inner(*args, **kwargs):
        '''send reservation update information based on function's arguments'''
        RealTimeAccounting.update_reservation(_gen_arg_dict(func, *args, **kwargs))
        return func(*args, **kwargs)
    return inner

__all__ = ["abort", "begin", "checkpoint", "checkpoint_restart", "delete", "end", "finish",
    "system_remove", "remove", "queue", "rerun", "start", "unconfirmed",
    "confirmed", "DatetimeFileHandler"]


#messages to send
@job_record
def abort (job_id, user, resource_list, account=None, resource=RESOURCE_NAME):
    """Job was aborted by the server."""
    message = {'resource':resource, 'Resource_List':resource_list, 'user': user}
    if account is not None:
        message['account'] = account
    return entry("A", job_id, message)

@reservation_record
def begin (id_string, owner, queue, ctime, start, end, duration, exec_host, authorized_users, resource_list, name=None,
        account=None, authorized_groups=None, authorized_hosts=None, resource=RESOURCE_NAME):

    """Beginning of reservation period.

    Arguments:
    id_string -- reservation or reservation-job identifier
    owner -- name of party who submitted the resource reservation
    queue -- name of the associated queue
    ctime -- time at which the reservation was created
    start -- time at which the reservation is to start
    end -- time at which the reservation is to end
    duration -- duration specified or computed for the reservation
    exec_host -- nodes and node-associated resources (see qrun -H)
    authorized_users -- list of acl_users on the reservation queue
    resource_list -- resources requested by the reservation

    Keyword arguments:
    name -- if submitter supplied a name string for the reservation
    account -- if submitter supplied a string for accounting
    authorized_groups -- the list of acl_groups in the reservation queue
    authorized_hosts -- the list of acl_hosts on the reservation queue
    """

    message = {'owner':owner, 'queue':queue, 'ctime':ctime, 'start':start, 'end':end, 'duration':duration, 'exec_host':exec_host,
            'authorized_users':authorized_users, 'Resource_List':resource_list, 'resource':resource}
    if name is not None:
        message['name'] = name
    if account is not None:
        message['account'] = account
    if authorized_groups is not None:
        message['authorized_groups'] = authorized_groups
    if authorized_hosts is not None:
        message['authorized_hosts'] = authorized_hosts
    return entry("B", id_string, message)

@job_record
def checkpoint (job_id, resource=RESOURCE_NAME):
    """Job was checkpointed and held."""
    return entry("C", job_id, {'resource':resource})

@job_record
def checkpoint_restart (job_id, resource=RESOURCE_NAME):
    """Job was restarted from a checkpoint"""
    return entry("T", job_id, {'resource':resource})

@job_record
def delete (job_id, requester, user, resource_list, account=None, resource=RESOURCE_NAME):

    """Job was deleted by request.

    Arguments:
    job_id -- id of the deleted job
    requester -- who deleted the job (user@host)
    """

    message = {'requester':requester, 'resource':resource, 'Resource_List':resource_list, 'user': user}
    if account is not None:
        message['account'] = account
    return entry("D", job_id, message)

@job_record
def end (job_id, user, group, jobname, queue, cwd, exe, args, mode, ctime, qtime, etime, start, exec_host, resource_list, session,
        end, exit_status, resources_used, account=None, resvname=None, resv_id=None, alt_id=None, accounting_id=None,
        total_etime=None, priority_core_hours=None, resource=RESOURCE_NAME):

    """Job ended (terminated execution).

    Arguments:
    job_id -- identifier of the job that ended
    user -- the user name under which the job executed
    group -- the group name under which the job executed
    jobname -- the name of the job
    queue -- the name of the queue in which the job executed
    cwd -- the current working directory used by the job
    exe -- the executable run by the job
    args -- the arguments passsed to the executable
    mode -- the exection mode of the job
    ctime -- time when job was created
    qtime -- time when job was queued into current queue
    etime -- time in seconds when job became eligible to run
    start -- time when job execution started
    exec_host -- name of host on which the job is being executed
    resource_list -- list of the specified resource limits
    session -- session number of job
    end -- time when job ended execution
    exit_status -- exit status of the job
    resources_used -- aggregate amount (value) of resources used

    Keyword arguments:
    account -- if job has an "account name" string
    resvname -- the name of the resource reservation, if applicable
    resv_id -- the id of the resource reservation, if applicable
    alt_id -- optional alternate job identifier
    accounting_id -- CSA JID, job ID
    """

    message = {'user':user, 'group':group, 'jobname':jobname, 'queue':queue,
        'cwd':cwd, 'exe':exe, 'args':args, 'mode':mode,
        'ctime':ctime, 'qtime':qtime, 'etime':etime, 'start':start,
        'exec_host':exec_host, 'Resource_List':resource_list,
        'session':session, 'end':end, 'Exit_status':exit_status,
        'resources_used':resources_used, 'resource':resource}
    if account is not None:
        message['account'] = account
    if resvname is not None:
        message['resvname'] = resvname
    if resv_id is not None:
        message['resvID'] = resv_id
    if alt_id is not None:
        message['accounting_id'] = accounting_id
    if total_etime is not None:
        message['approx_total_etime'] = int(total_etime)

    if priority_core_hours is not None:
        message['priority_core_hours'] = int(priority_core_hours)
    else:
        message['priority_core_hours'] = 0

    return entry("E", job_id, message)

@reservation_record
def finish (reservation_id, resource=RESOURCE_NAME):
    """Resource reservation period finished."""
    return entry("F", reservation_id, resource)

@reservation_record
def system_remove (reservation_id, requester, resource=RESOURCE_NAME):

    """Scheduler or server requested removal of the reservation.

    Arguments:
    reservation_id -- id of the reservation that was removed
    requester -- user@host to identify who deleted the resource reservation
    """

    return entry("K", reservation_id, {'requester':requester})

@reservation_record
def remove (reservation_id, requester, resource=RESOURCE_NAME):

    """Resource reservation terminated by ordinary client.

    Arguments:
    reservation_id -- id of the reservation that was terminated
    requester -- user@host to identify who deleted the resource reservation
    """

    return entry("k", reservation_id, {'requester':requester})

@job_record
def queue (job_id, queue, user, resource_list, account=None, resource=RESOURCE_NAME):

    """Job entered a queue.

    Arguments:
    job_id -- id of the job that entered the queue
    queue -- the queue into which the job was placed
    user -- the user name under which the job will be executed
    resource_list -- a dictionary of specified resource limits
    account -- if not None, the account the job will be charged to (default: None)
    resource -- the resource that this job will run on (specified in the cobalt.conf file, default: default)

    """
    message = {'queue':queue, 'resource':resource, 'Resource_List':resource_list, 'user': user}
    if account is not None:
        message['account'] = account
    return entry("Q", job_id, message)

@job_record
def rerun (job_id, resource=RESOURCE_NAME):
    """Job was rerun."""
    return entry("R", job_id, {'resource':resource})

@job_record
def start (job_id, user, group, jobname, queue, cwd, exe, args, mode, ctime, qtime, etime, start, exec_host, resource_list, session,
        account=None, resvname=None, resv_id=None, alt_id=None, accounting_id=None, resource=RESOURCE_NAME):

    """Job started (terminated execution).

    Arguments:
    job_id -- identifier of the job that started
    user -- the user name under which the job executed
    group -- the group name under which the job executed
    jobname -- the name of the job
    queue -- the name of the queue in which the job resides
    cwd -- the current working directory used by the job
    exe -- the executable run by the job
    args -- the arguments passsed to the executable
    mode -- the exection mode of the job
    ctime -- time when job was created
    qtime -- time when job was queued into current queue
    etime -- time in seconds when job became eligible to run
    start -- time when job execution started
    exec_host -- name of host on which the job is being executed
    resource_list -- list of the specified resource limits
    session -- session number of job

    Keyword arguments:
    account -- if job has an "account name" string
    resvname -- the name of the resource reservation, if applicable
    resv_id -- the id of the resource reservation, if applicable
    alt_id -- optional alternate job identifier
    accounting_id -- CSA JID, job ID
    """

    message = {'user':user, 'group':group, 'jobname':jobname, 'queue':queue, 'cwd':cwd, 'exe':exe, 'args':args, 'mode':mode,
            'ctime':ctime, 'qtime':qtime, 'etime':etime, 'start':start, 'exec_host':exec_host, 'Resource_List':resource_list,
            'session':session, 'resource':resource}
    if account is not None:
        message['account'] = account
    if resvname is not None:
        message['resvname'] = resvname
    if resv_id is not None:
        message['resvID'] = resv_id
    if alt_id is not None:
        message['accounting_id'] = accounting_id
    return entry("S", job_id, message)

@reservation_record
def unconfirmed (reservation_id, requester, resource=RESOURCE_NAME):

    """Created unconfirmed resources reservation.

    Arguments:
    reservation_id -- id of the unconfirmed reservation
    requester -- user@host to identify who requested the resources reservation
    """

    return entry("U", reservation_id, {'requester':requester})

@reservation_record
def confirmed (reservation_id, requester, resource=RESOURCE_NAME):

    """Created unconfirmed resources reservation.

    Arguments:
    reservation_id -- id of the unconfirmed reservation
    requester -- user@host to identify who requested the resources reservation
    """

    return entry("Y", reservation_id, {'requester':requester})


class DatetimeFileHandler (BaseRotatingHandler):

    """A log file handler that rotates logs based on the current date/time.

    This handler determines the intended file name by parsing a pattern
    with datetime.now().strftime(). To create a daily log file in /var/log:

        DatetimeFileHandler("/var/log/%Y-%m-%d")

    The log will be rotated any time the intended filename changes.

    Arguments:
    file_pattern -- the pattern to be passed to datetime.now().strftime()

    Keyword arguments:
    encoding -- see FileHandler
    """

    def __init__ (self, file_pattern, encoding=None):
        self.file_pattern = file_pattern
        BaseRotatingHandler.__init__(self, self.get_baseFilename(),
                "a", encoding)

    def get_baseFilename (self):
        return os.path.abspath(datetime.now().strftime(self.file_pattern))

    def doRollover (self):
        self.stream.close()
        self.baseFilename = self.get_baseFilename()
        if self.encoding:
            self.stream = codecs.open(self.baseFilename, 'w', self.encoding)
        else:
            self.stream = open(self.baseFilename, 'w')

    def shouldRollover (self, record):
        return self.baseFilename != self.get_baseFilename()


def entry (record_type, id_string, message=None):

    """Generate an entry in a PBS accounting log.

    Arguments:
    record_type -- a single character indicating the type of record
    id_string -- the job, reservation, or reservation-job identifier
    message -- dictionary containing appropriate message data
    """

    if message is None:
        message = {}
    return _entry(datetime.now(), record_type, id_string, message)


def _entry (datetime_, record_type, id_string, message):

    """Generate an entry in a PBS accounting log.

    Arguments:
    datetime_ -- a date and time stamp
    record_type -- a single character indicating the type of record
    id_string -- the job, reservation, or reservation-job identifier
    message -- dictionary containing appropriate message data
    """

    assert record_type in ("A", "B", "C", "D", "E", "F", "K", "k", "Q", "R", "S", "T", "U", "Y"), \
            "invalid record_type %r" % record_type
    datetime_s = datetime_.strftime("%m/%d/%Y %H:%M:%S")
    message_text = serialize_message(message)
    return "%s;%s;%s;%s" % (datetime_s, record_type, id_string, message_text)


def serialize_message (message):
    '''convert message into PBS style keyval pairs.'''
    message = message.copy()
    for keyword, value in message.items():
        try:
            items = value.items()
        except AttributeError:
            pass
        else:
            del message[keyword]
            for keyword_, value_ in items:
                message['%s.%s' % (keyword, keyword_)] = value_
    for keyword, value in message.items():
        if isinstance(value, basestring):
            if ' ' in value or '"' in value or "," in value:
                message[keyword] = '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
        else:
            for func in (serialize_list, serialize_dt, serialize_td):
                try:
                    message[keyword] = func(message[keyword])
                except ValueError:
                    continue
                else:
                    break
    return " ".join("%s=%s" % (keyword, value)
        for (keyword, value) in sorted(message.items()))


def serialize_list (list_):
    '''Convert list into PBS-style ',' delimited list'''
    try:
        values = []
        for value in list_:
            if ' ' in value or '"' in value or "," in value:
                value = '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
            values.append(value)
        return ",".join(str(i) for i in values)
    except TypeError, ex:
        raise ValueError(ex)


def serialize_dt (datetime_):
    '''datetime to seconds from epoch with microseconds'''
    try:
        return (mktime(datetime_.timetuple()) +
            (1.0 * datetime_.microsecond / 1000000))
    except AttributeError, ex:
        raise ValueError(ex)


def serialize_td (timedelta_):
    '''Convert a timedelta to seconds.'''
    try:
        return ((1.0 * timedelta_.days * 24 * 60 * 60) + timedelta_.seconds
            + (timedelta_.microseconds / 1000000))
    except AttributeError, ex:
        raise ValueError(ex)



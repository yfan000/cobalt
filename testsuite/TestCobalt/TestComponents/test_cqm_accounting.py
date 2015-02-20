"""Unit tests for cqm accounting interface methods.

"""

from nose.tools import raises
import Cobalt.Components.cqm
import mock
from mock import Mock, MagicMock, patch
import time
import ConfigParser
import Cobalt.RTAccounting.stub_interface as RTAStub
import Cobalt.Components.cqm
from Cobalt.Components.cqm import QueueManager

config_file = 'test_cqm_accounting.conf'
CQM_CONFIG_FILE_ENTRY="""
[accounting]
exceptions_fatal = True

"""

def cqm_config_file_update(options = {}):
    config_file = Cobalt.CONFIG_FILES[0]
    config_fp = open(config_file, "w")
    config_fp.write(CQM_CONFIG_FILE_ENTRY)
    for option, value in options.iteritems():
        print >>config_fp, "%s: %s" % (option, value)
    config_fp.close()
    config = ConfigParser.ConfigParser()
    config.read(Cobalt.CONFIG_FILES)
    Cobalt.Components.cqm.config = config

class TestCQMAccounting(object):

    def setup(self):
        self.now = time.time()
        self.qm = Cobalt.Components.cqm.QueueManager()

    def teardown(self):
        Cobalt.Components.cqm.REALTIME_INTERFACE_ERRORS_FATAL = False #revert to default
        del self.qm

    def gen_jobs(self):
        '''call after setup has been called.  Generate a standard list of job specs
        to add to cqm's queue.

        '''

        spec = {'nodes':512, 'walltime':30, 'queue':'default', 'project':'foo'}
        return [dict(spec) for _ in range(0, 4)]

    def test__validate_cja_response_is_valid(self):
        test_response = {'jobid': 1, 'reason':'test message', 'status':'OK'}
        assert self.qm._validate_cja_response(test_response), "Reported valid response as invalid"

    def test__validate_cja_response_is_invalid_ignore(self):
        test_response1 = {'jobid': 1, 'reason':'test message'}
        test_response2 = {'jobid': 1, 'status':'OK'}
        test_response3 = {'reason':'test message', 'status':'OK'}
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_IGNORE = True
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_FALLBACK = False
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_CONTINUE = False
        assert not self.qm._validate_cja_response(test_response1), "Did not fail bad response 1."
        assert not self.qm._validate_cja_response(test_response2), "Did not fail bad response 2."
        assert not self.qm._validate_cja_response(test_response3), "Did not fail bad response 3."

    @raises(Cobalt.Exceptions.InvalidResponse)
    def test__validate_cja_response_is_invalid_fallback(self):
        test_response = {'jobid': 1, 'reason':'test message'}
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_IGNORE = False
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_FALLBACK = True
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_CONTINUE = False
        Cobalt.Components.cqm.RealTimeAccounting = MagicMock()
        try:
            self.qm._validate_cja_response(test_response)
        except Cobalt.Exceptions.InvalidResponse:
            assert Cobalt.Components.cqm.RealTimeAccounting == \
                Cobalt.Components.cqm.RTAFallbackInterface, \
                "Did not unload RTA interface."
            raise
        assert False, "Allowed an invalid response to pass, and no exception raised"

    @raises(Cobalt.Exceptions.InvalidResponse)
    def test__validate_cja_response_is_invalid_continue(self):
        test_response = {'jobid': 1, 'reason':'test message'}
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_IGNORE = False
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_FALLBACK = False
        Cobalt.Components.cqm.REALTIME_INTERFACE_BAD_RESPONSE_CONTINUE = True
        Cobalt.Components.cqm.RealTimeAccounting = MagicMock()
        try:
            self.qm._validate_cja_response(test_response)
        except Cobalt.Exceptions.InvalidResponse:
            assert Cobalt.Components.cqm.RealTimeAccounting != \
                Cobalt.Components.cqm.RTAFallbackInterface, \
                "Unloaded RTA interface."
            raise
        assert False, "Allowed an invalid response to pass, and no exception raised"

    def test__handle_cja_response_OK_no_hold(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        response = self.qm.Queues.add_jobs(self.gen_jobs())
        response[1].accounting_hold = True
        response[2].user_hold = True
        response[3].admin_hold = True
        changed = self.qm._handle_cja_response({'jobid':1, 'reason':'good msg', 'status':'OK'}, False)
        assert changed == False, "nothing should change"
        jobs = self.qm.Queues.get_jobs([{'jobid':'*', 'state':'*'}])
        other_jobs = []
        for job in jobs:
            if job.jobid == 1:
                job1 = job
            else:
                other_jobs.append(job)
        assert job1.state == 'queued', "job taken out of queued state"
        for job in other_jobs:
            assert job.state != 'queued', "wrong job modified"

    def test__handle_cja_response_OK_hold(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        response = self.qm.Queues.add_jobs(self.gen_jobs())
        response[1].accounting_hold = True
        response[2].user_hold = True
        response[3].admin_hold = True
        changed = self.qm._handle_cja_response({'jobid':2, 'reason':'good msg', 'status':'OK'}, False)
        assert changed == True, "hold not set"
        jobs = self.qm.Queues.get_jobs([{'jobid':'*', 'state':'*'}])
        other_jobs = []
        for job in jobs:
            if job.jobid == 1:
                job1 = job
            elif job.jobid == 2:
                job2 = job
            else:
                other_jobs.append(job)
        assert job1.state == 'queued', "job taken out of queued state"
        assert job2.state == 'queued', "job not in queued state"
        for job in other_jobs:
            assert job.state != 'queued', "wrong job modified"

    def test__handle_cja_response_HOLD_no_hold(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        response = self.qm.Queues.add_jobs(self.gen_jobs())
        response[1].accounting_hold = True
        response[2].user_hold = True
        response[3].admin_hold = True
        changed = self.qm._handle_cja_response({'jobid':1, 'reason':'good msg', 'status':'HOLD'}, False)
        assert changed == True, "hold not set"
        changed = self.qm._handle_cja_response({'jobid':3, 'reason':'good msg', 'status':'HOLD'}, False)
        assert changed == True, "hold not set"
        changed = self.qm._handle_cja_response({'jobid':4, 'reason':'good msg', 'status':'HOLD'}, False)
        assert changed == True, "hold not set"
        jobs = self.qm.Queues.get_jobs([{'jobid':'*', 'state':'*'}])
        other_jobs = []
        for job in jobs:
            if job.jobid == 1:
                job1 = job
            if job.jobid == 3:
                job3 = job
            if job.jobid == 4:
                job4 = job
            else:
                other_jobs.append(job)
        assert job1.state == 'accounting_hold', "job not in accounting_hold state"
        assert job3.accounting_hold == True, "accounting_hold not set on job 3"
        assert job4.accounting_hold == True, "accounting_hold not set on job 4"
        for job in other_jobs:
            assert job.state != 'queued', "wrong job modified"

    def test__handle_cja_response_HOLD_hold(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        response = self.qm.Queues.add_jobs(self.gen_jobs())
        response[1].accounting_hold = True
        response[2].user_hold = True
        response[3].admin_hold = True
        changed = self.qm._handle_cja_response({'jobid':2, 'reason':'good msg', 'status':'HOLD'}, False)
        assert changed == False, "nothing should change"
        jobs = self.qm.Queues.get_jobs([{'jobid':'*', 'state':'*'}])
        other_jobs = []
        for job in jobs:
            if job.jobid == 1:
                job1 = job
            elif job.jobid == 2:
                job2 = job
            else:
                other_jobs.append(job)
        assert job1.state == 'queued', "job not in queued state"
        assert job2.accounting_hold == True, "accounting_hold not set on job 3"
        for job in other_jobs:
            assert job.accounting_hold == False, "wrong job modified"

    def test__handle_cja_response_REMOVE(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        response = self.qm.Queues.add_jobs(self.gen_jobs())
        response[1].accounting_hold = True
        response[2].user_hold = True
        response[3].admin_hold = True
        changed = self.qm._handle_cja_response({'jobid':1, 'reason':'good msg', 'status':'REMOVE'}, False)
        assert changed == True, "nothing changed"
        jobs = self.qm.Queues.get_jobs([{'jobid':'*', 'state':'*'}])
        other_jobs = []
        for job in jobs:
            assert job.jobid != 1, "did not delete job"
            other_jobs.append(job)
        assert len(other_jobs) == 3, "deleted too many jobs"

    def test__handle_cja_response_UNKNOWN(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        response = self.qm.Queues.add_jobs(self.gen_jobs())
        response[1].accounting_hold = True
        response[2].user_hold = True
        response[3].admin_hold = True
        changed = self.qm._handle_cja_response({'jobid':1, 'reason':'good msg', 'status':'UNKNOWN'}, False)
        jobs = self.qm.Queues.get_jobs([{'jobid':'*', 'state':'*'}])
        assert changed == False, "Changed set when it shouldn't be."

    def test_queuemanager_add_jobs_good_spec(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg', 'status':'OK'}])
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512}]
        response = self.qm.add_jobs(job_specs)
        assert response[0].jobid == 1, "expected jobid 1, got: %s" % response[0].jobid
        assert response[0].queue == 'default', "expected queue default, got: %s" % response[0].queue
        assert response[0].accounting_hold == False, "accounting hold should not be set"

    #add_jobs top-level tests
    def test_queuemanager_add_jobs_bad_spec(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg','status':'HOLD'}])
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512, 'project':'foo'}]
        response = self.qm.add_jobs(job_specs)
        jobs = self.qm.get_jobs([{'jobid':'*'}])
        assert jobs[0].jobid == 1, "expected jobid 1, got: %s" % jobs[0].jobid
        assert jobs[0].queue == 'default', "expected queue default, got: %s" % jobs[0].queue
        assert jobs[0].accounting_hold == True, "accounting hold should be set"

    def test_queuemanager_add_jobs_bad_message(self):
        Cobalt.Components.cqm.REALTIME_INTERFACE_ERRORS_FATAL = True
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg', 'status':'HOLD'}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status.side_effect = \
                Cobalt.RTAccounting.Exceptions.BadMessage("Test BadMessage")
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512}]
        response = self.qm.add_jobs(job_specs)
        jobs = self.qm.get_jobs([{'jobid':'*'}])
        assert jobs[0].jobid == 1, "expected jobid 1, got: %s" % jobs[0].jobid
        assert jobs[0].queue == 'default', "expected queue default, got: %s" % jobs[0].queue
        assert jobs[0].accounting_hold == False, "accounting hold should not be set"

    def test_queuemanager_add_jobs_connection_failure(self):
        Cobalt.Components.cqm.REALTIME_INTERFACE_ERRORS_FATAL = True
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg', 'status':'HOLD'}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status.side_effect = \
                Cobalt.RTAccounting.Exceptions.ConnectionFailure("Test ConnectionFailure")
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512}]
        response = self.qm.add_jobs(job_specs)
        jobs = self.qm.get_jobs([{'jobid':'*'}])
        assert jobs[0].jobid == 1, "expected jobid 1, got: %s" % jobs[0].jobid
        assert jobs[0].queue == 'default', "expected queue default, got: %s" % jobs[0].queue
        assert jobs[0].accounting_hold == False, "accounting hold should not be set"

    def test_queuemanager_add_jobs_missing_field(self):
        Cobalt.Components.cqm.REALTIME_INTERFACE_ERRORS_FATAL = True
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'bad msg'}])
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512}]
        response = self.qm.add_jobs(job_specs)
        jobs = self.qm.get_jobs([{'jobid':'*'}])
        assert jobs[0].jobid == 1, "expected jobid 1, got: %s" % jobs[0].jobid
        assert jobs[0].queue == 'default', "expected queue default, got: %s" % jobs[0].queue
        assert jobs[0].accounting_hold == False, "accounting hold should not be set"

    def test_queuemanager_set_jobs_good_change(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg', 'status':'OK'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        joblist = self.qm.set_jobs([{'jobid':1,'tag':"job", 'queue':"*"}], {'walltime':600})
        assert len(joblist) == 1, 'Wrong number of jobs in joblist'
        assert joblist[0].walltime == 600, 'Walltime not set'
        assert joblist[0].accounting_hold == False, "accounting hold set"

    def test_queuemanager_set_jobs_bad_change(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg', 'status':'OK'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = \
                MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'HOLD'}])
        joblist = self.qm.set_jobs([{'jobid':1, 'tag':"job", 'queue':"*"}], {'walltime':600})
        assert joblist[0].walltime == 600, 'Walltime not set'
        assert joblist[0].accounting_hold == True, "accounting hold not set"

    def test_queuemanager_set_jobs_force_change(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg', 'status':'OK'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = \
                MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'HOLD'}])
        joblist = self.qm.set_jobs([{'jobid':1, 'tag':"job", 'queue':"*"}], {'walltime':600}, force=True)
        assert joblist[0].walltime == 600, 'Walltime not set'
        assert joblist[0].accounting_hold == False, "accounting hold set"

    def test_queuemanager_set_jobs_bad_message(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg', 'status':'HOLD'}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status.side_effect = \
                Cobalt.RTAccounting.Exceptions.BadMessage("Bad Message")
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = \
                MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'HOLD'}])
        joblist = self.qm.set_jobs([{'jobid':1, 'tag':"job", 'queue':"*"}], {'walltime':600}, force=True)
        assert joblist[0].walltime == 600, 'Walltime not set'
        assert joblist[0].accounting_hold == False, "accounting hold set"


    def test_queuemanager_set_jobs_conenction_failure(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'reason':'good msg', 'status':'HOLD'}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status.side_effect = \
                Cobalt.RTAccounting.Exceptions.ConnectionFailure("ConnectionFailure")
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = \
                MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'HOLD'}])
        joblist = self.qm.set_jobs([{'jobid':1, 'tag':"job", 'queue':"*"}], {'walltime':600}, force=True)
        assert joblist[0].walltime == 600, 'Walltime not set'
        assert joblist[0].accounting_hold == False, "accounting hold set"

    def test_queuemanager_set_jobs_out_of_spec_msg(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = MagicMock(return_value=[{'jobid':1,
            'status':'HOLD'}])
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status.side_effect = \
                Cobalt.RTAccounting.Exceptions.ConnectionFailure("ConnectionFailure")
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        Cobalt.Components.cqm.RealTimeAccounting.fetch_job_status = \
                MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'HOLD'}])
        joblist = self.qm.set_jobs([{'jobid':1, 'tag':"job", 'queue':"*"}], {'walltime':600}, force=True)
        assert joblist[0].walltime == 600, 'Walltime not set'
        assert joblist[0].accounting_hold == False, "accounting hold set"



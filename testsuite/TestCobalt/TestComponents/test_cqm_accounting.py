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

    def test__verify_job_spec_good_job(self):
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        test_spec = {'jobid': 1, 'resource': 'defualt', 'timestamp': self.now}
        success_string = "Job 1 accounting validation successful."
        failed, failure_msg = self.qm._verify_job_spec(test_spec)
        assert not failed, "Good job failed erroniously."
        assert failure_msg == success_string, "got %s expected %s" % (failure_msg, success_string)

    def test__verify_job_spec_good_job_advisory(self):
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        test_spec = {'jobid': 1, 'resource': 'defualt', 'timestamp': self.now}
        success_string = "Job 1 accounting validation successful."
        failed, failure_msg = self.qm._verify_job_spec(test_spec, advisory=True)
        assert not failed, "Good job failed erroniously."
        assert failure_msg == success_string, "got %s expected %s" % (failure_msg, success_string)

    def test__verify_job_spec_bad_job(self):
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'testsuite', 'status':'REJECT'}])
        test_spec = {'jobid': 1, 'resource': 'defualt', 'timestamp': self.now}
        success_string = "Job failed to validate.  Reason: testsuite"
        failed, failure_msg = self.qm._verify_job_spec(test_spec)
        assert failed, "Rejected job reported as passing"
        assert failure_msg == success_string, "got %s expected %s" % (failure_msg, success_string)

    def test__verify_job_spec_bad_job_advisory(self):
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'testsuite', 'status':'REJECT'}])
        test_spec = {'jobid': 1, 'resource': 'defualt', 'timestamp': self.now}
        success_string = "Job failed to validate.  Reason: testsuite"
        failed, failure_msg = self.qm._verify_job_spec(test_spec, advisory=True)
        assert not failed, "Advisory flag should always pass"
        assert failure_msg == success_string, "got %s expected %s" % (failure_msg, success_string)

    @raises(RTAStub.BadMessage)
    def test__verify_job_specs_bad_message(self):
        RTAStub.verify_job = MagicMock()
        RTAStub.verify_job.side_effect = RTAStub.BadMessage("Test Bad Message")
        test_spec = {'jobid': 1, 'resource': 'defualt', 'timestamp': self.now}
        failed, failure_msg = self.qm._verify_job_spec(test_spec)
        assert not failed, 'Somehow got out of function.'

    @raises(RTAStub.ConnectionFailure)
    def test__verify_job_specs_connection_failure(self):
        RTAStub.verify_job = MagicMock()
        RTAStub.verify_job.side_effect = RTAStub.ConnectionFailure("Test ConnectionFailure")
        test_spec = {'jobid': 1, 'resource': 'defualt', 'timestamp': self.now}
        failed, failure_msg = self.qm._verify_job_spec(test_spec)
        assert not failed, 'Somehow got out of function.'


    def test_queuemanager_add_jobs_good_spec(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512}]
        response = self.qm.add_jobs(job_specs)
        assert response[0].jobid == 1, "expected jobid 1, got: %s" % response[0].jobid
        assert response[0].queue == 'default', "expected queue default, got: %s" % response[0].queue

    @raises(Cobalt.Exceptions.QueueError)
    def test_queuemanager_add_jobs_bad_spec(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'REJECT'}])
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512}]
        response = self.qm.add_jobs(job_specs)
        assert response[0].jobid == 1, "expected jobid 1, got: %s" % response[0].jobid
        assert response[0].queue == 'default', "expected queue default, got: %s" % response[0].queue

    @raises(RTAStub.BadMessage)
    def test_queuemanager_add_jobs_bad_message(self):
        Cobalt.Components.cqm.REALTIME_INTERFACE_ERRORS_FATAL = True
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'REJECT'}])
        RTAStub.verify_job.side_effect = RTAStub.BadMessage("Test BadMessage")
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512}]
        response = self.qm.add_jobs(job_specs)
        assert response[0].jobid == 1, "expected jobid 1, got: %s" % response[0].jobid
        assert response[0].queue == 'default', "expected queue default, got: %s" % response[0].queue

    @raises(RTAStub.ConnectionFailure)
    def test_queuemanager_add_jobs_connection_failure(self):
        Cobalt.Components.cqm.REALTIME_INTERFACE_ERRORS_FATAL = True
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'REJECT'}])
        RTAStub.verify_job.side_effect = RTAStub.ConnectionFailure("Test ConnectionFailure")
        job_specs = [{'walltime':300, 'queue':'default', 'nodes':512}]
        response = self.qm.add_jobs(job_specs)
        assert response[0].jobid == 1, "expected jobid 1, got: %s" % response[0].jobid
        assert response[0].queue == 'default', "expected queue default, got: %s" % response[0].queue

    def test_queuemanager_set_jobs_good_change(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        joblist = self.qm.set_jobs([{'tag':"job", 'queue':"*"}], {'jobname':"hello", 'walltime':600})
        assert len(joblist) == 1, 'Wrong number of jobs in joblist'
        assert joblist[0].walltime == 600, 'Walltime not set'

    @raises(Cobalt.Exceptions.JobValidationError)
    def test_queuemanager_set_jobs_bad_change(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'REJECT'}])
        self.qm.set_jobs([{'tag':"job", 'queue':"*"}], {'jobname':"hello", 'walltime':600})

    def test_queuemanager_set_jobs_bad_change_advisory(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'REJECT'}])
        joblist = self.qm.set_jobs([{'tag':"job", 'queue':"*"}], {'jobname':"hello", 'walltime':600}, advisory=True)
        assert len(joblist) == 1, 'Wrong number of jobs in joblist'
        assert joblist[0].walltime == 600, 'Walltime not set'

    @raises(RTAStub.BadMessage)
    def test_queuemanager_set_jobs_bad_message(self):
        Cobalt.Components.cqm.REALTIME_INTERFACE_ERRORS_FATAL = True
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        RTAStub.verify_job = MagicMock()
        RTAStub.verify_job.side_effect = RTAStub.BadMessage("Test BadMessage")
        joblist = self.qm.set_jobs([{'tag':"job", 'queue':"*"}], {'jobname':"hello", 'walltime':600})
        assert len(joblist) == 1, 'Wrong number of jobs in joblist'
        assert joblist[0].walltime == 600, 'Walltime not set'

    @raises(RTAStub.ConnectionFailure)
    def test_queuemanager_set_jobs_conenction_failure(self):
        Cobalt.Components.cqm.REALTIME_INTERFACE_ERRORS_FATAL = True
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        RTAStub.verify_job = MagicMock()
        RTAStub.verify_job.side_effect = RTAStub.ConnectionFailure("Test ConnectionFailure")
        joblist = self.qm.set_jobs([{'tag':"job", 'queue':"*"}], {'jobname':"hello", 'walltime':600})
        assert len(joblist) == 1, 'Wrong number of jobs in joblist'
        assert joblist[0].walltime == 600, 'Walltime not set'

    def test_queuemanager_set_jobs_bad_message_ignored(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        RTAStub.verify_job = MagicMock()
        RTAStub.verify_job.side_effect = RTAStub.BadMessage("Test BadMessage")
        joblist = self.qm.set_jobs([{'tag':"job", 'queue':"*"}], {'jobname':"hello", 'walltime':600})
        assert len(joblist) == 1, 'Wrong number of jobs in joblist'
        assert joblist[0].walltime == 600, 'Walltime not set'

    def test_queuemanager_set_jobs_conenction_failure_ignored(self):
        self.qm.add_queues([{'tag':"queue", 'name':"default"}])
        job_specs = [{'tag':'job', 'walltime':300, 'queue':'default', 'nodes':512}]
        response = None
        RTAStub.verify_job = MagicMock(return_value=[{'jobid':1, 'reason':'good msg', 'status':'ACCEPT'}])
        response = self.qm.add_jobs(job_specs)
        assert response is not None, "no job added, test cannot proceed."
        RTAStub.verify_job = MagicMock()
        RTAStub.verify_job.side_effect = RTAStub.ConnectionFailure("Test ConnectionFailure")
        joblist = self.qm.set_jobs([{'tag':"job", 'queue':"*"}], {'jobname':"hello", 'walltime':600})
        assert len(joblist) == 1, 'Wrong number of jobs in joblist'
        assert joblist[0].walltime == 600, 'Walltime not set'

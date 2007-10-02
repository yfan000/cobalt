import time

from Cobalt.Components.cqm import QueueManager


class TestQueueManager(object):
    
    def setup(self):
        self.cqm = QueueManager()
    
    def test_add_queues(self):
        self.cqm.add_queues([{'tag':"queue", 'name':"default"}])
        
        assert len(self.cqm.Queues) == 1
        assert 'default' in self.cqm.Queues
        assert self.cqm.Queues['default'].tag == 'queue'
        
    def test_get_queues(self):
         self.cqm.add_queues([{'tag':"queue", 'name':"default"}])
         
         results = self.cqm.get_queues([{'tag':"queue", 'name':"default"}])
         
         assert len(results) == 1
         assert results[0].name == 'default'
         
         self.cqm.add_queues([{'tag':"queue", 'name':"foo"}])
         self.cqm.add_queues([{'tag':"queue", 'name':"bar"}])
         
         results = self.cqm.get_queues([{'tag':"queue", 'name':"default"}])
         
         assert len(results) == 1
         assert results[0].name == 'default'

         results = self.cqm.get_queues([{'tag':"queue", 'name':"*"}])
         
         assert len(results) == 3
 
    # testing del_queues is going to require seeing what happens when you try to remove a queue
    # with jobs in it.  so defer that until job adding has been tested
    def test_del_queues(self):
        pass
    
    def test_setQueues(self):
         self.cqm.add_queues([{'tag':"queue", 'name':"default"}])
         
         self.cqm.setQueues([{'tag':"queue", 'name':"default"}], {'state':"bar"})
         results = self.cqm.get_queues([{'tag':"queue", 'name':"default"}])
         assert results[0].state == 'bar'

         self.cqm.add_queues([{'tag':"queue", 'name':"foo"}])
         self.cqm.add_queues([{'tag':"queue", 'name':"bar"}])
         self.cqm.setQueues([{'tag':"queue", 'name':"*"}], {'state':"bar"})
         results = self.cqm.get_queues([{'tag':"queue", 'name':"*"}])
    
         assert results[0].state == results[1].state == results[2].state == 'bar'
         
    def test_add_jobs(self):
        self.cqm.add_queues([{'tag':"queue", 'name':"default"}])
        self.cqm.add_queues([{'tag':"queue", 'name':"foo"}])
        self.cqm.add_queues([{'tag':"queue", 'name':"bar"}])

        self.cqm.add_jobs([{'tag':"job", 'queue':"default"}])
        
        results = self.cqm.get_queues([{'tag':"queue", 'name':"default"}])
        assert len(results[0].jobs) == 1
        
        results = self.cqm.get_queues([{'tag':"queue", 'name':"foo"}])
        assert len(results[0].jobs) == 0
        
        results = self.cqm.get_queues([{'tag':"queue", 'name':"bar"}])
        assert len(results[0].jobs) == 0

    def test_get_jobs(self):
        pass
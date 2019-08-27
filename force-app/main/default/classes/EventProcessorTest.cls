@isTest
private class EventProcessorTest {

    public class TestEventProcessor implements EventQueue.Processor {

        public String setKey(Map<String, Object> payload) {
            return 'TEST_' + payload.get('id');
        }

        public void process(List<EventQueue__c> events) {
            for (EventQueue__c event : events) {
                event.Status__c = 'Completed';
            }
        }

    }


    public class TestErrorHandling implements EventQueue.Processor {

        public String setKey(Map<String, Object> payload) {
            return 'TEST_' + payload.get('id');
        }

        public void process(List<EventQueue__c> events) {
            List<Account> recordsToUpsert = new List<Account>();
            for (EventQueue__c event : events) {
                recordsToUpsert.add(new Account()); // This should cause a caught DML exception
            }
            EventQueue.upsertRecords(recordsToUpsert, events, Account.SObjectType, Account.Fields.Id);
        }

    }

    public class TestBatchFailure implements EventQueue.Processor {

        public String setKey(Map<String, Object> payload) {
            return 'TEST_' + payload.get('id');
        }

        public void process(List<EventQueue__c> events) {
            insert new Account(); // This should cause an uncaught DML exception
        }

    }

    public class TestInvalidStatus implements EventQueue.Processor {

        public String setKey(Map<String, Object> payload) {
            return 'TEST_' + payload.get('id');
        }

        public void process(List<EventQueue__c> events) {
            for (EventQueue__c event : events) {
                // Not updating the status for an event throws an error by default
            }
        }

    }

    static testMethod void testEventProcessor() {
        EventQueue.setMetadataByType('TEST', new EventQueueType__mdt(DeveloperName='TEST', BatchSize__c=1, ClassName__c='EventProcessorTest.TestEventProcessor'));
        EventQueueType__mdt metadataType = EventQueue.getMetadataByType('TEST');
        Integer batchSize = metadataType.BatchSize__c.intValue();
        List<EventQueue__c> events = new List<EventQueue__c>();
        for (Integer i = 1; i <= batchSize; i++) {
            events.add(new EventQueue__c(Name='TEST', Payload__c=String.format('\'{\' "id": {0} \'}\'', new List<String>{ String.valueOf(i) })));
        }
        insert events;
        Test.startTest();
        Database.executeBatch(new EventQueueBatch(0), 1);
        Test.stopTest();
        events = [SELECT Name, Key__c, Payload__c, Status__c FROM EventQueue__c WHERE Status__c = 'Completed'];
        System.assertEquals(batchSize, events.size(), 'Events weren\'t completed');
    }

    static testMethod void testBatchFailure() {
        EventQueue.setMetadataByType('TEST', new EventQueueType__mdt(DeveloperName='TEST', BatchSize__c=1, ClassName__c='EventProcessorTest.TestBatchFailure'));
        EventQueueType__mdt metadataType = EventQueue.getMetadataByType('TEST');
        Integer batchSize = metadataType.BatchSize__c.intValue();
        List<EventQueue__c> events = new List<EventQueue__c>();
        for (Integer i = 1; i <= batchSize; i++) {
            events.add(new EventQueue__c(Name='TEST', Payload__c=String.format('\'{\' "id": {0} \'}\'', new List<String>{ String.valueOf(i) })));
        }
        insert events;
        Test.startTest();
        Database.executeBatch(new EventQueueBatch(0), 1);
        Test.stopTest();
        events = [SELECT LastErrorMessage__c FROM EventQueue__c WHERE Status__c = 'Queued' AND Attempts__c = 1];
        System.assertEquals(batchSize, events.size(), 'Events weren\'t requeued');
        System.assert(events[0].LastErrorMessage__c.contains('REQUIRED_FIELD_MISSING'), 'Last Error Message not set correctly, got: ' + events[0].LastErrorMessage__c);
    }

    static testMethod void testErrorHandling() {
        EventQueue.setMetadataByType('TEST', new EventQueueType__mdt(DeveloperName='TEST', BatchSize__c=1, ClassName__c='EventProcessorTest.TestErrorHandling'));
        EventQueueType__mdt metadataType = EventQueue.getMetadataByType('TEST');
        Integer batchSize = metadataType.BatchSize__c.intValue();
        List<EventQueue__c> events = new List<EventQueue__c>();
        for (Integer i = 1; i <= batchSize; i++) {
            events.add(new EventQueue__c(Name='TEST', Payload__c=String.format('\'{\' "id": {0} \'}\'', new List<String>{ String.valueOf(i) })));
        }
        insert events;
        Test.startTest();
        Database.executeBatch(new EventQueueBatch(0), 1);
        Test.stopTest();
        events = [SELECT Name, Key__c, Payload__c, Status__c FROM EventQueue__c WHERE Status__c = 'Queued' AND Attempts__c = 1];
        System.assertEquals(batchSize, events.size(), 'Events weren\'t requeued');
    }

    static testMethod void testInvalidStatus() {
        EventQueue.setMetadataByType('TEST', new EventQueueType__mdt(DeveloperName='TEST', BatchSize__c=1, ClassName__c='EventProcessorTest.TestInvalidStatus'));
        EventQueueType__mdt metadataType = EventQueue.getMetadataByType('TEST');
        Integer batchSize = metadataType.BatchSize__c.intValue();
        List<EventQueue__c> events = new List<EventQueue__c>();
        for (Integer i = 1; i <= batchSize; i++) {
            events.add(new EventQueue__c(Name='TEST', Payload__c=String.format('\'{\' "id": {0} \'}\'', new List<String>{ String.valueOf(i) })));
        }
        insert events;
        Test.startTest();
        Database.executeBatch(new EventQueueBatch(0), 1);
        Test.stopTest();
        events = [SELECT LastErrorMessage__c FROM EventQueue__c WHERE Status__c = 'Queued' AND Attempts__c = 1];
        System.assertEquals(batchSize, events.size(), 'Events weren\'t requeued');
        System.assert(events[0].LastErrorMessage__c.contains('Event status not set properly'), 'Last Error Message not set correctly, got: ' + events[0].LastErrorMessage__c);
    }

    static testMethod void testErrorMessageAppending() {
        EventQueue.setMetadataByType('TEST', new EventQueueType__mdt(DeveloperName='TEST', BatchSize__c=1, ClassName__c='EventProcessorTest.TestErrorHandling'));
        EventQueueType__mdt metadataType = EventQueue.getMetadataByType('TEST');
        Integer batchSize = metadataType.BatchSize__c.intValue();
        List<EventQueue__c> events = new List<EventQueue__c>();
        for (Integer i = 1; i <= batchSize; i++) {
            events.add(new EventQueue__c(Name='TEST', Attempts__c=1, LastErrorMessage__c='TEST_ERROR', Payload__c=String.format('\'{\' "id": {0} \'}\'', new List<String>{ String.valueOf(i) })));
        }
        insert events;
        // Set CreatedDate (which consequently sets LastModifiedDate) to a minute ago so the query will pick up the next attempt iteration
        for (EventQueue__c event : events) {
            Test.setCreatedDate(event.Id, Datetime.now().addMinutes(-1));
        }

        Map<String, Id> queuedEvents = new Map<String, Id>();
        for (AggregateResult queuedEvent : [SELECT Key__c, MAX(Id) Id FROM EventQueue__c WHERE Status__c = 'Queued' AND NextAttempt__c <= :Datetime.now() GROUP BY Key__c ORDER BY MIN(NextAttempt__c), MIN(Id) ASC LIMIT 1000]) {
            queuedEvents.put((String) queuedEvent.get('Key__c'), (Id) queuedEvent.get('Id'));
        }
        System.debug(queuedEvents);
        System.debug([SELECT Id, CreatedDate, LastModifiedDate, NextAttempt__c FROM EventQueue__c]);

        Test.startTest();
        Database.executeBatch(new EventQueueBatch(0), 1);
        Test.stopTest();
        events = [SELECT Name, Key__c, Payload__c, Status__c FROM EventQueue__c WHERE Status__c = 'Queued' AND Attempts__c = 2];
        System.assertEquals(batchSize, events.size(), 'Events weren\'t requeued');
    }

    static testMethod void testEventQueueIterable() {
        EventQueue.setMetadataByType('TEST', new EventQueueType__mdt(DeveloperName='TEST', BatchSize__c=1, ClassName__c='EventProcessorTest.TestEventProcessor'));
        EventQueueType__mdt metadataType = EventQueue.getMetadataByType('TEST');
        Integer batchSize = metadataType.BatchSize__c.intValue();
        List<EventQueue__c> events = new List<EventQueue__c>();
        for (Integer i = 1; i <= batchSize * 3 + 1; i++) {
            events.add(new EventQueue__c(Name='TEST', Payload__c=String.format('\'{\' "id": {0} \'}\'', new List<String>{ String.valueOf(i) })));
        }
        insert events;
        EventQueueIterable iterable = new EventQueueIterable();
        Integer count = 0;
        while (iterable.hasNext()) {
            count++;
            iterable.next();
        }
        System.assertEquals(4, count, 'Expected 4 batches');
    }

}

public class EventQueueBatch implements Database.Batchable<EventQueue.Wrapper>, Database.Stateful {

    private Integer chainCount;
    private Boolean hasRecords;

    public EventQueueBatch(Integer chainCount) {
        this.chainCount = chainCount;
        this.hasRecords = false;
    }

    public Iterable<EventQueue.Wrapper> start(Database.BatchableContext context) {
        return new EventQueueIterator();
    }

    public void execute(Database.BatchableContext context, List<EventQueue.Wrapper> wrappers) {
        System.assertEquals(1, wrappers.size(), 'Batch size must equal 1');
        this.hasRecords = true;
        EventQueue.Wrapper wrapper = wrappers[0];
        try {
            Type processorType = Type.forName(wrapper.className);
            EventQueue.Processor processor = (EventQueue.Processor) processorType.newInstance();
            processor.process(wrapper.events);
        } catch (Exception e) {
            for (EventQueue__c event : wrapper.events) {
                if (event.LastErrorMessage__c == null || event.LastErrorMessage__c == '') {
                    event.LastErrorMessage__c = Datetime.now().formatLong() + '\n' + e.getMessage();
                } else {
                    event.LastErrorMessage__c = Datetime.now().formatLong() + '\n' + e.getMessage() + '\n\n=========================================================\n\n' + event.LastErrorMessage__c;
                }
                event.Status__c = 'Failed';
            }
        }
        for (EventQueue__c event : wrapper.events) {
            event.Attempts__c = event.Attempts__c + 1;
            if (event.Status__c == 'Queued') {
                event.Status__c = 'Failed';
                if (event.LastErrorMessage__c == null || event.LastErrorMessage__c == '') {
                    event.LastErrorMessage__c = Datetime.now().formatLong() + '\n' + 'Event status not set properly';
                } else {
                    event.LastErrorMessage__c = Datetime.now().formatLong() + '\n' + 'Event status not set properly' + '\n\n=========================================================\n\n' + event.LastErrorMessage__c;
                }
            }
            if (event.Status__c == 'Failed' && event.Attempts__c < 10) {
                event.Status__c = 'Queued';
            }
        }
        update wrapper.events;
    }

    public void finish(Database.BatchableContext context) {
        if ([SELECT Id FROM EventQueue__c WHERE Status__c = 'Queued' AND NextAttempt__c <= :Datetime.now()].size() > 0) {
            if (!Test.isRunningTest()) Database.executeBatch(new EventQueueBatch(0), 1);
        } else {
            if (this.chainCount < 10) {
                this.chainCount++;
            }
            if (this.hasRecords) {
                this.chainCount = 1;
            }
            if (!Test.isRunningTest()) System.scheduleBatch(new EventQueueBatch(this.chainCount), 'EventQueueBatch', this.chainCount, 1);
        }
    }

}

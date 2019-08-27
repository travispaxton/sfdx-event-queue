public class EventQueueIterable implements Iterator<EventQueue.Wrapper> {

    private List<EventQueue__c> events;
    private Integer pointer;

    public EventQueueIterable() {
        // Get the latest queued events
        Map<String, Id> queuedEvents = new Map<String, Id>();
        for (AggregateResult queuedEvent : [SELECT Key__c, MAX(Id) Id FROM EventQueue__c WHERE Status__c = 'Queued' AND NextAttempt__c <= :Datetime.now() GROUP BY Key__c ORDER BY MIN(NextAttempt__c), MIN(Id) ASC LIMIT 1000]) {
            queuedEvents.put((String) queuedEvent.get('Key__c'), (Id) queuedEvent.get('Id'));
        }
        // Get the latest events for all keys from above
        Map<String, Id> latestEvents = new Map<String, Id>();
        for (AggregateResult latestEvent : [SELECT Key__c, MAX(Id) Id FROM EventQueue__c WHERE Key__c IN :queuedEvents.keySet() GROUP BY Key__c ORDER BY MIN(CreatedDate), MIN(Id)]) {
            latestEvents.put((String) latestEvent.get('Key__c'), (Id) latestEvent.get('Id'));
        }
        // Set all earlier events for keys to completed
        List<EventQueue__c> eventsToDiscard = [SELECT Id, Status__c FROM EventQueue__c WHERE Key__c IN :latestEvents.keySet() AND Id NOT IN :latestEvents.values() AND (Status__c = 'Queued' OR Status__c = 'Failed') LIMIT 1000];
        for (EventQueue__c eventToDiscard : eventsToDiscard) {
            eventToDiscard.Status__c = 'Completed';
        }
        update eventsToDiscard;
        // Clean up duplicate failures without later queued or completed events
        Map<String, Id> failedEvents = new Map<String, Id>();
        for (AggregateResult failedEvent : [SELECT Key__c, MAX(Id) Id FROM EventQueue__c WHERE Status__c = 'Failed' GROUP BY Key__c ORDER BY MIN(NextAttempt__c), MIN(Id) ASC LIMIT 100]) {
            failedEvents.put((String) failedEvent.get('Key__c'), (Id) failedEvent.get('Id'));
        }
        eventsToDiscard = [SELECT Id, Status__c FROM EventQueue__c WHERE Key__c IN :failedEvents.keySet() AND Id NOT IN :failedEvents.values() AND (Status__c = 'Failed') LIMIT 100];
        for (EventQueue__c eventToDiscard : eventsToDiscard) {
            eventToDiscard.Status__c = 'Completed';
        }
        update eventsToDiscard;
        // Get the latest queued events that are still queued after the cleanup
        queuedEvents = new Map<String, Id>();
        for (AggregateResult queuedEvent : [SELECT Key__c, MAX(Id) Id FROM EventQueue__c WHERE Status__c = 'Queued' AND Key__c IN :latestEvents.keySet() GROUP BY Key__c ORDER BY MIN(NextAttempt__c), MIN(Id) ASC LIMIT 1000]) {
            queuedEvents.put((String) queuedEvent.get('Key__c'), (Id) queuedEvent.get('Id'));
        }
        this.events = [SELECT Name, Status__c, Key__c, Payload__c, LastErrorMessage__c, Attempts__c FROM EventQueue__c WHERE Id IN :queuedEvents.values() ORDER BY Name, Id ASC];
        this.pointer = 0;
    }

    public Boolean hasNext() {
        if (this.pointer >= this.events.size()) {
            return false;
        } else {
            return true;
        }
    }

    public EventQueue.Wrapper next() {
        EventQueueType__mdt eventQueueType = EventQueue.getMetadataByType(this.events[this.pointer].Name);
        EventQueue.Wrapper wrapper = new EventQueue.Wrapper(this.events[this.pointer].Name, eventQueueType.ClassName__c);
        while (wrapper.events.size() < eventQueueType.BatchSize__c && this.hasNext() && wrapper.type == this.events[this.pointer].Name) {
            wrapper.events.add(this.events[this.pointer]);
            this.pointer++;
        }
        return wrapper;
    }

}

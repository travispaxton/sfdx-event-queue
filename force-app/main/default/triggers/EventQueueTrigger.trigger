trigger EventQueueTrigger on EventQueue__c (before insert) {

    Map<String, EventQueue.Processor> eventTypes = new Map<String, EventQueue.Processor>();
    
    for (EventQueue__c event : Trigger.new) {
        if (!eventTypes.containsKey(event.Name.toUpperCase())) {
            EventQueueType__mdt metadata = EventQueue.getMetadataByType(event.Name);
            Type processorType = Type.forName(metadata.ClassName__c);
            EventQueue.Processor processor = (EventQueue.Processor) processorType.newInstance();
            eventTypes.put(event.Name.toUpperCase(), processor);
        }
    }

    for (EventQueue__c event : Trigger.new) {
        Map<String, Object> payload = (Map<String, Object>) JSON.deserializeUntyped(event.Payload__c);
        event.Key__c = eventTypes.get(event.Name.toUpperCase()).setKey(payload);
    }

}
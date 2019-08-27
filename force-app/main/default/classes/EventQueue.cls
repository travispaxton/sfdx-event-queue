public class EventQueue {

    public static void upsertRecords(List<SObject> recordsToUpsert, List<EventQueue__c> events, Schema.SObjectType objectType, Schema.SObjectField lookupField) {
        System.assert(recordsToUpsert.size() == events.size(), 'List of events and records must be the same size');
        String objectName = objectType.getDescribe().getName();
        String lookupName = lookupField.getDescribe().getName();
        Map<String, Id> lookupMap = new Map<String, Id>();
        for (SObject record : recordsToUpsert) {
            lookupMap.put((String) record.get(lookupName), null);
        }
        List<String> lookupValues = new List<String>(lookupMap.keySet());
        List<SObject> lookupRecords = Database.query(String.format('SELECT Id, {1} FROM {0} WHERE {1} IN :lookupValues', new List<String>{
            objectName,
            lookupName
        }));
        for (SObject record : lookupRecords) {
            lookupMap.put((String) record.get(lookupName), record.Id);
        }
        for (SObject record : recordsToUpsert) {
            record.Id = lookupMap.get((String) record.get(lookupName));
        }
        List<Database.UpsertResult> upsertResults = Database.upsert(recordsToUpsert, false);
        for (Integer i = 0; i < upsertResults.size(); i++) {
            EventQueue__c event = events[i];
            if (upsertResults[i].isSuccess()) {
                event.Status__c = 'Completed';
            } else {
                for (Database.Error e : upsertResults[i].getErrors()) {
                    if (event.LastErrorMessage__c == null || event.LastErrorMessage__c == '') {
                        event.LastErrorMessage__c = Datetime.now().formatLong() + '\n' + e.getMessage();
                    } else {
                        event.LastErrorMessage__c = Datetime.now().formatLong() + '\n' + e.getMessage() + '\n\n=========================================================\n\n' + event.LastErrorMessage__c;
                    }
                }
                event.Status__c = 'Failed';
            }
        }
    }

    public interface Processor {
        String setKey(Map<String, Object> payload);
        void process(List<EventQueue__c> events);
    }

    public class Wrapper {
        public String type { get; set; }
        public String className { get; set; }
        public List<EventQueue__c> events { get; set; }

        public Wrapper(String type, String className) {
            this.type = type;
            this.className = className;
            this.events = new List<EventQueue__c>();
        }
    }

    public static EventQueueType__mdt getMetadataByType(String typeName) {
        if (EventQueue.metadataTypes.containsKey(typeName.toUpperCase())) {
            return EventQueue.metadataTypes.get(typeName.toUpperCase());
        } else {
            List<EventQueueType__mdt> eventQueueTypes = [SELECT ClassName__c, BatchSize__c FROM EventQueueType__mdt WHERE DeveloperName = :typeName AND Active__c = true];
            System.assertEquals(1, eventQueueTypes.size(), String.format('Event Queue Type not found for {0}', new List<String>{ typeName }));
            EventQueue.setMetadataByType(typeName, eventQueueTypes[0]);
            return eventQueueTypes[0];
        }
    }

    public static Map<String, EventQueueType__mdt> metadataTypes = new Map<String, EventQueueType__mdt>();

    public static void setMetadataByType(String typeName, EventQueueType__mdt metadataType) {
        EventQueue.metadataTypes.put(typeName.toUpperCase(), metadataType);
    }

}

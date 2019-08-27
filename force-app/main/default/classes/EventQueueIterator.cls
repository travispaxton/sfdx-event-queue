public class EventQueueIterator implements Iterable<EventQueue.Wrapper> {

    public Iterator<EventQueue.Wrapper> Iterator() {
        return new EventQueueIterable();
    }

}

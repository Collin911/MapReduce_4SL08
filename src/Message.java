import java.io.Serializable;

public class Message implements Serializable {
    public enum Type {
        TASK_ASSIGNMENT,
        TASK_DONE,
        WORD_PAIR,
        REQ_ACK,
        ACK,
        START_REDUCE,
        LOCAL_MIN_MAX,
        START_REDISTRIBUTE,
        REDISTRIBUTION,
        REDISTRIBUTION_DONE,
        SORT_AND_SEND_RESULT,
        FINAL_RESULT
    }

    public final Type type;
    public final String payload;
    public final Integer senderId;

    public Message(Type type, String payload, Integer senderId) {
        this.type = type;
        this.payload = payload;
        this.senderId = senderId;
    }

    @Override
    public String toString() {
        return "Message{" + "type=" + type + ", payload='" + payload + '\'' + '}';
    }
}
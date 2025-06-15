import java.io.Serializable;

public class WordPair implements Serializable {
    public String word;
    public int count;

    public WordPair(String word, int count) {
        this.word = word;
        this.count = count;
    }
}
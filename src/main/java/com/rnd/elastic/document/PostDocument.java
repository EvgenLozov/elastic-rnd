package com.rnd.elastic.document;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.springframework.data.elasticsearch.annotations.Document;

import java.time.Instant;

@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@Document(indexName = "post", type = "postdoc", createIndex = false)
public class PostDocument implements Versionable{

    private String id;

    private String title;
    private String content;
    private Instant createdAt;
    private Instant lastModified;
    private String author;

    private long seqNum = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;


    public PostDocument(String id, String title, String author) {
        this.id = id;
        this.title = title;
        this.author = author;
    }
}

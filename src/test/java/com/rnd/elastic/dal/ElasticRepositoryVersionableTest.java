package com.rnd.elastic.dal;

import com.rnd.elastic.document.PostDocument;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.EntityMapper;
import org.springframework.data.elasticsearch.core.ResultsMapper;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.query.IndexQuery;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ElasticRepositoryVersionableTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private SearchResultMapper searchResultMapper;
    @Autowired
    private EntityMapper entityMapper;

    private ElasticRepositoryVersionable elasticRepository;

    @BeforeEach
    void setUp() {
        elasticRepository = new DefaultElasticRepositoryVersionable(elasticsearchTemplate,
                                    new SeqNumPrimaryTermSearchResultMapper(searchResultMapper), entityMapper);
        elasticsearchTemplate.createIndex(PostDocument.class);
    }

    @AfterEach
    void tearDown() {
        elasticsearchTemplate.deleteIndex(PostDocument.class);
    }

    @Test
    void search() {
        var postDocument = new PostDocument(UUID.randomUUID().toString());
        postDocument.setAuthor("test author");
        postDocument.setTitle("testTitle");

        var indexQuery = new IndexQuery();
        indexQuery.setId(postDocument.getId());
        indexQuery.setIndexName("post");
        indexQuery.setType("postdoc");
        indexQuery.setObject(postDocument);

        elasticsearchTemplate.index(indexQuery);
        elasticsearchTemplate.refresh(PostDocument.class);

        List<PostDocument> postDocuments = elasticRepository.search(QueryBuilders.matchQuery("author", "test"), PostDocument.class);

        assertEquals(1, postDocuments.size());
        assertEquals(postDocument.getId(), postDocuments.get(0).getId());
        assertEquals(postDocument.getAuthor(), postDocuments.get(0).getAuthor());
        assertEquals(postDocument.getTitle(), postDocuments.get(0).getTitle());
        assertEquals(0, postDocuments.get(0).getSeqNum());
        assertEquals(1, postDocuments.get(0).getPrimaryTerm());
    }

    @Test
    void index() {
        var postDocument = new PostDocument(UUID.randomUUID().toString());
        postDocument.setAuthor("test author");
        postDocument.setTitle("testTitle");

        PostDocument saved = elasticRepository.index(postDocument);

        assertEquals(0, saved.getSeqNum());
        assertEquals(1, saved.getPrimaryTerm());

        saved.setContent("test");
        elasticRepository.index(saved);

        assertEquals(1, saved.getSeqNum());
        assertEquals(1, saved.getPrimaryTerm());
    }
}
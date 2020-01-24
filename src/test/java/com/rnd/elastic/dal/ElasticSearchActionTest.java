package com.rnd.elastic.dal;

import com.rnd.elastic.document.PostDocument;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsMapper;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class ElasticSearchActionTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private ResultsMapper resultsMapper;

    ElasticsearchSearchAction searchAction;

    @BeforeEach
    void setUp() {
        searchAction = new ElasticsearchSearchAction(elasticsearchTemplate.getClient());
        elasticsearchTemplate.createIndex(PostDocument.class);
    }

    @AfterEach
    void tearDown() {
        elasticsearchTemplate.deleteIndex(PostDocument.class);
    }

    @Test
    void search() {
        var postDocument = new PostDocument(UUID.randomUUID().toString(), "testTitle", "test author");
        index(postDocument);

        SearchResultMapper resultMapper = new SeqNumPrimaryTermSearchResultMapper(resultsMapper);
        Supplier<QueryBuilder> querySupplier =  () -> QueryBuilders.matchQuery("id", postDocument.getId());
        Function<SearchResponse, Optional<PostDocument>> responseHandler =
                (searchResponse) -> resultMapper.mapResults(searchResponse, PostDocument.class, Pageable.unpaged())
                        .getContent().stream().findFirst();

        var storedPost = searchAction.execute(new String[]{"post"}, querySupplier, responseHandler);

        PostDocument updatedPost = storedPost.map(post -> {
            post.setContent("It is a new post");
            return post;
        }).orElseThrow(() -> new RuntimeException("Post is not found"));

        index(updatedPost);

        storedPost = searchAction.execute(new String[]{"post"}, querySupplier, responseHandler);

        assertTrue(storedPost.isPresent());
        assertTrue(storedPost.get().getSeqNum() >= 0);
        assertTrue(storedPost.get().getPrimaryTerm() > 0);
        assertEquals(storedPost.get().getContent(), updatedPost.getContent());
    }

    private void index(PostDocument postDocument) {
        var indexQuery = new IndexQueryBuilder()
                .withIndexName("post")
                .withType("postdoc")
                .withObject(postDocument)
            .build();
        elasticsearchTemplate.index(indexQuery);
        elasticsearchTemplate.refresh(PostDocument.class);
    }
}
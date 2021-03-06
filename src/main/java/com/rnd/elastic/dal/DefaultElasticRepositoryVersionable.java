package com.rnd.elastic.dal;

import com.rnd.elastic.document.Versionable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.EntityMapper;
import org.springframework.data.elasticsearch.core.SearchResultMapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DefaultElasticRepositoryVersionable implements ElasticRepositoryVersionable {

    private static final int DEFAULT_FROM = 0;
    private static final int DEFAULT_SIZE = 50;

    private ElasticsearchTemplate elasticsearchTemplate;
    private SearchResultMapper resultsMapper;
    private EntityMapper entityMapper;

    public DefaultElasticRepositoryVersionable(ElasticsearchTemplate elasticsearchTemplate,
                                               SearchResultMapper resultsMapper, EntityMapper entityMapper) {
        this.elasticsearchTemplate = elasticsearchTemplate;
        this.resultsMapper = resultsMapper;
        this.entityMapper = entityMapper;
    }

    @Override
    public <T extends Versionable> List<T> search(QueryBuilder qb, Class<T> docClass) {

        var searchResponse = elasticsearchTemplate.getClient().prepareSearch(getIndexName(docClass))
                .setQuery(qb)
                .seqNoAndPrimaryTerm(true)
                .setFrom(DEFAULT_FROM)
                .setSize(DEFAULT_SIZE)
                .get();

        return resultsMapper.mapResults(searchResponse, docClass, Pageable.unpaged()).getContent();
    }

    @Override
    public <T extends Versionable> T index(T doc) throws VersionConflictException {
        try {
            var indexResponse = elasticsearchTemplate.getClient().index(prepareIndexRequest(doc)).actionGet();

            doc.setSeqNum(indexResponse.getSeqNo());
            doc.setPrimaryTerm(indexResponse.getPrimaryTerm());
        } catch (VersionConflictEngineException e) {
            throw new VersionConflictException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return doc;
    }

    @Override
    public <T extends Versionable> BulkResponse bulkIndex(List<T> docs) {
        var bulkRequest = new BulkRequest();

        bulkRequest.add(
                docs.stream()
                        .map(this::prepareIndexRequest)
                        .collect(Collectors.toList())
        );

        return elasticsearchTemplate.getClient().bulk(bulkRequest).actionGet();
    }

    private <T extends Versionable> IndexRequest prepareIndexRequest(T doc) {
        var indexRequest = new IndexRequest(getIndexName(doc.getClass()), getIndexType(doc.getClass()), doc.getId());
        indexRequest.setIfSeqNo(doc.getSeqNum());
        indexRequest.setIfPrimaryTerm(doc.getPrimaryTerm());
        indexRequest.source(convertToMap(doc), XContentType.JSON);

        return indexRequest;
    }

    private Map<String, Object> convertToMap(Object doc) {
        return entityMapper.mapObject(doc);
    }

    private String getIndexName(Class<?> docClass) {
        return elasticsearchTemplate.getPersistentEntityFor(docClass).getIndexName();
    }

    private String getIndexType(Class<?> docClass) {
        return elasticsearchTemplate.getPersistentEntityFor(docClass).getIndexType();
    }
}

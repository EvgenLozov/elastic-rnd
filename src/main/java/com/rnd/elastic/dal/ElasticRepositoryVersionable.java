package com.rnd.elastic.dal;

import com.rnd.elastic.document.Versionable;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

public interface ElasticRepositoryVersionable {
    <T extends Versionable> List<T> search(QueryBuilder qb, Class<T> docClass);
    <T extends Versionable> T index(T doc) throws VersionConflictException;
    <T extends Versionable> BulkResponse bulkIndex(List<T> docs);
}

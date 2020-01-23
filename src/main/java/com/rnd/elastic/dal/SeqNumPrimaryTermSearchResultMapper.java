package com.rnd.elastic.dal;

import com.rnd.elastic.document.Versionable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.SearchHit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;

import java.util.HashMap;
import java.util.Map;

public class SeqNumPrimaryTermSearchResultMapper implements SearchResultMapper {

    private SearchResultMapper resultsMapper;

    public SeqNumPrimaryTermSearchResultMapper(SearchResultMapper resultsMapper) {
        this.resultsMapper = resultsMapper;
    }

    @Override
    public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> clazz, Pageable pageable) {
        var page = resultsMapper.mapResults(response, clazz, pageable);

        Map<String, Long> seqNumsByIds = new HashMap<>();
        Map<String, Long> primaryTermByIds = new HashMap<>();

        response.getHits().forEach(searchHit -> {
            seqNumsByIds.put(searchHit.getId(), searchHit.getSeqNo());
            primaryTermByIds.put(searchHit.getId(), searchHit.getPrimaryTerm());
        });

        page.getContent().forEach(pageItem -> {
                    Versionable versionable = (Versionable) pageItem;
                    versionable.setSeqNum(seqNumsByIds.getOrDefault(versionable.getId(), SequenceNumbers.UNASSIGNED_SEQ_NO));
                    versionable.setPrimaryTerm(primaryTermByIds.getOrDefault(versionable.getId(), SequenceNumbers.UNASSIGNED_PRIMARY_TERM));
                });

        return page;
    }

    @Override
    public <T> T mapSearchHit(SearchHit searchHit, Class<T> type) {
        T result = resultsMapper.mapSearchHit(searchHit, type);
        if (result != null && Versionable.class.isAssignableFrom(type)){
            Versionable versionable = (Versionable) result;
            versionable.setSeqNum(searchHit.getSeqNo());
            versionable.setPrimaryTerm(searchHit.getPrimaryTerm());
        }
        return result;
    }
}

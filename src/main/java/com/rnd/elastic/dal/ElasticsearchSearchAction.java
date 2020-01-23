package com.rnd.elastic.dal;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.function.Function;
import java.util.function.Supplier;

public class ElasticsearchSearchAction {

    private Client client;

    public ElasticsearchSearchAction(Client client) {
        this.client = client;
    }

    public <T> T execute(String[] indexes, Supplier<QueryBuilder> querySupplier, Function<SearchResponse, T> responseHandler)
    {
        SearchResponse searchResponse = client.prepareSearch(indexes)
                .setQuery(querySupplier.get())
                .seqNoAndPrimaryTerm(true)
                .setSize(50)
                .execute()
                .actionGet();

        return responseHandler.apply(searchResponse);
    }
}

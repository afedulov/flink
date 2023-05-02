package org.apache.flink.table.gateway.rest.header.util;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkNotNull;

public class UrlPrefixDecorator<
                R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
        implements SqlGatewayMessageHeaders<R, P, M> {

    private final String prefixedUrl;
    private final SqlGatewayMessageHeaders<R, P, M> decorated;

    public UrlPrefixDecorator(SqlGatewayMessageHeaders<R, P, M> messageHeaders, String urlPrefix) {
        checkNotNull(messageHeaders);
        checkNotNull(urlPrefix);
        this.decorated = messageHeaders;
        this.prefixedUrl =
                urlPrefix
                        + "/"
                        + RestClient.VERSION_PLACEHOLDER
                        + messageHeaders.getTargetRestEndpointURL();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return decorated.getHttpMethod();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return prefixedUrl;
    }

    @Override
    public Class<P> getResponseClass() {
        return decorated.getResponseClass();
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return decorated.getResponseStatusCode();
    }

    @Override
    public String getDescription() {
        return decorated.getDescription();
    }

    @Override
    public Class<R> getRequestClass() {
        return decorated.getRequestClass();
    }

    @Override
    public M getUnresolvedMessageParameters() {
        return decorated.getUnresolvedMessageParameters();
    }
}

package com.tsurugidb.tsubakuro.auth.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;

/**
 * An implementation of {@link TokenProvider} which communicates with authentication server over HTTP(s).
 */
public class HttpTokenProvider implements TokenProvider {

    static final String PATH_ISSUE = "issue"; //$NON-NLS-1$

    static final String PATH_REFRESH = "refresh"; //$NON-NLS-1$

    static final String PATH_VERIFY = "verify"; //$NON-NLS-1$

    static final String KEY_CONTENT_TYPE = "Content-Type"; //$NON-NLS-1$

    static final String KEY_AUTHORIZATION = "Authorization"; //$NON-NLS-1$

    static final String PREFIX_BASIC = "Basic"; //$NON-NLS-1$

    static final String PREFIX_BEARER = "Bearer"; //$NON-NLS-1$

    static final String KEY_TOKEN_EXPIRATION = "X-Harinoki-Token-Expiration"; //$NON-NLS-1$

    static final String FIELD_TOKEN = "token"; //$NON-NLS-1$

    static final String FIELD_TYPE = "type"; //$NON-NLS-1$

    static final String FIELD_MESSAGE = "message"; //$NON-NLS-1$

    static final Logger LOG = LoggerFactory.getLogger(HttpTokenProvider.class);

    private final URI endpoint;

    private final HttpClient client;

    /**
     * Creates a new instance.
     * @param endpoint the server end-point URI; only considers scheme, host, port, and path
     */
    public HttpTokenProvider(@Nonnull String endpoint) {
        this(
                URI.create(Objects.requireNonNull(endpoint)),
                HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .build());
    }

    /**
     * Creates a new instance.
     * @param endpoint the server end-point URI; only considers scheme, host, port, and path
     * @param client the HTTP client
     */
    public HttpTokenProvider(URI endpoint, HttpClient client) {
        Objects.requireNonNull(endpoint);
        Objects.requireNonNull(client);
        this.endpoint = normalize(endpoint);
        this.client = client;
    }

    private static URI normalize(URI endpoint) {
        String path = endpoint.getPath();
        if (path == null) {
            path = "/"; //$NON-NLS-1$
        }
        if (!path.endsWith("/")) {
            path = path + "/"; //$NON-NLS-1$
        }

        try {
            return new URI(
                    endpoint.getScheme(),
                    null,
                    endpoint.getHost(),
                    endpoint.getPort(),
                    path,
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns the end-point URI.
     * @return the end-point URI
     */
    public URI getEndpoint() {
        return endpoint;
    }

    /**
     * Returns the HTTP client.
     * @return the HTTP client
     */
    public HttpClient getClient() {
        return client;
    }

    @Override
    public String issue(@Nonnull String user, @Nonnull String password)
            throws InterruptedException, IOException, CoreServiceException {
        Objects.requireNonNull(user);
        Objects.requireNonNull(password);
        var target = buildUri(PATH_ISSUE);
        String credential = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", user, password) //$NON-NLS-1$
                        .getBytes(StandardCharsets.UTF_8));
        var request = HttpRequest.newBuilder()
                .uri(target)
                .header(KEY_AUTHORIZATION, String.format("%s %s", PREFIX_BASIC, credential))
                .GET();
        var response = submit(request.build());
        if (response.status == 200) {
            return checkToken(target, response);
        }
        if (response.status == 401 || response.status == 403) {
            throw new CoreServiceException(
                    CoreServiceCode.AUTHENTICATION_ERROR,
                    response.message("invalid username/password"));
        }
        handleError(target, response);
        throw new AssertionError(); // unreachable
    }

    @Override
    public String refresh(@Nonnull String token, long expiration, @Nonnull TimeUnit unit)
            throws InterruptedException, IOException, CoreServiceException {
        Objects.requireNonNull(token);
        Objects.requireNonNull(unit);
        var target = buildUri(PATH_REFRESH);
        var request = HttpRequest.newBuilder()
                .uri(target)
                .header(KEY_AUTHORIZATION, String.format("%s %s", PREFIX_BEARER, token))
                .GET();
        if (expiration > 0) {
            request.header(KEY_TOKEN_EXPIRATION, String.valueOf(Math.max(1, unit.toSeconds(expiration))));
        }

        var response = submit(request.build());
        if (response.status == 200) {
            return checkToken(target, response);
        }
        handleError(target, response);
        throw new AssertionError(); // unreachable
    }

    @Override
    public void verify(String token) throws InterruptedException, IOException, CoreServiceException {
        Objects.requireNonNull(token);
        var target = buildUri(PATH_VERIFY);
        var request = HttpRequest.newBuilder()
                .uri(target)
                .header(KEY_AUTHORIZATION, String.format("%s %s", PREFIX_BEARER, token))
                .GET();

        var response = submit(request.build());
        if (response.status == 200) {
            return;
        }
        handleError(target, response);
        throw new AssertionError(); // unreachable
    }

    private URI buildUri(String path) {
        try {
            return new URI(
                    endpoint.getScheme(),
                    null,
                    endpoint.getHost(),
                    endpoint.getPort(),
                    endpoint.getPath() + path,
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String checkToken(URI target, Response response) throws InvalidResponseException {
        if (response.token == null) {
            throw new InvalidResponseException(MessageFormat.format(
                    "invalid authentication response (missing token): uri={0}, status={1}, type={2}",
                    target,
                    response.status,
                    response.type));
        }
        return response.token;
    }

    private static void handleError(URI target, Response response)
            throws CoreServiceException, InvalidResponseException {
        if (response.status == 401) {
            switch (response.type) {
            case AUTH_ERROR:
                throw new CoreServiceException(
                        CoreServiceCode.AUTHENTICATION_ERROR,
                        response.message("invalid username/password"));
            case INVALID_AUDIENCE:
                throw new CoreServiceException(
                        CoreServiceCode.AUTHENTICATION_ERROR,
                        response.message("unacceptable token (invalid audience)"));
            case INVALID_TOKEN:
                throw new CoreServiceException(
                        CoreServiceCode.BROKEN_CREDENTIAL,
                        response.message("authentication token was unrecognized"));
            case NO_TOKEN:
                throw new CoreServiceException(
                        CoreServiceCode.AUTHENTICATION_ERROR,
                        response.message("authentication token was not specified"));
            case TOKEN_EXPIRED:
                throw new CoreServiceException(
                        CoreServiceCode.REFRESH_EXPIRED,
                        response.message("refresh token was expired"));
            default:
                throw new CoreServiceException(
                        CoreServiceCode.AUTHENTICATION_ERROR,
                        response.message("authentication failed"));
            }
        }
        if (500 <= response.status && response.status <= 599) {
            throw new CoreServiceException(
                    CoreServiceCode.SYSTEM_ERROR,
                    response.message(MessageFormat.format(
                            "authentication server is not available: HTTP status code={0}",
                            response.status)));
        }
        throw new InvalidResponseException(MessageFormat.format(
                "authentication service provides unrecognized message: uri={0}, status={1}",
                target,
                response.status));
    }

    Response submit(HttpRequest request) throws IOException, InterruptedException {
        LOG.debug("auth request: uri={}", request.uri()); //$NON-NLS-1$
        var response = client.send(request, BodyHandlers.ofString());
        LOG.trace("auth response: uri={}, status={}", response.uri(), response.statusCode()); //$NON-NLS-1$
        LOG.trace("auth response: uri={}, body={}", response.uri(), response.body()); //$NON-NLS-1$

        var result = analyze(response);
        LOG.debug("auth response: uri={}, result={}", response.uri(), result);

        return result;
    }

    private static Response analyze(HttpResponse<String> response) throws InvalidResponseException {
        if (response.headers().firstValue(KEY_CONTENT_TYPE)
                .filter(it -> it.contains("/json"))
                .isPresent()) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode tree;
            try {
                tree = mapper.readTree(response.body());
            } catch (JsonProcessingException e) {
                throw new InvalidResponseException(MessageFormat.format(
                        "invalid authentication response (broken message): uri={0}, status={1}",
                        response.uri(),
                        response.statusCode()), e);
            }
            var result = new Response(
                    response.statusCode(),
                    Optional.ofNullable(toString(tree.get(FIELD_TYPE)))
                        .map(MessageType::deserialize)
                        .orElse(MessageType.UNKNOWN),
                    toString(tree.get(FIELD_TOKEN)),
                    toString(tree.get(FIELD_MESSAGE)));
            return result;
        }
        return new Response(response.statusCode(), MessageType.UNKNOWN, null, null);
    }

    private static String toString(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asText();
    }

    static class Response {

        int status;
        MessageType type;


        String token;

        String message;

        Response(int status, MessageType type, String token, String message) {
            assert type != null;
            this.status = status;
            this.type = type;
            this.token = token;
            this.message = message;
        }

        String message(String defaultMessage) {
            if (message != null) {
                return message;
            }
            return defaultMessage;
        }

        @Override
        public String toString() {
            return String.format(
                    "Response [status=%s, type=%s, token=%s, message=%s]", //$NON-NLS-1$
                    status, token, type, message);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        HttpTokenProvider other = (HttpTokenProvider) obj;
        return Objects.equals(endpoint, other.endpoint);
    }

    @Override
    public String toString() {
        return String.format("HttpTokenProvider(%s)", endpoint);
    }
}

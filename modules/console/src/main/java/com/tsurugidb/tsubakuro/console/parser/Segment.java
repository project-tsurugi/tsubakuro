package com.tsurugidb.tsubakuro.console.parser;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.console.model.Region;

/**
 * Represents a segment of each statement.
 */
class Segment {

    static final Logger LOG = LoggerFactory.getLogger(Segment.class);

    private final String text;

    private final long offset;

    private final List<TokenInfo> tokens;

    private final List<TokenInfo> comments;

    /**
     * A builder of {@link Segment}.
     */
    static class Builder {

        private long offset = -1L;

        private final StringBuilder text = new StringBuilder();

        private final List<TokenInfo> tokens = new ArrayList<>();

        private final List<TokenInfo> comments = new ArrayList<>();

        /**
         * Creates a new instance.
         */
        Builder() {
            super();
        }

        /**
         * Returns whether or not this builder is initialized.
         * @return {@code true} if this is initialized, otherwise {@code false}
         * @see #initialize(long)
         */
        boolean isInitialized() {
            return offset >= 0;
        }

        /**
         * Initializes this builder.
         * @param newOffset the starting offset of the segment in the document
         */
        void initialize(long newOffset) {
            if (isInitialized()) {
                throw new IllegalStateException("segment is already initialized");
            }
            offset = newOffset;
        }

        /**
         * Returns the number of characters are added to this segment.
         * @return the number of characters
         * @see #append(String)
         */
        int size() {
            return text.length();
        }

        /**
         * Returns offset position from this segment.
         * @param position the character position in the document
         * @return the offset from the segment
         */
        int relative(long position) {
            checkInitialized();
            if (position < offset) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "invalid message position: {0} (must be >= {1})",
                        position,
                        offset));
            }
            return (int) (position - offset);
        }

        /**
         * Appends segment contents.
         * @param snippet the snippet of the contents
         */
        void append(@Nonnull String snippet) {
            Objects.requireNonNull(snippet);
            checkInitialized();
            text.append(snippet);
        }

        /**
         * Adds a splitted token.
         * @param token the token in this segment, may be a comment
         */
        void addToken(@Nonnull TokenInfo token) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("token: kind={}, position={}, delimiter={}, line={}, column={}, text={}", new Object[] { //$NON-NLS-1$
                        token.getKind(),
                        offset + token.getOffset(),
                        token.getKind().isStatementDelimiter(),
                        token.getStartLine(),
                        token.getStartColumn(),
                        getText(token).map(it -> "'" + it + "'").orElse("N/A") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                });
            }
            Objects.requireNonNull(token);
            checkInitialized();
            switch (token.getKind()) {
            case BLOCK_COMMENT:
            case SLASH_COMMENT:
            case HYPHEN_COMMENT:
                comments.add(token);
                break;

            default:
                tokens.add(token);
                break;
            }
        }

        Optional<String> getText(@Nonnull TokenInfo token) {
            Objects.requireNonNull(token);
            if (token.getOffset() + token.getLength() <= text.length()) {
                return Optional.of(text.substring(token.getOffset(), token.getOffset() + token.getLength()));
            }
            return Optional.empty();
        }

        /**
         * Builds a segment from previously added contents and tokens.
         * After this operation, this builder becomes not initialized.
         * @return the built segment
         */
        Segment build() {
            checkInitialized();
            normalizeDelimiter();
            Segment result = new Segment(text.toString(), offset, tokens, comments);
            offset = -1L;
            text.setLength(0);
            tokens.clear();
            comments.clear();
            return result;
        }

        private void normalizeDelimiter() {
            if (!tokens.isEmpty()) {
                var last = tokens.get(tokens.size() - 1);
                if (last.getKind() == TokenKind.EOF || last.getKind() == TokenKind.SEMICOLON) {
                    tokens.set(tokens.size() - 1, new TokenInfo(
                            TokenKind.END_OF_STATEMENT,
                            last.getOffset(),
                            0,
                            last.getStartLine(),
                            last.getStartColumn()));
                    int size = Stream.concat(tokens.stream(), comments.stream())
                            .mapToInt(it -> it.getOffset() + it.getLength())
                            .max()
                            .orElse(0);
                    text.setLength(size);
                }
            }
        }

        private void checkInitialized() {
            if (!isInitialized()) {
                throw new IllegalStateException("segment must be initialized"); //$NON-NLS-1$
            }
        }
    }

    /**
     * Creates a new instance.
     * @param text the segment text.
     * @param offset the segment offset in the document
     * @param tokens the tokens except comments
     * @param comments the comments
     */
    Segment(
            @Nonnull String text,
            long offset,
            @Nonnull List<? extends TokenInfo> tokens,
            @Nonnull List<? extends TokenInfo> comments) {
        this.text = text;
        this.offset = offset;
        this.tokens = List.copyOf(tokens);
        this.comments = List.copyOf(comments);
    }

    /**
     * Returns the segment text.
     * @return the segment text
     */
    String getText() {
        return text;
    }

    /**
     * Returns the text snippet.
     * @param region the target region
     * @return the text snippet
     */
    Optional<String> getText(@Nonnull Region region) {
        Objects.requireNonNull(region);
        int start = (int) (region.getPosition() - offset);
        if (start >= 0 && start + region.getSize() <= text.length()) {
            return Optional.of(text.substring(start, start + region.getSize()));
        }
        return Optional.empty();
    }

    /**
     * Returns the token text.
     * @param token the target token
     * @return the token text
     */
    Optional<String> getText(@Nonnull TokenInfo token) {
        Objects.requireNonNull(token);
        if (token.getOffset() + token.getLength() <= text.length()) {
            return Optional.of(text.substring(token.getOffset(), token.getOffset() + token.getLength()));
        }
        return Optional.empty();
    }

    /**
     * Returns the starting offset of this segment in the document.
     * @return the starting offset
     */
    long getOffset() {
        return offset;
    }

    /**
     * the starting line number in the document (0-origin).
     * @return the line number
     */
    int getStartLine() {
        return getFirstToken()
                .map(TokenInfo::getStartLine)
                .orElse(0);
    }

    /**
     * the starting column number in the document (0-origin).
     * @return the column number
     */
    int getStartColumn() {
        return getFirstToken()
                .map(TokenInfo::getStartColumn)
                .orElse(0);
    }

    private Optional<TokenInfo> getFirstToken() {
        return Stream.concat(tokens.stream().findFirst().stream(), comments.stream().findFirst().stream())
                .min(Comparator.comparing(TokenInfo::getOffset));
    }

    /**
     * Returns the tokens in this segment except comments.
     * @return the tokens
     * @see #getComments()
     */
    List<TokenInfo> getTokens() {
        return tokens;
    }

    /**
     * Returns the comments in this segment.
     * @return the comments
     */
    List<TokenInfo> getComments() {
        return comments;
    }
}

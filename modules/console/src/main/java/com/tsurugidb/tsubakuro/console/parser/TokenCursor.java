package com.tsurugidb.tsubakuro.console.parser;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.console.model.Region;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class TokenCursor {

    private final Segment segment;

    private int cursor = 0;

    TokenCursor(@Nonnull Segment segment) {
        Objects.requireNonNull(segment);
        this.segment = segment;
    }

    Optional<TokenInfo> lookahead(int offset) {
        var ts = segment.getTokens();
        if (cursor + offset >= ts.size()) {
            return Optional.empty();
        }
        return Optional.of(ts.get(cursor + offset));
    }

    TokenInfo token(int offset) {
        return checkOffset(offset);
    }

    Region region(int offset) {
        var token = checkOffset(offset);
        return new Region(
                segment.getOffset() + token.getOffset(),
                token.getLength(),
                token.getStartLine(),
                token.getStartColumn());
    }

    String text(int offset) {
        var token = checkOffset(offset);
        return segment.getText(token).get();
    }

    Region region(int first, int last) {
        return region(first).union(region(last));
    }

    @SuppressFBWarnings(
            value = "RV",
            justification = "misdetection: segment.getTokens() may raise an exception")
    public void consume(int count) {
        checkOffset(count - 1);
        cursor += count;
    }

    private TokenInfo checkOffset(int offset) {
        var ts = segment.getTokens();
        if (cursor + offset >= ts.size()) {
            throw new IndexOutOfBoundsException("cursor index out of bounds"); //$NON-NLS-1$
        }
        return ts.get(cursor + offset);
    }
}

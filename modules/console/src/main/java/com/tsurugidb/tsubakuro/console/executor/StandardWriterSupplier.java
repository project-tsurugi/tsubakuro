package com.tsurugidb.tsubakuro.console.executor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Provides standard output.
 */
public class StandardWriterSupplier implements IoSupplier<Writer> {

    private final Charset charset;

    /**
     * Creates a new instance with the default charset.
     */
    public StandardWriterSupplier() {
        this(Charset.defaultCharset());
    }

    /**
     * Creates a new instance.
     * @param charset the output charset
     */
    public StandardWriterSupplier(@Nonnull Charset charset) {
        Objects.requireNonNull(charset);
        this.charset = charset;
    }

    @Override
    public Writer get() throws IOException {
        return new OutputStreamWriter(System.out, charset);
    }
}

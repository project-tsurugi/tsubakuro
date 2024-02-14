package com.tsurugidb.tsubakuro.util;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A basic implementation of {@link DocumentSnippet}.
 */
public class BasicDocumentSnippet implements DocumentSnippet {

    private final List<String> description;

    private final List<String> notes;

    private final List<Reference> references;

    /**
     * Creates an empty instance.
     */
    public BasicDocumentSnippet() {
        this(List.of(), List.of(), List.of());
    }

    /**
     * Creates a new instance.
     * @param description the description in the document snippet
     * @param notes additional notesin the document snippet
     * @param references optional references in the document snippet
     */
    public BasicDocumentSnippet(
            @Nonnull List<? extends String> description,
            @Nonnull List<? extends String> notes,
            @Nonnull List<? extends Reference> references) {
        Objects.requireNonNull(description);
        Objects.requireNonNull(notes);
        Objects.requireNonNull(references);
        this.description = List.copyOf(description);
        this.notes = List.copyOf(notes);
        this.references = List.copyOf(references);
    }

    /**
     * Creates a new instance.
     * @param description the description in the document snippet
     * @return the created instance
     */
    public static BasicDocumentSnippet of(@Nonnull String... description) {
        Objects.requireNonNull(description);
        return new BasicDocumentSnippet(Arrays.asList(description), List.of(), List.of());
    }

    /**
     * Creates a new instance from the {@code Doc} annotation.
     * @param annotation the source annotation
     * @return the corresponding value
     */
    public static BasicDocumentSnippet of(@Nonnull Doc annotation) {
        var description = Arrays.asList(annotation.value());
        var notes = Arrays.asList(annotation.note());
        var references = Arrays.stream(annotation.reference())
                .filter(it -> !it.isBlank())
                .map(it -> {
                    var delimAt = it.lastIndexOf(Doc.REFERENCE_DELIMITER);
                    if (delimAt < 0) {
                        return new BasicReference(it, null);
                    }
                    var title = it.substring(0, delimAt).strip();
                    var location = it.substring(delimAt + 1).strip();
                    return new BasicReference(location, title);
                })
                .collect(Collectors.toList());
        return new BasicDocumentSnippet(description, notes, references);
    }

    @Override
    public List<String> getDescription() {
        return description;
    }

    @Override
    public List<String> getNotes() {
        return notes;
    }

    @Override
    public List<Reference> getReferences() {
        return references;
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, notes, references);
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
        BasicDocumentSnippet other = (BasicDocumentSnippet) obj;
        return Objects.equals(description, other.description)
                && Objects.equals(notes, other.notes)
                && Objects.equals(references, other.references);
    }

    @Override
    public String toString() {
        return String.format("BasicDocumentSnippet(description=%s, notes=%s, references=%s)", //$NON-NLS-1$
                description, notes, references);
    }

    /**
     * A basic implementation of {@link com.tsurugidb.tsubakuro.util.DocumentSnippet.Reference Reference}.
     */
    public static class BasicReference implements Reference {

        private final String title;

        private final String location;

        /**
         * Creates a new instance.
         * @param location the location of the reference
         * @param title the title of the reference (optional)
         */
        public BasicReference(@Nonnull String location, @Nullable String title) {
            Objects.requireNonNull(location);
            this.title = title;
            this.location = location;
        }

        @Override
        public Optional<String> getTitle() {
            return Optional.ofNullable(title);
        }

        @Override
        public String getLocation() {
            return location;
        }

        @Override
        public int hashCode() {
            return Objects.hash(location, title);
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
            var other = (BasicReference) obj;
            return Objects.equals(location, other.location) && Objects.equals(title, other.title);
        }

        @Override
        public String toString() {
            return String.format("Reference(location=%s, title=%s)", location, title); //$NON-NLS-1$
        }
    }
}

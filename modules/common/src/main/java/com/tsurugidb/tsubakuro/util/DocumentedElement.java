package com.tsurugidb.tsubakuro.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An element with {@link DocumentSnippet}.
 * @param <T> the element type
 */
public class DocumentedElement<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentedElement.class);

    private final DocumentSnippet document;

    private final T element;

    /**
     * Creates a new instance.
     * @param document the document
     * @param element the documented element
     */
    public DocumentedElement(@Nonnull DocumentSnippet document, @Nonnull T element) {
        Objects.requireNonNull(document);
        Objects.requireNonNull(element);
        this.document = document;
        this.element = element;
    }

    /**
     * Creates a list of {@link DocumentedElement} about enum constants.
     * <p>
     * Note: For convenience, the type parameter bound does not have {@code Enum<T>}.
     * </p>
     * @param <T> the enum type
     * @param enumClass the enum class object
     * @return a list of created instances for the constants of the specified enum type,
     *      or empty list if the specified type does not indicates an enum type
     */
    public static <T> List<DocumentedElement<T>> constantsOf(@Nonnull Class<T> enumClass) {
        Objects.requireNonNull(enumClass);
        if (!Enum.class.isAssignableFrom(enumClass)) {
            return List.of();
        }
        var fields = enumClass.getFields();
        var results = new ArrayList<DocumentedElement<T>>(fields.length);
        var constants = enumClass.getEnumConstants();
        for (var c : constants) {
            var constant = (Enum<?>) c;
            Field field;
            try {
                field = enumClass.getField(constant.name());
            } catch (NoSuchFieldException | SecurityException e) {
                // may not occur
                LOG.warn("failed to obtain enum constant declaration: {}.{}", enumClass.getName(), constant.name(), e); //$NON-NLS-1$
                continue;
            }
            var document = Optional.ofNullable(field.getAnnotation(Doc.class))
                    .map(BasicDocumentSnippet::of)
                    .orElseGet(BasicDocumentSnippet::new);
            results.add(new DocumentedElement<>(document, c));
        }
        return results;
    }

    /**
     * Returns the document snippet for the element.
     * @return the document snippet
     */
    public DocumentSnippet getDocument() {
        return document;
    }

    /**
     * Returns the wrapped element.
     * @return the wrapped element
     */
    public T getElement() {
        return element;
    }

    @Override
    public String toString() {
        return String.format("DocumentedElement(document=%s, element=%s)", document, element); //$NON-NLS-1$
    }
}

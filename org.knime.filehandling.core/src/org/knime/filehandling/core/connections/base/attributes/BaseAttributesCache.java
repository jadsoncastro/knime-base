/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   13.01.2020 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.connections.base.attributes;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Cache for file attributes. Attributes can be stored
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class BaseAttributesCache implements AttributesCache {

    private final long m_timeTolive;

    private final Cache<String, BaseFileAttributes> m_attributesCache;

    /**
     * Constructs a attribute cache with the given time to live in milliseconds.
     *
     * @param timeToLive time to live in milliseconds
     */
    public BaseAttributesCache(final long timeToLive) {
        m_timeTolive = timeToLive;
        m_attributesCache =
            CacheBuilder.newBuilder().softValues().expireAfterWrite(m_timeTolive, TimeUnit.MILLISECONDS).build();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void storeAttributes(final String path, final BaseFileAttributes attributes) {
        m_attributesCache.put(path, attributes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Optional<BaseFileAttributes> getAttributes(final String path) {

        Optional<BaseFileAttributes> attributes = Optional.ofNullable(m_attributesCache.getIfPresent(path));
        if (attributes.isPresent() && isExpired(attributes.get())) {
            m_attributesCache.invalidate(path);
            attributes = Optional.empty();
        }
        return attributes;

    }

    private boolean isExpired(final BaseFileAttributes attributes) {
        return (System.currentTimeMillis() - attributes.getFetchTime()) > m_timeTolive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void clearCache() {
        m_attributesCache.invalidateAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAttribute(final String path) {
        m_attributesCache.invalidate(path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void removeAttributes(final String prefix) {
        List<String> keys = m_attributesCache.asMap().keySet().stream().filter(key -> key.startsWith(prefix))
            .collect(Collectors.toList());

        m_attributesCache.invalidateAll(keys);
    }

}

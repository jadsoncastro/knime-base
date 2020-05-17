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
 *   May 16, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.node.table.reader.randomaccess;

import java.util.ArrayList;

import gnu.trove.set.hash.TIntHashSet;

/**
 * A decorator for {@link RandomAccessible RandomAccessibles} that caches values returned by the underlying
 * {@link RandomAccessible RandomAccessible's} {@link RandomAccessible#get(int)} method.</br>
 * The cache's capacity is extended whenever a new decoratee whose size is larger than the current capacity of the
 * cache. The cache is then filled lazily, i.e. whenever the values are retrieved for the first time via
 * {@link RandomAccessible#get(int)}. We use an {@link ArrayList} as cache under the assumption that the access patterns
 * are likely to be dense i.e. in most cases most values are actually retrieved from the underlying
 * {@link RandomAccessible}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <V> the type of values this {@link RandomAccessible} holds
 */
public final class CachingRandomAccessible<V> implements RandomAccessible<V> {

    private final TIntHashSet m_cachedIdxs = new TIntHashSet();

    private final ArrayList<V> m_cache = new ArrayList<>();

    private RandomAccessible<V> m_decoratee;

    private int m_decorateeSize;

    public void setDecoratee(final RandomAccessible<V> decoratee) {
        assert decoratee != null : "decoratees must not be null.";
        m_decoratee = decoratee;
        m_cachedIdxs.clear();
        m_decorateeSize = decoratee.size();
        m_cache.ensureCapacity(m_decorateeSize);
        // ensure that we can cache the values without running out of bounds
        for (int i = m_cache.size(); i < m_decorateeSize; i++) {
            m_cache.add(null);
        }
    }

    @Override
    public int size() {
        assert m_decoratee != null : "No decoratee set.";
        return m_decorateeSize;
    }

    @Override
    public V get(final int idx) {
        assert m_decoratee != null : "No decoratee set.";
        if (m_cachedIdxs.contains(idx)) {
            return m_cache.get(idx);
        } else {
            m_cachedIdxs.add(idx);
            final V value = m_decoratee.get(idx);
            // the setDecoratee method ensures that we don't run out of bounds
            m_cache.set(idx, value);
            return value;
        }
    }

}

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
 *   Jan 28, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.node.table.reader;

import org.knime.core.data.convert.map.MappingFramework;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.core.data.convert.map.Source;
import org.knime.filehandling.core.node.table.reader.randomaccess.CachingRandomAccessible;
import org.knime.filehandling.core.node.table.reader.randomaccess.RandomAccessible;
import org.knime.filehandling.core.node.table.reader.read.Read;

/**
 * Serves as adapter between a {@link Read} and the mapping framework by representing a {@link Source}.</br>
 *
 * An extending class should look as follows:
 *
 * <pre>
 * final class ExampleReadAdapter extends ReadAdapter<Type, Value> {
 * }
 * </pre>
 *
 * That is, it should not contain any implementation and should only define the class to be used when creating a
 * {@link ProducerRegistry} via {@link MappingFramework#forSourceType(Class)}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <T> type used to identify data types
 * @param <V> type of tokens read by the reader
 * @noreference not meant to be referenced by clients
 */
public abstract class ReadAdapter<T, V> implements Source<T> {

    // TODO we know the size of the random accessibles, so we could use a static cache
    private final CachingRandomAccessible<V> m_cache = new CachingRandomAccessible<>();

    private RandomAccessible<V> m_current;

    private Object[] m_cacheArray;

    /**
     * Constructor to be called by extending classes.
     */
    protected ReadAdapter() {
    }


    /**
     * Sets a {@link RandomAccessible} that serves as new source.
     *
     * @param current
     */
    public void setSource(final RandomAccessible<V> current) {
//        m_cache.setDecoratee(current);
        m_current = current;
        if (m_cacheArray == null || m_cacheArray.length < current.size()) {
            m_cacheArray = new Object[current.size()];
        }
        for (int i = 0; i < m_cacheArray.length; i++) {
            m_cacheArray[i] = current.get(i);
        }

    }

    /**
     * Returns the value identified by the provided {@link ReadAdapterParams}. When implementing your
     * CellValueProducers, call this method to access the values.
     *
     * @param params read parameters
     * @return the value identified by params
     */
    public final V get(final ReadAdapterParams<?> params) {
//        return m_cache.get(params.getIdx());
//        return m_current.get(params.getIdx());
        return (V)m_cacheArray[params.getIdx()];
    }

    /**
     * Used to identify values in {@link ReadAdapter#get(ReadAdapterParams)}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param <A> the concrete ReadAdapter implementation (only necessary to satisfy the compiler)
     * @noreference not meant to be referenced by clients
     */
    public static final class ReadAdapterParams<A extends ReadAdapter<?, ?>> implements ProducerParameters<A> {

        private final int m_idx;

        /**
         * Constructor.
         *
         * @param idx of the corresponding column
         */
        public ReadAdapterParams(final int idx) {
            m_idx = idx;
        }

        private int getIdx() {
            return m_idx;
        }

        @Override
        public String toString() {
            return Integer.toString(m_idx);
        }
    }

}

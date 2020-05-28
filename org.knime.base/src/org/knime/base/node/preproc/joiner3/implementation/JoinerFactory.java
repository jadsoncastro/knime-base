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
 *   Apr 29, 2020 (carlwitt): created
 */
package org.knime.base.node.preproc.joiner3.implementation;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.core.node.BufferedDataTable;

/**
 * A joiner factory provides a method to create a joiner implementation.
 * This functional interface describes joiner constructor signatures.
 * The provided JoinAlgorithm enum can be used to configure the joiner node, e.g., to select an implementation.
 * See {@link Joiner3Settings#getJoinAlgorithm()}.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 *
 */
@FunctionalInterface
public interface JoinerFactory {

    public enum JoinAlgorithm {
        HASH("Hash Join", HashJoin::new),
        HYBRID_HASH("Hybrid Hash Join", HybridHashJoin::new),
        AUTO("Automatic", CostModelFactory.Instance);

        private final String m_text;
        private final JoinerFactory m_factory;

        private JoinAlgorithm(final String text, final JoinerFactory factory) {
            m_text = text;
            m_factory = factory;
        }

        @Override
        public String toString() {
            return m_text;
        }
        public JoinerFactory getFactory() {
            return m_factory;
        }
    }

    public JoinImplementation create(final Joiner3Settings settings, final BufferedDataTable... tables);

}
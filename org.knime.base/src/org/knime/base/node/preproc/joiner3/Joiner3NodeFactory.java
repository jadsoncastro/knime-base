/*
 * ------------------------------------------------------------------------
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
 */
package org.knime.base.node.preproc.joiner3;

import java.util.Optional;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;

/**
 * This factory create all necessary classes for the joiner node.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 * @author Thorsten Meinl, University of Konstanz
 *
 */
public class Joiner3NodeFactory extends ConfigurableNodeFactory<Joiner3NodeModel> {

    @Override
    public int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<Joiner3NodeModel> createNodeView(final int viewIndex, final Joiner3NodeModel nodeModel) {
        throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean hasDialog() {
        return true;
    }

    // when changing any of these, change the group identifiers in the factory.xml as well.
    private final static String outerGroupId = "Outer table";
    private final static String innerGroupId = "Inner table";
    private final static String additionalInnerGroupId = "Additional inner table";
    private final static String resultId = "Join result";

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        PortsConfigurationBuilder b = new PortsConfigurationBuilder();

        // two mandatory inputs: outer and inner table
        b.addFixedInputPortGroup(outerGroupId, BufferedDataTable.TYPE);
        b.addFixedInputPortGroup(innerGroupId, BufferedDataTable.TYPE);

        // optional additional input ports
        b.addExtendableInputPortGroup(additionalInnerGroupId, BufferedDataTable.TYPE);

        // single output
        b.addFixedOutputPortGroup(resultId, BufferedDataTable.TYPE);

        return Optional.of(b);
    }

    @Override
    protected Joiner3NodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        // cannot be null due to #createPortsConfigBuilder's correctness
        return new Joiner3NodeModel(creationConfig.getPortConfig().get());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        return new Joiner3NodeDialog();
    }

}

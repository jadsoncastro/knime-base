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
 *   19 Feb 2020 (Temesgen H. Dadi, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.base.node.io.filehandling.imagewriter;

import java.io.OutputStream;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.image.ImageValue;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.node.portobject.writer.PortObjectToFileWriterNodeModel;

/**
 * Node model of the Image Port writer node.
 *
 * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
 */
public class ImagePortWriterNodeModel extends PortObjectToFileWriterNodeModel<ImagePortWriterNodeConfig> {

    /**
     * @param creationConfig node creation configuration
     */
    protected ImagePortWriterNodeModel(final NodeCreationConfiguration creationConfig) {
        super(creationConfig, new ImagePortWriterNodeConfig(creationConfig));
    }

    @Override
    protected void write(final PortObject object, final OutputStream outputStream, final ExecutionContext exec)
        throws Exception {

        CheckUtils.checkArgument(object instanceof ImagePortObject, "The connected port is not a valid Image Port.");

        final ImagePortObject imgObject = (ImagePortObject)object;
        final DataCell imgDataCell = imgObject.toDataCell();

        CheckUtils.checkArgument(imgDataCell instanceof ImageValue,
            "Image object does not produce a valid image object but " + imgDataCell.getClass().getName(), "");
        final ImageValue imgValue = (ImageValue)imgDataCell;
        imgValue.getImageContent().save(outputStream);
    }

    @Override
    protected void configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        // Check for the compatibility of the DataType at the input ports.
        final ImagePortObjectSpec imgObject = (ImagePortObjectSpec)inSpecs[0];
        final DataType dataType = imgObject.getDataType();
        CheckUtils.checkSetting(dataType.isCompatible(ImageValue.class),
            "The image provided by the connected port is not supported.");
    }

}

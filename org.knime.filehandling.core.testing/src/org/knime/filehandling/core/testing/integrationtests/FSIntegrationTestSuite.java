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
 */
package org.knime.filehandling.core.testing.integrationtests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.knime.filehandling.core.testing.integrationtests.files.TempDirectoriesTest;
import org.knime.filehandling.core.testing.integrationtests.files.TempFilesTest;
import org.knime.filehandling.core.testing.integrationtests.filesystem.GetPathTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.ByteChannelTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.CheckAccessTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.CopyTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.CreateTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.DeleteTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.DirectoryStreamTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.InputStreamTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.MoveTest;
import org.knime.filehandling.core.testing.integrationtests.filesystemprovider.OutputStreamTest;
import org.knime.filehandling.core.testing.integrationtests.location.FSLocationTest;
import org.knime.filehandling.core.testing.integrationtests.path.CompareTest;
import org.knime.filehandling.core.testing.integrationtests.path.EndsStartsWithTest;
import org.knime.filehandling.core.testing.integrationtests.path.GetFileNameTest;
import org.knime.filehandling.core.testing.integrationtests.path.PathTest;
import org.knime.filehandling.core.testing.integrationtests.path.SubPathTest;
import org.knime.filehandling.core.testing.integrationtests.path.ToStringTest;

@RunWith(Suite.class)
@SuiteClasses({//
    GetPathTest.class, //
    ByteChannelTest.class, //
    CheckAccessTest.class, //
    CopyTest.class, //
    CreateTest.class, //
    DeleteTest.class, //
    DirectoryStreamTest.class, //
    InputStreamTest.class, //
    MoveTest.class, //
    PathTest.class, //
    SubPathTest.class, //
    PathTest.class, //
    ToStringTest.class, //
    EndsStartsWithTest.class, //
    GetFileNameTest.class, //
    CompareTest.class, //
    OutputStreamTest.class,//
    TempFilesTest.class,//
    TempDirectoriesTest.class,//
    FSLocationTest.class//
})
public class FSIntegrationTestSuite {

}

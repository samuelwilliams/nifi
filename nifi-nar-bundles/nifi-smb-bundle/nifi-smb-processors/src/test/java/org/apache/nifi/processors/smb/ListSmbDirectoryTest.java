/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.smb;

import com.hierynomus.msdtyp.FileTime;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileBasicInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.msfscc.fileinformation.FileStandardInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.common.SmbPath;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.*;

public class ListSmbDirectoryTest {
    private TestRunner testRunner;

    private DiskShare diskShare;

    private final static String HOSTNAME = "host";
    private final static String SHARE = "share";
    private final static String DIRECTORY = "nifi\\input";
    private final static String DOMAIN = "";
    private final static String USERNAME = "user";
    private final static String PASSWORD = "pass";

    private void setupSmbProcessor() throws IOException {
        SMBClient smbClient = mock(SMBClient.class);
        Connection connection = mock(Connection.class);
        Session session = mock(Session.class);
        diskShare = mock(DiskShare.class);

        when(smbClient.connect(any(String.class))).thenReturn(connection);
        when(connection.authenticate(any(AuthenticationContext.class))).thenReturn(session);
        when(session.connectShare(SHARE)).thenReturn(diskShare);


        testRunner.setProperty(ListSmbDirectory.HOSTNAME, HOSTNAME);
        testRunner.setProperty(ListSmbDirectory.SHARE, SHARE);
        testRunner.setProperty(ListSmbDirectory.DIRECTORY, DIRECTORY);
        if (!DOMAIN.isEmpty()) {
            testRunner.setProperty(ListSmbDirectory.DOMAIN, DOMAIN);
        }
        testRunner.setProperty(ListSmbDirectory.USERNAME, USERNAME);
        testRunner.setProperty(ListSmbDirectory.PASSWORD, PASSWORD);

        ListSmbDirectory ListSmbDirectory = (ListSmbDirectory) testRunner.getProcessor();
        ListSmbDirectory.initSmbClient(smbClient);
    }

    private FileIdBothDirectoryInformation mockFile(String path, String filename, String fileContent, long fileAttributes) {
        File smbFile = mock(File.class);
        final String fullPath = path + "\\" + filename;
        when(diskShare.openFile(
                eq(fullPath),
                anySet(),
                anySet(),
                anySet(),
                any(SMB2CreateDisposition.class),
                anySet()
        )).thenReturn(smbFile);
        when(smbFile.getFileName()).thenReturn(filename);

        if (fileContent != null) {
            InputStream is = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8));
            when(smbFile.getInputStream()).thenReturn(is);
        }

        FileTime fileTime = FileTime.ofEpochMillis(0);
        FileIdBothDirectoryInformation fdInfo = mock(FileIdBothDirectoryInformation.class);
        when(fdInfo.getFileName()).thenReturn(filename);
        when(fdInfo.getFileAttributes()).thenReturn(fileAttributes);
        when(fdInfo.getCreationTime()).thenReturn(fileTime);
        when(fdInfo.getLastWriteTime()).thenReturn(fileTime);
        when(fdInfo.getLastAccessTime()).thenReturn(fileTime);
        when(fdInfo.getChangeTime()).thenReturn(fileTime);
        when(fdInfo.getShortName()).thenReturn("TESTFI~1.TXT");

        FileAllInformation fileAllInfo = mock(FileAllInformation.class);
        FileBasicInformation fileBasicInfo = new FileBasicInformation(fileTime, fileTime, fileTime, fileTime, 0);
        FileStandardInformation fileStandardInformation = mock(FileStandardInformation.class);

        when(smbFile.getFileInformation()).thenReturn(fileAllInfo);
        when(fileAllInfo.getBasicInformation()).thenReturn(fileBasicInfo);
        when(fileAllInfo.getStandardInformation()).thenReturn(fileStandardInformation);
        when(fileStandardInformation.getEndOfFile()).thenReturn((long) 0);

        return fdInfo;
    }

    private FileIdBothDirectoryInformation mockFile(String path, String filename, String fileContent) {
        return mockFile(path, filename, fileContent, FileAttributes.FILE_ATTRIBUTE_NORMAL.getValue());
    }

    private FileIdBothDirectoryInformation mockDir(String path, List<FileIdBothDirectoryInformation> files) {
        final String[] fileSplits = path.split("\\\\");
        final String filename = fileSplits[fileSplits.length - 1];
        SmbPath smbPath = new SmbPath(HOSTNAME, SHARE);
        when(diskShare.folderExists(path)).thenReturn(true);
        when(diskShare.list(path)).thenReturn(files);
        when(diskShare.getSmbPath()).thenReturn(smbPath);

        FileIdBothDirectoryInformation fdInfo = mock(FileIdBothDirectoryInformation.class);
        when(fdInfo.getFileName()).thenReturn(filename);
        when(fdInfo.getFileAttributes()).thenReturn(FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
        return fdInfo;
    }

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(ListSmbDirectory.class);
        MockitoAnnotations.openMocks(this).close();
        setupSmbProcessor();
    }

    @Test
    public void testNonRecurse() {
        testRunner.setProperty(ListSmbDirectory.RECURSE, "false");
        String subDir = DIRECTORY + "\\subdir1";
        mockDir(DIRECTORY, new ArrayList<>() {{
            add(mockFile(DIRECTORY, "README.txt", "This is some text."));
            add(mockFile(DIRECTORY, "image.jpg", "image/jpg"));
            add(mockDir(subDir, new ArrayList<>() {{
                add(mockFile(subDir, "file3.txt", "abc"));
            }}));
        }});

        testRunner.run();

        testRunner.assertTransferCount(ListSmbDirectory.REL_SUCCESS, 2);
    }

    @Test
    public void testRecurse() {
        testRunner.setProperty(ListSmbDirectory.RECURSE, "true");
        String subDir = DIRECTORY + "\\subdir1";
        mockDir(DIRECTORY, new ArrayList<>() {{
            add(mockFile(DIRECTORY, "README.txt", "This is some text."));
            add(mockFile(DIRECTORY, "image.jpg", "image/jpg"));
            add(mockDir(subDir, new ArrayList<>() {{
                add(mockFile(subDir, "file3.txt", "abc"));
            }}));
        }});

        testRunner.run();

        testRunner.assertTransferCount(ListSmbDirectory.REL_SUCCESS, 3);
    }
}

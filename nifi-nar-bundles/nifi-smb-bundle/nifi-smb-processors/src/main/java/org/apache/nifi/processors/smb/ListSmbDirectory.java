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

import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"samba, smb, cifs, files, list"})
@CapabilityDescription("Lists files on a samba network location and outputs newline separated list to a flowfile. " +
        "Use this processor instead of a cifs mounts if share access control is important. " +
        "Configure the Hostname, Share and Directory accordingly: \\\\[Hostname]\\[Share]\\[path\\to\\Directory]")
@SeeAlso({PutSmbFile.class})

public class ListSmbDirectory extends AbstractProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The network host to which files should be written.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .name("Share")
            .description("The network share to which files should be written. This is the \"first folder\"" +
                    "after the hostname: \\\\hostname\\[share]\\dir1\\dir2")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The network folder to which files should be written. This is the remaining relative " +
                    "path after the share: \\\\hostname\\share\\[dir1\\dir2].")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .name("Domain")
            .description("The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username used for authentication. If no username is set then anonymous authentication is attempted.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password used for authentication. Required if Username is set.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether or not to list files from subdirectories")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All files are routed to success").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private SMBClient smbClient = null; // this gets synchronized when the `connect` method is called

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = List.of(
                HOSTNAME,
                SHARE,
                DIRECTORY,
                DOMAIN,
                USERNAME,
                PASSWORD,
                RECURSE
        );

        this.relationships = Set.of(REL_SUCCESS);

        if (this.smbClient == null) {
            initSmbClient();
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = new ArrayList<>();
        if (validationContext.getProperty(USERNAME).isSet() && !validationContext.getProperty(PASSWORD).isSet()) {
            set.add(new ValidationResult.Builder().explanation("Password must be set if username is supplied.").build());
        }
        return set;
    }

    private void initSmbClient() {
        initSmbClient(new SMBClient());
    }

    public void initSmbClient(SMBClient smbClient) {
        this.smbClient = smbClient;
    }

    private ArrayList<FileInfo> performListing(final DiskShare diskShare, final String directory, final boolean recurseSubdirectories) {
        final ArrayList<FileInfo> dirList = new ArrayList<>();

        if (!diskShare.folderExists(directory)) {
            return dirList;
        }

        final List<FileIdBothDirectoryInformation> children = diskShare.list(directory);
        if (children == null) {
            return dirList;
        }

        for (final FileIdBothDirectoryInformation child : children) {
            final String filename = child.getFileName();
            if (filename.equals(".") || filename.equals("..")) {
                continue;
            }
            String fullPath = (directory.isEmpty()) ? filename : directory + "\\" + filename;

            //Determine if file is a directory
            if ((child.getFileAttributes() & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) != 0) {
                if (recurseSubdirectories) {
                    dirList.addAll(performListing(diskShare, fullPath, true));
                }
            } else {
                dirList.add(new FileInfo(child, directory, diskShare.getSmbPath().toString()));
            }
        }

        return dirList;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = getLogger();

        final String hostname = context.getProperty(HOSTNAME).getValue();
        final String shareName = context.getProperty(SHARE).getValue();

        final String domain = context.getProperty(DOMAIN).getValue();
        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();

        String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        if (directory == null) {
            directory = "";
        }

        AuthenticationContext ac;
        if (username != null && password != null) {
            ac = new AuthenticationContext(
                    username,
                    password.toCharArray(),
                    domain);
        } else {
            ac = AuthenticationContext.anonymous();
        }

        try (Connection connection = smbClient.connect(hostname);
             Session smbSession = connection.authenticate(ac);
             DiskShare share = (DiskShare) smbSession.connectShare(shareName)
        ) {
            final ArrayList<FileInfo> listing = performListing(share, directory, context.getProperty(RECURSE).asBoolean());

            for (FileInfo file : listing) {
                FlowFile flowFile = session.create();
                session.putAllAttributes(flowFile, file.getAllAttributes());
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            throw new ProcessException("An error occurred", e);
            //logger.error("Could not establish smb connection because of error {}", new Object[]{e});
            //context.yield();
        }
    }

    public static final class FileInfo {
        private final String fileName;
        private final String shortName;
        private final String path;
        private final String absolutePath;
        private final String size;
        private final String creationTime;
        private final String lastWriteTime;
        private final String lastAccessTime;
        private final String changeTime;

        public FileInfo(FileIdBothDirectoryInformation childItem, String directory, String smbShare) {
            this.fileName = childItem.getFileName();
            this.path = (directory.isEmpty()) ? this.fileName : directory + "\\" + this.fileName;;
            this.size = String.valueOf(childItem.getAllocationSize());
            this.creationTime = childItem.getCreationTime().toString();
            this.lastWriteTime = childItem.getLastWriteTime().toString();
            this.lastAccessTime = childItem.getLastAccessTime().toString();
            this.changeTime = childItem.getChangeTime().toString();
            this.shortName = childItem.getShortName();
            this.absolutePath = smbShare + "\\" + this.path;
        }

        public Map<String, String> getAllAttributes() {
            final Map<String, String> attributes = new HashMap<>();

            attributes.put("filename", this.fileName);
            attributes.put("file.shortName", this.shortName);
            attributes.put("path", this.path);
            attributes.put("absolute.path", this.absolutePath);
            attributes.put("file.size", this.size);
            attributes.put("file.creationTime", this.creationTime);
            attributes.put("file.lastWriteTime", this.lastWriteTime);
            attributes.put("file.lastAccessTime", this.lastAccessTime);
            attributes.put("file.changeTime", this.changeTime);

            return attributes;
        }
    }
}

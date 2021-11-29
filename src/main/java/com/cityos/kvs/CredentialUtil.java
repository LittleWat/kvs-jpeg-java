package com.cityos.kvs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialUtil {
    private static final Logger log = LoggerFactory.getLogger(CredentialUtil.class);

    public static AWSCredentialsProvider generateAWSCredentialsProvider(boolean isLocalExec) {
        if (isLocalExec) {
            return new ProfileCredentialsProvider();
        } else {
            return new EC2ContainerCredentialsProviderWrapper();
        }
    }
}

package com.cityos.kvs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.kinesisvideo.client.KinesisVideoClient;
import com.amazonaws.kinesisvideo.client.mediasource.CameraMediaSourceConfiguration;
import com.amazonaws.kinesisvideo.common.exception.KinesisVideoException;
import com.amazonaws.kinesisvideo.java.client.KinesisVideoJavaClientFactory;
import com.amazonaws.kinesisvideo.parser.examples.lambda.EncodedFrame;
import com.amazonaws.kinesisvideo.parser.examples.lambda.KVSMediaSource;
import com.amazonaws.kinesisvideo.parser.utilities.*;
import com.amazonaws.kinesisvideo.producer.StreamInfo;
import com.amazonaws.regions.Regions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Path;

public class JpegToKvs {
    private static final Logger log = LoggerFactory.getLogger(JpegToKvs.class);

    private static final Regions region = Regions.AP_NORTHEAST_1;
    private AWSCredentialsProvider credentialsProvider;
    private final String outStreamName;
    private MyKVSMediaSource kvsMediaSource;
    private boolean isKVSProducerInitialized = false;
    private boolean isEncoderInitialized = false;

    private H264FrameEncoder h264Encoder;
    private int frameNo = 0;
    private int currentWidth = 0;
    private int currentHeight = 0;
    private EncodedFrame currentEncodedFrame;

    private final JpegDirManager jpegDirManager;

    public JpegToKvs(String outStreamName, String imageDir, boolean isLocalExec) {
        credentialsProvider = CredentialUtil.generateAWSCredentialsProvider(isLocalExec);
        this.outStreamName = outStreamName;
        jpegDirManager = new JpegDirManager(imageDir);
    }

    public void run() {
        while (true) {
            Path latestFilPath = jpegDirManager.getLatestFilePath();
            try {
                if(latestFilPath == null) {
                    log.info("Waiting for a new file ...");
                    Thread.sleep(1000);
                    continue;
                }
                BufferedImage readBufferedImage = ImageIO.read(latestFilPath.toFile());

                EncodedFrame encodedH264Frame = encodeH264Frame(readBufferedImage);
                encodedH264Frame.setTimeCode(System.currentTimeMillis());

//                log.info("Encoded frame : {} with timecode : {}", frameNo, encodedH264Frame.getTimeCode());
                h264Encoder.setFrameNumber(0); // TODO 無理矢理、全てのフレームをキーフレームにしているので書き直したい

                putFrame(encodedH264Frame);
                log.info("putFrame:" +  latestFilPath.toString());
                frameNo++;

                jpegDirManager.cleanDirOlderThan(latestFilPath.toString());
                Thread.sleep(1000);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private EncodedFrame encodeH264Frame(BufferedImage bufferedImage) {
        try {
            initializeEncoder(bufferedImage);
            return h264Encoder.encodeFrame(bufferedImage);
        } catch (Exception var3) {
            throw new RuntimeException("Unable to encode the bufferedImage !", var3);
        }
    }

    private void initializeEncoder(BufferedImage bufferedImage) {
        if (!isEncoderInitialized || currentWidth != bufferedImage.getWidth() || currentHeight != bufferedImage.getHeight()) {
            h264Encoder = new H264FrameEncoder(bufferedImage.getWidth(), bufferedImage.getHeight(), 1024);

            isEncoderInitialized = true;
            currentWidth = bufferedImage.getWidth();
            currentHeight = bufferedImage.getHeight();
        }
    }

    private void putFrame(EncodedFrame encodedH264Frame) {
        if (!isKVSProducerInitialized) {
            log.info("Initializing JNI...");
            initializeKinesisVideoProducer(encodedH264Frame.getCpd().array());
            isKVSProducerInitialized = true;
        }

        kvsMediaSource.putFrameData(encodedH264Frame);
        log.info("PutFrame successful for frame no : {}", frameNo);
    }


    public void initializeKinesisVideoProducer(byte[] cpd) {
        try {
            log.info("Initializing KVS Producer with stream name {} and region : {}", outStreamName, region.getName());
            final KinesisVideoClient kinesisVideoClient = KinesisVideoJavaClientFactory.createKinesisVideoClient(region, credentialsProvider);
            CameraMediaSourceConfiguration configuration = (new CameraMediaSourceConfiguration.Builder())
                    .withFrameRate(1)
                    .withRetentionPeriodInHours(1)
                    .withCameraId("/dev/video0")
                    .withIsEncoderHardwareAccelerated(false)
                    .withEncodingMimeType("video/avc")
                    .withNalAdaptationFlags(StreamInfo.NalAdaptationFlags.NAL_ADAPTATION_ANNEXB_NALS)
                    .withIsAbsoluteTimecode(true)
                    .withEncodingBitRate(100000)
                    .withHorizontalResolution(1280)
                    .withVerticalResolution(720)
                    .withCodecPrivateData(cpd)
                    .build();
            kvsMediaSource = new MyKVSMediaSource(ProducerStreamUtil.toStreamInfo(outStreamName, configuration));
            kvsMediaSource.configure(configuration);
            kinesisVideoClient.registerMediaSource(kvsMediaSource);
        } catch (KinesisVideoException var4) {
            log.error("Exception while initialize KVS Producer !", var4);
        }

    }

    public static void main(final String[] args) {
        try {
            boolean isLocalExec = false;
            if (args.length != 2) {
                if (args.length == 3) {
                    isLocalExec = Boolean.valueOf(args[2]);
                } else {
                    throw new IllegalArgumentException("Invalid Arguments. outputStreamNamem, imageDir is required.");
                }
            }

            String outputStreamName = args[0];
            String imageDir = args[1];

            JpegToKvs jpegToKvs = new JpegToKvs(outputStreamName, imageDir, isLocalExec);
            jpegToKvs.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

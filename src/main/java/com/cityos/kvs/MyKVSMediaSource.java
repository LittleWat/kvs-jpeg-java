package com.cityos.kvs;

import com.amazonaws.kinesisvideo.client.mediasource.CameraMediaSourceConfiguration;
import com.amazonaws.kinesisvideo.client.mediasource.MediaSourceState;
import com.amazonaws.kinesisvideo.common.exception.KinesisVideoException;
import com.amazonaws.kinesisvideo.internal.client.mediasource.MediaSource;
import com.amazonaws.kinesisvideo.internal.client.mediasource.MediaSourceConfiguration;
import com.amazonaws.kinesisvideo.internal.client.mediasource.MediaSourceSink;
import com.amazonaws.kinesisvideo.parser.examples.lambda.EncodedFrame;
import com.amazonaws.kinesisvideo.producer.KinesisVideoFrame;
import com.amazonaws.kinesisvideo.producer.StreamCallbacks;
import com.amazonaws.kinesisvideo.producer.StreamInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class MyKVSMediaSource implements MediaSource {
    private static final Logger log = LoggerFactory.getLogger(MyKVSMediaSource.class);
    private static final int FRAME_FLAG_KEY_FRAME = 1;
    private static final int FRAME_FLAG_NONE = 0;
    private static final long HUNDREDS_OF_NANOS_IN_MS = 10000L;
    private static final long FRAME_DURATION_20_MS = 20L;
    private CameraMediaSourceConfiguration cameraMediaSourceConfiguration;
    private MediaSourceState mediaSourceState;
    private MediaSourceSink mediaSourceSink;
    private int frameIndex;
    private final StreamInfo streamInfo;

    private void putFrame(KinesisVideoFrame kinesisVideoFrame) {
        try {
            this.mediaSourceSink.onFrame(kinesisVideoFrame);
        } catch (KinesisVideoException var3) {
            throw new RuntimeException(var3);
        }
    }

    public MediaSourceState getMediaSourceState() {
        return this.mediaSourceState;
    }

    public MediaSourceConfiguration getConfiguration() {
        return this.cameraMediaSourceConfiguration;
    }

    public StreamInfo getStreamInfo() throws KinesisVideoException {
        return this.streamInfo;
    }

    public void initialize(MediaSourceSink mediaSourceSink) {
        this.mediaSourceSink = mediaSourceSink;
    }

    public void configure(MediaSourceConfiguration configuration) {
        if (!(configuration instanceof CameraMediaSourceConfiguration)) {
            throw new IllegalStateException("Configuration must be an instance of CameraMediaSourceConfiguration");
        } else {
            this.cameraMediaSourceConfiguration = (CameraMediaSourceConfiguration)configuration;
            this.frameIndex = 0;
        }
    }

    public void start() {
        this.mediaSourceState = MediaSourceState.RUNNING;
    }

    public void putFrameData(EncodedFrame encodedFrame) {
        int flags = encodedFrame.isKeyFrame() ? 1 : 0;
        if (encodedFrame.getByteBuffer() != null) {
//            KinesisVideoFrame frame = new KinesisVideoFrame(this.frameIndex++, flags, encodedFrame.getTimeCode() * 10000L, encodedFrame.getTimeCode() * 10000L, 200000L, encodedFrame.getByteBuffer());
            KinesisVideoFrame frame = new KinesisVideoFrame(this.frameIndex++,
                    flags,
                    encodedFrame.getTimeCode() * 10000L,
                    encodedFrame.getTimeCode() * 10000L,
                    2000L, // 100ns
                    encodedFrame.getByteBuffer());
            if (frame.getSize() == 0) {
                return;
            }

            this.putFrame(frame);
        } else {
            log.info("Frame Data is null !");
        }

    }

    public void stop() {
        this.mediaSourceState = MediaSourceState.STOPPED;
    }

    public boolean isStopped() {
        return this.mediaSourceState == MediaSourceState.STOPPED;
    }

    public void free() {
    }

    public MediaSourceSink getMediaSourceSink() {
        return this.mediaSourceSink;
    }

    @Nullable
    public StreamCallbacks getStreamCallbacks() {
        return null;
    }

    public MyKVSMediaSource(StreamInfo streamInfo) {
        this.streamInfo = streamInfo;
    }
}

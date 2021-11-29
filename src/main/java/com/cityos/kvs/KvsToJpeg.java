package com.cityos.kvs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.kinesisvideo.parser.examples.GetMediaWorker;
import com.amazonaws.kinesisvideo.parser.mkv.Frame;
import com.amazonaws.kinesisvideo.parser.mkv.FrameProcessException;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadata;
import com.amazonaws.kinesisvideo.parser.utilities.FrameVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.H264FrameDecoder;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTrackMetadata;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideo;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoClientBuilder;
import com.amazonaws.services.kinesisvideo.model.StartSelector;
import com.amazonaws.services.kinesisvideo.model.StartSelectorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KvsToJpeg {
    private static final Logger log = LoggerFactory.getLogger(KvsToJpeg.class);

    private final Regions region = Regions.AP_NORTHEAST_1;
    private final AWSCredentialsProvider credentialsProvider;

    private final String inStreamName;
    private final String outDir;

    private ExecutorService executorService;
    private AmazonKinesisVideo amazonKinesisVideo;
    private int outputFrames = 0;

    public KvsToJpeg(String inStreamName, String outDir, boolean isLocalExec) {
        credentialsProvider = CredentialUtil.generateAWSCredentialsProvider(isLocalExec);
        this.amazonKinesisVideo = AmazonKinesisVideoClientBuilder.standard().withCredentials(credentialsProvider)
                .withRegion(region).build();

        this.executorService = Executors.newFixedThreadPool(1);
        this.inStreamName = inStreamName;
        this.outDir = outDir;
    }

    public void run() throws InterruptedException {
        log.info("inStreamName: {}", inStreamName);

        try {
            /*
             * KVS からの映像入力を開始。 KVS に映像がない場合は、一定時間おきにリトライする。
             */
            for (; outputFrames == 0; ) {
                log.info("outputFrames:" + outputFrames);
                submitGetMediaWorker();

                if (outputFrames == 0) {
                    Thread.sleep(1 * 1000);
                }
            }
        } catch (InterruptedException e) {
            log.info("Interruppted");
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);

        if (!executorService.isTerminated()) {
            log.warn("Shutting down executor service by force");
            executorService.shutdownNow();
        } else {
            log.info("Executor service is shutdown");
        }
    }

    private GetMediaWorker submitGetMediaWorker() {
        FrameVisitor frameVisitor = FrameVisitor.create(new H264FrameToJpeg());

        GetMediaWorker getMediaWorker = GetMediaWorker.create(region, credentialsProvider, inStreamName,
                new StartSelector().withStartSelectorType(StartSelectorType.NOW), amazonKinesisVideo, frameVisitor);
        executorService.submit(getMediaWorker);

        return getMediaWorker;
    }

    private class H264FrameToJpeg extends H264FrameDecoder {
        private long previousTimestampMillis;
        private SimpleDateFormat sdfForTemp;

        public H264FrameToJpeg() {
            sdfForTemp = new SimpleDateFormat("yyyyMMddHHmmss");
        }

        @Override
        public void process(final Frame frame, final MkvTrackMetadata trackMetadata,
                            final Optional<FragmentMetadata> fragmentMetadata) throws FrameProcessException {
            final BufferedImage bufferedImage = decodeH264Frame(frame, trackMetadata);

            /*
             * メタデータを取得。 ない場合は何もしない。
             */
            if (!fragmentMetadata.isPresent()) {
                return;
            }

            FragmentMetadata metadata = fragmentMetadata.get();

            /*
             * 以前に処理したタイムスタンプより、一定時間経過していない場合、何もしない。
             */
            long timestampMillis = metadata.getProducerSideTimestampMillis();
            if (timestampMillis - previousTimestampMillis < 1 * 1000) {
                return;
            }

            /*
             * フレームを JPEG として保存。
             */
            Date timestampAsDate = metadata.getProducerSideTimetampAsDate();

            File file = new File(outDir + "/" + sdfForTemp.format(timestampAsDate) + ".jpg");
            try {
                ImageIO.write(bufferedImage, "jpeg", file);
                log.info("Saved: {}", file.getAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            outputFrames++;
            previousTimestampMillis = timestampMillis;
        }
    }

    public static void main(final String[] args) {
        try {
            boolean isLocalExec = false;
            if (args.length != 2) {
                if (args.length == 3) {
                    isLocalExec = Boolean.valueOf(args[2]);
                } else {
                    throw new IllegalArgumentException("Invalid Arguments. inputStreamName, imageDir is required.");
                }
            }

            String inputStreamName = args[0];
            String outDir = args[1];
            KvsToJpeg kvsToJpeg = new KvsToJpeg(inputStreamName, outDir, isLocalExec);
            kvsToJpeg.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

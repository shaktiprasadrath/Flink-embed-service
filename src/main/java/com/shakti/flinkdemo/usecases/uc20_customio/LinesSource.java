package com.shakti.flinkdemo.usecases.uc20_customio;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A custom source written against Flink's unified Source API (FLIP-27).
 *
 * <ul>
 *   <li>{@link FileSplit}: one unit of work = one file, plus how many lines were already read</li>
 *   <li>{@link Enumerator}: runs once (on the JobManager) and hands splits to readers on request</li>
 *   <li>{@link Reader}: runs in each parallel subtask and turns a split into records</li>
 *   <li>Serializers: splits and enumerator state are part of checkpoints</li>
 * </ul>
 * Each emitted record is a {@link FileLine}. Bounded: the job ends when all files are read.
 */
public class LinesSource implements Source<FileLine, LinesSource.FileSplit, List<LinesSource.FileSplit>> {

    private final List<String> files;

    public LinesSource(List<Path> files) {
        this.files = files.stream().map(p -> p.toAbsolutePath().toString()).toList();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<FileSplit, List<FileSplit>> createEnumerator(SplitEnumeratorContext<FileSplit> context) {
        List<FileSplit> splits = files.stream().map(f -> new FileSplit(f, 0)).toList();
        return new Enumerator(context, splits);
    }

    @Override
    public SplitEnumerator<FileSplit, List<FileSplit>> restoreEnumerator(SplitEnumeratorContext<FileSplit> context,
                                                                         List<FileSplit> checkpoint) {
        return new Enumerator(context, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<FileSplit> getSplitSerializer() {
        return new SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<List<FileSplit>> getEnumeratorCheckpointSerializer() {
        return new EnumeratorStateSerializer();
    }

    @Override
    public SourceReader<FileLine, FileSplit> createReader(SourceReaderContext context) {
        return new Reader(context);
    }

    /** A file and the number of lines already emitted from it. */
    public record FileSplit(String path, long linesRead) implements SourceSplit {
        @Override
        public String splitId() {
            return path;
        }
    }

    static class Enumerator implements SplitEnumerator<FileSplit, List<FileSplit>> {
        private final SplitEnumeratorContext<FileSplit> context;
        private final Deque<FileSplit> pending;

        Enumerator(SplitEnumeratorContext<FileSplit> context, List<FileSplit> splits) {
            this.context = context;
            this.pending = new ArrayDeque<>(splits);
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            FileSplit next = pending.poll();
            if (next != null) {
                context.assignSplit(next, subtaskId);
            } else {
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public void addSplitsBack(List<FileSplit> splits, int subtaskId) {
            splits.forEach(pending::addFirst); // a reader failed: hand its splits out again
        }

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public List<FileSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(pending);
        }

        @Override
        public void close() {}
    }

    static class Reader implements SourceReader<FileLine, FileSplit> {
        private final SourceReaderContext context;
        private final Deque<FileSplit> splits = new ArrayDeque<>();
        private CompletableFuture<Void> available = new CompletableFuture<>();
        private boolean noMoreSplits;

        private FileSplit current;
        private BufferedReader reader;
        private long lineNo;

        Reader(SourceReaderContext context) {
            this.context = context;
        }

        @Override
        public void start() {
            context.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<FileLine> output) throws Exception {
            if (reader == null) {
                current = splits.poll();
                if (current == null) {
                    if (noMoreSplits) {
                        return InputStatus.END_OF_INPUT;
                    }
                    // Flink now waits on isAvailable(); completed again by addSplits / notifyNoMoreSplits
                    if (available.isDone()) {
                        available = new CompletableFuture<>();
                    }
                    return InputStatus.NOTHING_AVAILABLE;
                }
                reader = Files.newBufferedReader(Path.of(current.path()), StandardCharsets.UTF_8);
                lineNo = 0;
                while (lineNo < current.linesRead() && reader.readLine() != null) {
                    lineNo++; // skip lines emitted before a restore
                }
            }
            String line = reader.readLine();
            if (line == null) {
                reader.close();
                reader = null;
                current = null;
                context.sendSplitRequest(); // finished this file, ask for another
                return InputStatus.MORE_AVAILABLE;
            }
            lineNo++;
            output.collect(new FileLine(Path.of(current.path()).getFileName().toString(), lineNo, line));
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<FileSplit> snapshotState(long checkpointId) {
            List<FileSplit> state = new ArrayList<>(splits);
            if (current != null) {
                state.add(0, new FileSplit(current.path(), lineNo));
            }
            return state;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return available;
        }

        @Override
        public void addSplits(List<FileSplit> newSplits) {
            splits.addAll(newSplits);
            signalAvailable();
        }

        @Override
        public void notifyNoMoreSplits() {
            noMoreSplits = true;
            signalAvailable();
        }

        // all SourceReader methods run in the task's mailbox thread, so no locking is needed
        private void signalAvailable() {
            available.complete(null);
        }

        @Override
        public void close() throws Exception {
            if (reader != null) {
                reader.close();
            }
        }
    }

    static class SplitSerializer implements SimpleVersionedSerializer<FileSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(FileSplit split) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeUTF(split.path());
                out.writeLong(split.linesRead());
            }
            return bytes.toByteArray();
        }

        @Override
        public FileSplit deserialize(int version, byte[] serialized) throws IOException {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
                return new FileSplit(in.readUTF(), in.readLong());
            }
        }
    }

    static class EnumeratorStateSerializer implements SimpleVersionedSerializer<List<FileSplit>> {
        private final SplitSerializer splitSerializer = new SplitSerializer();

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(List<FileSplit> splits) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(splits.size());
                for (FileSplit split : splits) {
                    byte[] s = splitSerializer.serialize(split);
                    out.writeInt(s.length);
                    out.write(s);
                }
            }
            return bytes.toByteArray();
        }

        @Override
        public List<FileSplit> deserialize(int version, byte[] serialized) throws IOException {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
                int n = in.readInt();
                Collection<FileSplit> splits = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    byte[] s = new byte[in.readInt()];
                    in.readFully(s);
                    splits.add(splitSerializer.deserialize(version, s));
                }
                return new ArrayList<>(splits);
            }
        }
    }
}

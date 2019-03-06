package io.pravega.controller.server;

import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PravegaConnectionManager {
    private final PravegaNodeUri uri;
    private final LinkedBlockingQueue<ConnectionObject> availableConnections;
    private boolean isRunning;
    private final ConnectionFactory clientCF;

    PravegaConnectionManager(PravegaNodeUri pravegaNodeUri, ConnectionFactory clientCF) {
        this.uri = pravegaNodeUri;
        this.clientCF = clientCF;
        this.availableConnections = new LinkedBlockingQueue<>();
        this.isRunning = true;
    }

    CompletableFuture<ConnectionObject> getConnection(ReplyProcessor processor) {
        ConnectionObject obj = availableConnections.poll();
        if (obj != null) {
            // return the object from the queue
            obj.processor.initialize(processor);
            return CompletableFuture.completedFuture(obj);
        } else {
            // dont create a new one.. return a created one
            ReusableReplyProcessor rp = new ReusableReplyProcessor();
            rp.initialize(processor);
            return clientCF.establishConnection(uri, rp)
                           .thenApply(connection1 -> {
                               ConnectionObject p = new ConnectionObject(connection1, rp);
                               return p;
                           });
        }
    }

    void returnConnection(ConnectionObject pair) {
        pair.processor.uninitialize();
        synchronized (this) {
            if (!isRunning) {
                // Since any connection given out is returned to this class so the connection will be closed anytime it is returned 
                // after after the shutdown has been initiated.
                pair.connection.close();
            } else {
                // as connections are returned to us, we put them in queue to be reused
                availableConnections.offer(pair);
            }
        }
    }

    /**
     * Shutdown the connection manager where all returned connections are closed and not put back into the 
     * available queue of connections.  
     * It is important to note that even after shutdown is initiated, if `getConnection` is invoked, it will return a connection. 
     */
    void shutdown() {
        // as connections are returned we need to shut them down
        synchronized (this) {
            isRunning = false;
        }
        ConnectionObject connection = availableConnections.poll();
        while(connection != null) {
            returnConnection(connection);
            connection = availableConnections.poll();
        }
    }

    class ConnectionObject {
        @Getter
        private final ClientConnection connection;
        private final ReusableReplyProcessor processor;

        ConnectionObject(ClientConnection connection, ReusableReplyProcessor processor) {
            this.connection = connection;
            this.processor = processor;
        }
    }

    private class ReusableReplyProcessor implements ReplyProcessor {
        private final AtomicReference<ReplyProcessor> replyProcessor = new AtomicReference<>();

        void initialize(ReplyProcessor replyProcessor) {
            this.replyProcessor.set(replyProcessor);
        }

        void uninitialize() {
            replyProcessor.set(null);
        }

        @Override
        public void hello(WireCommands.Hello hello) {
            replyProcessor.get().hello(hello);
        }

        @Override
        public void wrongHost(WireCommands.WrongHost wrongHost) {
            replyProcessor.get().wrongHost(wrongHost);
        }

        @Override
        public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
            replyProcessor.get().segmentAlreadyExists(segmentAlreadyExists);
        }

        @Override
        public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
            replyProcessor.get().segmentIsSealed(segmentIsSealed);
        }

        @Override
        public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {
            replyProcessor.get().segmentIsTruncated(segmentIsTruncated);
        }

        @Override
        public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
            replyProcessor.get().noSuchSegment(noSuchSegment);
        }

        @Override
        public void tableSegmentNotEmpty(WireCommands.TableSegmentNotEmpty tableSegmentNotEmpty) {
            replyProcessor.get().tableSegmentNotEmpty(tableSegmentNotEmpty);
        }

        @Override
        public void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber) {
            replyProcessor.get().invalidEventNumber(invalidEventNumber);
        }

        @Override
        public void appendSetup(WireCommands.AppendSetup appendSetup) {
            replyProcessor.get().appendSetup(appendSetup);
        }

        @Override
        public void dataAppended(WireCommands.DataAppended dataAppended) {
            replyProcessor.get().dataAppended(dataAppended);
        }

        @Override
        public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {
            replyProcessor.get().conditionalCheckFailed(dataNotAppended);
        }

        @Override
        public void segmentRead(WireCommands.SegmentRead segmentRead) {
            replyProcessor.get().segmentRead(segmentRead);
        }

        @Override
        public void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated) {
            replyProcessor.get().segmentAttributeUpdated(segmentAttributeUpdated);
        }

        @Override
        public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {
            replyProcessor.get().segmentAttribute(segmentAttribute);
        }

        @Override
        public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {
            replyProcessor.get().streamSegmentInfo(streamInfo);
        }

        @Override
        public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
            replyProcessor.get().segmentCreated(segmentCreated);
        }

        @Override
        public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
            replyProcessor.get().segmentsMerged(segmentsMerged);
        }

        @Override
        public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
            replyProcessor.get().segmentSealed(segmentSealed);
        }

        @Override
        public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {
            replyProcessor.get().segmentTruncated(segmentTruncated);
        }

        @Override
        public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
            replyProcessor.get().segmentDeleted(segmentDeleted);
        }

        @Override
        public void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported) {
            replyProcessor.get().operationUnsupported(operationUnsupported);
        }

        @Override
        public void keepAlive(WireCommands.KeepAlive keepAlive) {
            replyProcessor.get().keepAlive(keepAlive);
        }

        @Override
        public void connectionDropped() {
            replyProcessor.get().connectionDropped();
        }

        @Override
        public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated) {
            replyProcessor.get().segmentPolicyUpdated(segmentPolicyUpdated);
        }

        @Override
        public void processingFailure(Exception error) {
            replyProcessor.get().processingFailure(error);
        }

        @Override
        public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
            replyProcessor.get().authTokenCheckFailed(authTokenCheckFailed);
        }

        @Override
        public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {
            replyProcessor.get().tableEntriesUpdated(tableEntriesUpdated);
        }

        @Override
        public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {
            replyProcessor.get().tableKeysRemoved(tableKeysRemoved);
        }

        @Override
        public void tableRead(WireCommands.TableRead tableRead) {
            replyProcessor.get().tableRead(tableRead);
        }

        @Override
        public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
            replyProcessor.get().tableKeyDoesNotExist(tableKeyDoesNotExist);
        }

        @Override
        public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
            replyProcessor.get().tableKeyBadVersion(tableKeyBadVersion);
        }

        @Override
        public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {
            replyProcessor.get().tableKeysRead(tableKeysRead);
        }

        @Override
        public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {
            replyProcessor.get().tableEntriesRead(tableEntriesRead);
        }
    }
}

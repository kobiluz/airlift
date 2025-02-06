package io.airlift.memory.jetty;

import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.RetainableByteBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentRetainableBufferPool
        implements ByteBufferPool
{
    private static final int MIN_POOL_SIZE_SHIFT = 7;  // inclusive
    private static final int MAX_POOL_SIZE_SHIFT = 25; // exclusive
    private static final int NUM_POOLS = MAX_POOL_SIZE_SHIFT - MIN_POOL_SIZE_SHIFT;
    private static final int NUM_BUCKETS = 128;
    private static final long DEFAULT_MAX_HEAP_MEMORY = 8 * 1024 * 1024;
    private static final long DEFAULT_MAX_OFF_HEAP_MEMORY = 8 * 1024 * 1024;

    private final long maxHeapMemory;
    private final long maxOffHeapMemory;
    private final ArenaBucket[] heapBuckets;
    private final ArenaBucket[] offHeapBuckets;
    private final AtomicBoolean evictor = new AtomicBoolean(false);

    /**
     * Creates a new retianble buffer pool with the given configuration.
     */
    public ConcurrentRetainableBufferPool(int maxBufferSize, long maxHeapMemory, long maxOffHeapMemory)
    {
        if (maxBufferSize > 1 << MAX_POOL_SIZE_SHIFT) {
            throw new RuntimeException("maxBufferSize " + maxBufferSize + " too large");
        }
        if (maxHeapMemory <= 0) {
            maxHeapMemory = DEFAULT_MAX_HEAP_MEMORY;
        }
        if (maxOffHeapMemory <= 0) {
            maxOffHeapMemory = DEFAULT_MAX_OFF_HEAP_MEMORY;
        }
        this.maxHeapMemory = maxHeapMemory;
        this.maxOffHeapMemory = maxOffHeapMemory;

        heapBuckets = new ArenaBucket[NUM_BUCKETS];
        offHeapBuckets = new ArenaBucket[NUM_BUCKETS];
        for (int i = 0; i < NUM_BUCKETS; i++) {
            heapBuckets[i] = new ArenaBucket(false);
            offHeapBuckets[i] = new ArenaBucket(true);
        }
    }

    @Override
    public RetainableByteBuffer acquire(int size, boolean isOffHeap)
    {
        ArenaBucket bucket = getBucketArray(isOffHeap)[(int) (Thread.currentThread().threadId() % NUM_BUCKETS)];
        return bucket.alloc(size);
    }

    private ArenaBucket[] getBucketArray(boolean isOffHeap)
    {
        return isOffHeap ? offHeapBuckets : heapBuckets;
    }

    private void checkMaxMemory(boolean isOffHeap)
    {
        long max = isOffHeap ? maxOffHeapMemory : maxHeapMemory;
        if (max <= 0 || !evictor.compareAndSet(false, true)) {
            return;
        }

        try {
            if (getMemory(isOffHeap) > max) {
                evict(isOffHeap);
            }
        }
        finally {
            evictor.set(false);
        }
    }

    private void evict(boolean isOffHeap)
    {
        for (ArenaBucket bucket : getBucketArray(isOffHeap)) {
            bucket.evict();
        }
    }

    public long getOffHeapBufferCount()
    {
        return getBufferCount(true);
    }

    public long getHeapBufferCount()
    {
        return getBufferCount(false);
    }

    private long getBufferCount(boolean isOffHeap)
    {
        return Arrays.stream(getBucketArray(isOffHeap)).mapToLong(bucket -> bucket.getBufferCount()).sum();
    }

    public long getAvailableOffHeapBufferCount()
    {
        return getAvailableBufferCount(true);
    }

    public long getAvailableHeapBufferCount()
    {
        return getAvailableBufferCount(false);
    }

    private long getAvailableBufferCount(boolean isOffHeap)
    {
        return Arrays.stream(getBucketArray(isOffHeap)).mapToLong(bucket -> bucket.getAvailableBufferCount()).sum();
    }

    public long getOffHeapMemory()
    {
        return getMemory(true);
    }

    public long getHeapMemory()
    {
        return getMemory(false);
    }

    private long getMemory(boolean isOffHeap)
    {
        return Arrays.stream(getBucketArray(isOffHeap)).mapToLong(bucket -> bucket.getMemory()).sum();
    }

    public long getAvailableOffHeapMemory()
    {
        return getAvailableMemory(true);
    }

    public long getAvailableHeapMemory()
    {
        return getAvailableMemory(false);
    }

    private long getAvailableMemory(boolean isOffHeap)
    {
        return Arrays.stream(getBucketArray(isOffHeap)).mapToLong(bucket -> bucket.getAvailableMemory()).sum();
    }

    public void clear()
    {
        evict(true);
        evict(false);
    }

    @Override
    public String toString()
    {
        return String.format("%s{heap=%d/%d,offheap=%d/%d}", super.toString(), getHeapMemory(), maxHeapMemory, getOffHeapMemory(), maxOffHeapMemory);
    }

    private class ArenaBucket
    {
        private Arena arena;
        private Arena autoArena;
        private final boolean isOffHeap;
        private final FixedSizeBufferPool[] pools;

        ArenaBucket(boolean isOffHeap)
        {
            this.arena = Arena.ofShared();
            this.autoArena = Arena.ofAuto();
            this.isOffHeap = isOffHeap;

            this.pools = new FixedSizeBufferPool[NUM_POOLS];
            for (int i = 0; i < NUM_POOLS; i++) {
                pools[i] = new FixedSizeBufferPool(1 << i + MIN_POOL_SIZE_SHIFT, isOffHeap);
            }
        }

        synchronized RetainableByteBuffer alloc(int size)
        {
            int poolSizeShift = getPoolSizeShift(size);
            Buffer buffer = pools[poolSizeShift].alloc((isOffHeap && (poolSizeShift == 8)) ? autoArena : arena);
            return (RetainableByteBuffer) buffer;
        }

        private int getPoolSizeShift(int size)
        {
            int poolSizeShift = Math.max(MIN_POOL_SIZE_SHIFT, 32 - Integer.numberOfLeadingZeros(size - 1));
            poolSizeShift -= MIN_POOL_SIZE_SHIFT;
            if (poolSizeShift >= NUM_POOLS) {
                throw new RuntimeException("buffer too large " + size);
            }
            return poolSizeShift;
        }

        synchronized void evict()
        {
            boolean canClose = isOffHeap;
            for (FixedSizeBufferPool pool : pools) {
                pool.evict();
                canClose &= pool.getBufferCount() == 0;
            }
            if (canClose) {
                arena.close(); // free all memory associated with this arena
                arena = Arena.ofShared(); // restart the arena for new allocations
            }
        }

        long getBufferCount()
        {
            return Arrays.stream(pools).mapToLong(pool -> pool.getBufferCount()).sum();
        }

        long getAvailableBufferCount()
        {
            return Arrays.stream(pools).mapToLong(pool -> pool.getAvailableBufferCount()).sum();
        }

        long getMemory()
        {
            return Arrays.stream(pools).mapToLong(pool -> pool.getMemory()).sum();
        }

        long getAvailableMemory()
        {
            return Arrays.stream(pools).mapToLong(pool -> pool.getAvailableMemory()).sum();
        }
    }

    private class FixedSizeBufferPool
    {
        private final List<MemorySegment> bufferList;
        private final int bufferSize;
        private final boolean isOffHeap;
        private int numAllocatedBuffers;

        FixedSizeBufferPool(int bufferSize, boolean isOffHeap)
        {
            this.bufferList = new ArrayList<>();
            this.bufferSize = bufferSize;
            this.isOffHeap = isOffHeap;
        }

        synchronized Buffer alloc(Arena arena)
        {
            boolean allocateFromArean = bufferList.isEmpty();
            MemorySegment buffer = allocateFromArean ? allocate(arena) : bufferList.remove(0);
            numAllocatedBuffers++;
            return new Buffer(buffer, this);
        }

        synchronized void free(MemorySegment buffer)
        {
            if (numAllocatedBuffers == 0) {
                throw new RuntimeException("pool is already without allocated buffers");
            }
            numAllocatedBuffers--;
            bufferList.add(buffer);
        }

        synchronized long evict()
        {
            long availableMemory = getAvailableMemory();
            bufferList.clear();
            return availableMemory;
        }

        private MemorySegment allocate(Arena arena)
        {
            return isOffHeap ? arena.allocate(bufferSize, Integer.BYTES) : MemorySegment.ofArray(new byte[bufferSize]);
        }

        long getMemory()
        {
            return (numAllocatedBuffers + bufferList.size()) * (long) bufferSize;
        }

        long getAvailableMemory()
        {
            return bufferList.size() * (long) bufferSize;
        }

        int getBufferCount()
        {
            return numAllocatedBuffers + bufferList.size();
        }

        int getAvailableBufferCount()
        {
            return bufferList.size();
        }

        boolean isOffHeap()
        {
            return isOffHeap;
        }
    }

    private class Buffer
            implements RetainableByteBuffer
    {
        private AtomicInteger refCount;
        private MemorySegment buffer;
        private ByteBuffer byteBuffer;
        private FixedSizeBufferPool pool;
        private Arena arena;

        Buffer(MemorySegment buffer, FixedSizeBufferPool pool)
        {
            this.refCount = new AtomicInteger(1);
            this.buffer = buffer;
            this.pool = pool;

            this.byteBuffer = buffer.asByteBuffer();
            byteBuffer.position(0); // this is a requirement to return the byte buffer with these attributes
            byteBuffer.limit(0);    // this is a requirement to return the byte buffer with these attributes
        }

        @Override
        public void retain()
        {
            if (byteBuffer == null) {
                throw new IllegalStateException("buffer cannot be retained since already released");
            }
            refCount.getAndUpdate(c -> c + 1);
        }

        @Override
        public boolean release()
        {
            if (byteBuffer == null) {
                return true; // idiom potent
            }
            boolean shouldRelease = refCount.updateAndGet(c -> c - 1) == 0;
            if (shouldRelease) {
                pool.free(buffer);
                byteBuffer = null; // safety

                checkMaxMemory(pool.isOffHeap());
            }
            return shouldRelease;
        }

        @Override
        public boolean canRetain()
        {
            return true;
        }

        @Override
        public boolean isRetained()
        {
            return refCount.get() > 1;
        }

        @Override
        public ByteBuffer getByteBuffer()
        {
            return byteBuffer;
        }
    }
}

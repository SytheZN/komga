package org.gotson.komga.infrastructure.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

private const val THUMBNAIL_CACHE_MAX_BOOKS = 50
private const val THUMBNAIL_CACHE_MAX_SIZE_MB = 10
private const val BYTES_PER_MB = 1024 * 1024

@Service
class BookPageThumbnailCache(
  private val failureCooldownMs: Long = 5 * 60 * 1000L,
) {
  private val cache: Cache<String, ByteArray> =
    Caffeine
      .newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build()

  private val bookMetadata = ConcurrentHashMap<String, BookCacheMetadata>()

  private val pageCompletions = ConcurrentHashMap<String, CompletableFuture<ByteArray>>()

  private val failedPages = ConcurrentHashMap<String, Long>()

  data class BookCacheMetadata(
    val bookId: String,
    var pageCount: Int,
    var totalBytes: Long,
    var lastAccessed: Long = System.currentTimeMillis(),
  )

  fun getThumbnail(
    bookId: String,
    filename: String,
  ): ByteArray? {
    val key = cacheKey(bookId, filename)
    val thumbnail = cache.getIfPresent(key)
    if (thumbnail != null) {
      bookMetadata[bookId]?.lastAccessed = System.currentTimeMillis()
    }
    return thumbnail
  }

  fun putThumbnail(
    bookId: String,
    filename: String,
    bytes: ByteArray,
  ) {
    val key = cacheKey(bookId, filename)
    evictIfNeeded(bytes.size.toLong())
    cache.put(key, bytes)
    pageCompletions.remove(key)?.complete(bytes)
    updateBookMetadata(bookId)
    logger.debug { "Cached thumbnail for book $bookId, page $filename (${bytes.size / 1024} KB)" }
  }

  fun putThumbnails(
    bookId: String,
    thumbnails: Map<String, ByteArray>,
  ) {
    if (thumbnails.isEmpty()) return

    val thumbnailBytes = thumbnails.values.sumOf { it.size.toLong() }
    evictIfNeeded(thumbnailBytes)

    thumbnails.forEach { (filename, bytes) ->
      val key = cacheKey(bookId, filename)
      cache.put(key, bytes)
      pageCompletions.remove(key)?.complete(bytes)
    }

    bookMetadata.compute(bookId) { _, existing ->
      if (existing != null) {
        existing.pageCount = existing.pageCount + thumbnails.size
        existing.totalBytes = existing.totalBytes + thumbnailBytes
        existing
      } else {
        BookCacheMetadata(
          bookId = bookId,
          pageCount = thumbnails.size,
          totalBytes = thumbnailBytes,
        )
      }
    }

    logger.debug { "Cached ${thumbnails.size} thumbnails for book $bookId (${thumbnailBytes / 1024} KB)" }
  }

  fun waitForThumbnail(
    bookId: String,
    filename: String,
  ): CompletableFuture<ByteArray>? {
    val key = cacheKey(bookId, filename)
    if (cache.getIfPresent(key) != null) {
      return null
    }
    return pageCompletions.computeIfAbsent(key) { CompletableFuture() }
  }

  fun hasFuture(
    bookId: String,
    filename: String,
  ): Boolean {
    val key = cacheKey(bookId, filename)
    return pageCompletions.containsKey(key)
  }

  fun createFuture(
    bookId: String,
    filename: String,
  ): Boolean {
    val key = cacheKey(bookId, filename)
    return pageCompletions.putIfAbsent(key, CompletableFuture()) == null
  }

  fun isPageRecentlyFailed(
    bookId: String,
    filename: String,
  ): Boolean {
    val key = cacheKey(bookId, filename)
    val timestamp = failedPages[key] ?: return false
    val age = System.currentTimeMillis() - timestamp
    if (age > failureCooldownMs) {
      failedPages.remove(key)
      return false
    }
    return true
  }

  fun completeThumbnailExceptionally(
    bookId: String,
    filename: String,
    exception: Throwable,
  ) {
    val key = cacheKey(bookId, filename)
    pageCompletions.remove(key)?.completeExceptionally(exception)
    failedPages[key] = System.currentTimeMillis()
  }

  fun completeThumbnailExceptionallyWithoutCaching(
    bookId: String,
    filename: String,
    exception: Throwable,
  ) {
    val key = cacheKey(bookId, filename)
    pageCompletions.remove(key)?.completeExceptionally(exception)
  }

  fun isBookCached(bookId: String): Boolean {
    val isCached = bookMetadata.containsKey(bookId)
    if (isCached) {
      bookMetadata[bookId]?.lastAccessed = System.currentTimeMillis()
    }
    return isCached
  }

  private fun updateBookMetadata(bookId: String) {
    val pages = cache.asMap().filterKeys { it.startsWith("$bookId:") }
    if (pages.isNotEmpty()) {
      bookMetadata.compute(bookId) { _, existing ->
        if (existing != null) {
          existing.pageCount = pages.size
          existing.totalBytes = pages.values.sumOf { it.size.toLong() }
          existing.lastAccessed = System.currentTimeMillis()
          existing
        } else {
          BookCacheMetadata(
            bookId = bookId,
            pageCount = pages.size,
            totalBytes = pages.values.sumOf { it.size.toLong() },
          )
        }
      }
    }
  }

  fun evictBook(bookId: String) {
    bookMetadata.remove(bookId)?.let { metadata: BookCacheMetadata ->
      cache.asMap().keys.removeIf { it.startsWith("$bookId:") }
      pageCompletions.keys.removeIf { it.startsWith("$bookId:") }
      logger.debug { "Evicted book $bookId from cache (${metadata.pageCount} pages, ${metadata.totalBytes / 1024} KB)" }
    }
  }

  private fun evictIfNeeded(additionalBytes: Long) {
    val currentBookCount = bookMetadata.size
    val currentTotalBytes = bookMetadata.values.sumOf { it.totalBytes }

    val bookLimitExceeded = currentBookCount >= THUMBNAIL_CACHE_MAX_BOOKS
    val sizeLimitExceeded = (currentTotalBytes + additionalBytes) > (THUMBNAIL_CACHE_MAX_SIZE_MB * BYTES_PER_MB)

    if (!bookLimitExceeded && !sizeLimitExceeded) return

    val lruBook =
      bookMetadata.values
        .minByOrNull { it.lastAccessed }
        ?: return

    logger.debug {
      "Cache limit exceeded (books: $currentBookCount/$THUMBNAIL_CACHE_MAX_BOOKS, " +
        "size: ${currentTotalBytes / BYTES_PER_MB}MB/${THUMBNAIL_CACHE_MAX_SIZE_MB}MB). " +
        "Evicting LRU book: ${lruBook.bookId}"
    }

    evictBook(lruBook.bookId)

    if (bookMetadata.size >= THUMBNAIL_CACHE_MAX_BOOKS ||
      (bookMetadata.values.sumOf { it.totalBytes } + additionalBytes) > (THUMBNAIL_CACHE_MAX_SIZE_MB * BYTES_PER_MB)
    ) {
      evictIfNeeded(additionalBytes)
    }
  }

  private fun cacheKey(
    bookId: String,
    filename: String,
  ): String = "$bookId:$filename"

  fun getCacheStats() =
    mapOf(
      "bookCount" to bookMetadata.size,
      "totalSizeBytes" to bookMetadata.values.sumOf { it.totalBytes },
      "totalSizeMB" to bookMetadata.values.sumOf { it.totalBytes } / BYTES_PER_MB,
      "thumbnailCount" to cache.estimatedSize(),
    )
}
